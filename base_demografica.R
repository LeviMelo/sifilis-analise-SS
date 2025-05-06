# =======================================================================
# Comprehensive Demographic Basis Builder Script (Wide Format) - v14.3
# =======================================================================
#
# Purpose:
#   Builds a unified, wide-format data table. Uses a DEFINITIVE TARGET
#   AGE SCHEMA of 12 contiguous, non-overlapping bands derived from T2093
#   for demographic breakdowns. Ensures integrity of original census totals
#   and EstimaPop values. Granular pop data for census years ONLY.
#   For 2000, "Sem declaração" race counts are proportionally redistributed.
#
# Key Changes in v14.3:
#   - PIVOT_WIDER FIX: Added explicit group_by/summarise steps during the
#     creation of `census_aggregated_to_target` to ensure unique combinations
#     of keys before pivoting. This aims to resolve the "Can't convert fill" error.
#   - DEBUG AIDS: Added print statements (commented out) for PIB/CEMPRE name checking.
#
# Previous Key Changes (v14.2):
#   - RACE REDISTRIBUTION (2000): "Sem declaração" (Ignored) race counts from
#     the 2000 census (T2093) are now proportionally redistributed.
#   - SIDRA FETCH REVERTED: Reverted `fetch_sidra_data` to use the simpler
#     `get_sidra(x=...)` syntax.
# =======================================================================

# --- 0. Setup: Load Libraries ---
library(purrr)
library(sidrar)
library(dplyr)
library(furrr)
library(future)
library(readr)
library(stringr)
library(tidyr)
library(zoo)
library(geobr)
library(sf)
library(knitr)

# --- 1. Configuration Section ---
target_munis_6dig <- c("2700300")
target_years      <- 2000:2022
ALL_TARGET_YEARS  <- min(target_years):max(target_years) # <<<--- ADD THIS LINE
OUTPUT_FILENAME   <- "demographic_basis_interpolated_v15.rds" # Make sure this is updated too
RAW_TOTALS_FILENAME <- "raw_census_totals_for_validation_v14.3_PivotFix.rds" # Or update to v15
OUTPUT_DIR        <- "output"

sidra_tables_config <- list(
  estimapop    = list(table=6579,  desc="EstimaPop Est. Pop. Res.", target_var="População residente estimada"),
  pib          = list(table=5938,  desc="PIBmuni2010 GDP at Current Prices",   target_vars=c("Produto Interno Bruto a preços correntes", "Valor adicionado bruto a preços correntes da agropecuária", "Valor adicionado bruto a preços correntes da indústria", "Valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social", "Valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social", "Impostos, líquidos de subsídios, sobre produtos a preços correntes")),
  cempre_06_21 = list(table=1685,  desc="CEMPRE Local Units & Enterprises (06-21)", target_vars=c("Número de unidades locais", "Número de empresas e outras organizações atuantes", "Pessoal ocupado total", "Pessoal ocupado assalariado", "Salário médio mensal", "Salário médio mensal em reais", "Salários e outras remunerações")),
  cempre_22    = list(table=9509,  desc="CEMPRE Local Units & Enterprises (22)",    target_vars=c("Número de unidades locais", "Número de empresas e outras organizações atuantes", "Pessoal ocupado total", "Pessoal ocupado assalariado", "Salário médio mensal", "Salário médio mensal em reais", "Salários e outras remunerações")),
  censo_2000   = list(table=2093,  desc="CENSO 00 Pop. by Race/Age/Gender",  target_var="População residente"),
  censo_10_22  = list(table=9606,  desc="CENSO 10/22 Pop. by Race/Age/Gender",  target_var="População residente")
)

# IMPORTANT: Verify these keys EXACTLY match SIDRA 'Variável' names after pivot
col_rename_map <- c(
  "População residente estimada" = "pop_total_est_src",
  "Produto Interno Bruto a preços correntes" = "gdp_total_nom_brl1k",
  "Valor adicionado bruto a preços correntes da agropecuária" = "gdp_agr_nom_brl1k",
  "Valor adicionado bruto a preços correntes da indústria" = "gdp_ind_nom_brl1k",
  "Valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social" = "gdp_ser_priv_nom_brl1k",
  "Valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social" = "gdp_ser_pub_nom_brl1k",
  "Impostos, líquidos de subsídios, sobre produtos a preços correntes" = "gdp_tax_nom_brl1k",
  "Número de unidades locais" = "cempre_n_units",
  "Número de empresas e outras organizações atuantes" = "cempre_n_firms",
  "Pessoal ocupado total" = "cempre_pop_occupied",
  "Pessoal ocupado assalariado" = "cempre_pop_salaried",
  "Salário médio mensal" = "cempre_wage_avg_mw",
  "Salário médio mensal em reais" = "cempre_wage_avg_brl",
  "Salários e outras remunerações" = "cempre_wage_total_brl1k"
)

RACE_CODES <- c("Branca"="Wht", "Preta"="Blk", "Amarela"="Ylw", "Parda"="Brn", "Indígena"="Ind", "Sem declaração"="Ign", "Total"="T")
SEX_CODES  <- c("Homens"="M", "Mulheres"="W", "Total"="T")

TARGET_AGE_BANDS <- c(
  "00_04", "05_09", "10_14", "15_19", "20_24", "25_29",
  "30_39", "40_49", "50_59", "60_69", "70_79", "80p"
)
AGE_BAND_TOTAL_CODE <- "Tot_Raw_Age"

VARS_WITH_POINT_DECIMAL <- c("Salário médio mensal", "Salário médio mensal em reais")

T2093_DESIRED_AGE_STRINGS <- c(
  "0 a 4 anos", "5 a 9 anos", "10 a 14 anos", "15 a 19 anos", "20 a 24 anos", "25 a 29 anos",
  "30 a 39 anos", "40 a 49 anos", "50 a 59 anos", "60 a 69 anos", "70 a 79 anos", "80 anos ou mais"
)
T9606_DESIRED_AGE_STRINGS <- c(
  "0 a 4 anos", "5 a 9 anos", "10 a 14 anos", "15 a 19 anos", "20 a 24 anos", "25 a 29 anos",
  "30 a 34 anos", "35 a 39 anos", "40 a 44 anos", "45 a 49 anos", "50 a 54 anos", "55 a 59 anos",
  "60 a 64 anos", "65 a 69 anos", "70 a 74 anos", "75 a 79 anos", "80 a 84 anos", "85 a 89 anos",
  "90 a 94 anos", "95 a 99 anos", "100 anos ou mais"
)
CENSUS_YEARS <- c(2000, 2010, 2022)

# --- 2. Helper Functions ---
message("--- 2. Defining Helper Functions ---")
safe_numeric <- function(x_raw_char, var_name_for_decimal_rule = NULL) {
  x_raw_char <- as.character(x_raw_char)
  is_non_numeric <- x_raw_char %in% c("-", "...", "..", "", NA)
  decimal_marker <- ","
  if (!is.null(var_name_for_decimal_rule) && var_name_for_decimal_rule %in% VARS_WITH_POINT_DECIMAL) {
    decimal_marker <- "."
  }
  parsed_val <- suppressWarnings(
    readr::parse_number(x_raw_char,
                        locale = readr::locale(decimal_mark = decimal_marker,
                                               grouping_mark = ifelse(decimal_marker == ".", ",", ".")))
  )
  needs_fallback <- is.na(parsed_val) & !is_non_numeric & (decimal_marker == ",")
  if (any(needs_fallback)) {
    parsed_val_alt <- suppressWarnings(
      readr::parse_number(x_raw_char[needs_fallback],
                          locale = readr::locale(decimal_mark = ".", grouping_mark = ","))
    )
    parsed_val[needs_fallback] <- ifelse(is.na(parsed_val[needs_fallback]) & !is.na(parsed_val_alt), parsed_val_alt, parsed_val[needs_fallback])
  }
  final_val <- ifelse(is_non_numeric, NA_real_, parsed_val)
  return(final_val)
}

fetch_sidra_data <- function(tbl_config, muni_codes){
  message(paste0("Attempting fetch: ", tbl_config$table, " - ", tbl_config$desc))
  df <- tryCatch(
    sidrar::get_sidra(x = tbl_config$table, variable = "all", period = "all", geo = "City",
                      geo.filter = list("City" = muni_codes), classific = "all", category = "all", format = 3),
    error = function(e){ warning(paste0(" FAILED fetch: ", tbl_config$table, " - ", e$message)); return(NULL) }
  )
  if (!is.null(df)){
    msg_status <- if(nrow(df) > 0) paste0("SUCCESS (", nrow(df), " rows)") else "SUCCESS (0 rows)"
    message(paste0("  -> ", msg_status, ": ", tbl_config$table))
    df <- df %>% mutate(Fetched_Table_ID = as.character(tbl_config$table),
                        Fetched_Table_Desc = as.character(tbl_config$desc))
  }
  return(df)
}

standardize_keys_types <- function(df){
  if (is.null(df) || nrow(df) == 0) return(tibble())
  required_cols <- c("Município (Código)", "Ano", "Valor", "Variável")
  if (!"Fetched_Table_ID" %in% names(df)) { df$Fetched_Table_ID <- NA_character_ }
  if (!"Fetched_Table_Desc" %in% names(df)) { df$Fetched_Table_Desc <- NA_character_ }
  missing_req <- setdiff(required_cols, names(df))
  if (length(missing_req) > 0) { 
    warning("Standardize keys: Missing SIDRA cols: ", paste(missing_req, collapse=", "), " in table ", first(df$Fetched_Table_ID %||% "UnknownTable"), ". Filling with NA."); 
    for(m_col in missing_req) { df[[m_col]] <- NA_character_ } 
  }
  df$Fetched_Table_ID <- as.character(df$Fetched_Table_ID)
  df$Fetched_Table_Desc <- as.character(df$Fetched_Table_Desc)
  if (!"Valor" %in% names(df)) {
    warning("Standardize keys: 'Valor' column missing in table ", first(df$Fetched_Table_ID %||% "UnknownTable"), ".")
    df$Valor_raw <- NA_character_
    df$value <- NA_real_
  } else {
    df$Valor_raw <- as.character(df$Valor)
    df <- df %>% rowwise() %>% mutate(value = safe_numeric(Valor_raw, Variável)) %>% ungroup()
  }
  df %>%
    rename(muni_code = `Município (Código)`, ano_chr = Ano) %>%
    mutate( muni_code = stringr::str_pad(as.character(muni_code), 7, "left", "0"),
            ano = as.integer(ano_chr), Variável = trimws(as.character(Variável)) ) %>%
    select(any_of(c("muni_code", "ano", "Variável", "value", "Valor_raw", "Fetched_Table_ID", "Fetched_Table_Desc")),
           everything(), -ano_chr, -any_of("Valor"))
}

harmonize_age_band_target_schema_scalar <- function(age_str_scalar) {
  # ... (function remains the same as v14.2)
  if (!is.character(age_str_scalar) || length(age_str_scalar) != 1 || is.na(age_str_scalar)) {
    return(NA_character_)
  }
  age_str <- trimws(age_str_scalar)
  case_when(
    age_str %in% c("0 a 4 anos") ~ "00_04",
    age_str %in% c("5 a 9 anos") ~ "05_09",
    age_str %in% c("10 a 14 anos") ~ "10_14",
    age_str %in% c("15 a 19 anos") ~ "15_19",
    age_str %in% c("20 a 24 anos") ~ "20_24",
    age_str %in% c("25 a 29 anos") ~ "25_29",
    age_str == "30 a 39 anos" ~ "30_39",
    age_str == "30 a 34 anos" ~ "30_39",
    age_str == "35 a 39 anos" ~ "30_39",
    age_str == "40 a 49 anos" ~ "40_49",
    age_str == "40 a 44 anos" ~ "40_49",
    age_str == "45 a 49 anos" ~ "40_49",
    age_str == "50 a 59 anos" ~ "50_59",
    age_str == "50 a 54 anos" ~ "50_59",
    age_str == "55 a 59 anos" ~ "50_59",
    age_str == "60 a 69 anos" ~ "60_69",
    age_str == "60 a 64 anos" ~ "60_69",
    age_str == "65 a 69 anos" ~ "60_69",
    age_str == "70 a 79 anos" ~ "70_79",
    age_str == "70 a 74 anos" ~ "70_79",
    age_str == "75 a 79 anos" ~ "70_79",
    age_str == "80 anos ou mais" ~ "80p",
    age_str %in% c("80 a 84 anos", "85 a 89 anos", "90 a 94 anos", "95 a 99 anos", "100 anos ou mais") ~ "80p",
    age_str == "Total" ~ AGE_BAND_TOTAL_CODE,
    TRUE ~ NA_character_
  )
}

# --- NEW HELPER FUNCTIONS FOR GRANULAR INTERPOLATION (to be added in Section 2) ---

prepare_census_shares_for_main_script <- function(demographic_basis_df, census_years_vec, all_genders, all_races, all_age_bands) {
  message("      Sub-step: Preparing long format shares from census years for interpolation...")
  
  expected_gra_cols <- expand.grid(g=all_genders, r=all_races, a=all_age_bands) %>%
    mutate(col_name = paste0("pop_", g, "_", r, "_", a)) %>%
    pull(col_name)
  actual_gra_cols_in_data <- intersect(expected_gra_cols, names(demographic_basis_df))
  
  if(length(actual_gra_cols_in_data) == 0) {
    stop("In prepare_census_shares_for_main_script: No granular population columns found.")
  }
  
  # Denominator for shares in census years is pop_total_original_raw
  # which is equal to pop_total_est in census years due to earlier logic.
  shares_long_df <- demographic_basis_df %>%
    filter(ano %in% census_years_vec) %>%
    select(muni_code, ano, pop_total_est, # Using pop_total_est as denominator (matches original_raw for census)
           all_of(actual_gra_cols_in_data)) %>%
    pivot_longer(
      cols = all_of(actual_gra_cols_in_data),
      names_to = "gra_column",
      values_to = "pop_census_val" # These are already 0-filled from earlier step
    ) %>%
    mutate(
      gender = str_match(gra_column, "^pop_([MW])_")[,2],
      race = str_match(gra_column, "^pop_[MW]_([A-Za-z]{3})_")[,2],
      age_band = str_match(gra_column, "^pop_[MW]_[A-Za-z]{3}_(.*)$")[,2]
    ) %>%
    filter(!is.na(gender) & !is.na(race) & !is.na(age_band)) %>%
    mutate(
      share = ifelse(pop_total_est > 0, pop_census_val / pop_total_est, 0)
    ) %>%
    mutate(share = ifelse(is.na(share) & pop_total_est == 0 & pop_census_val == 0, 0, share)) %>%
    select(muni_code, ano, gender, race, age_band, share, pop_census_val) # pop_census_val needed to restore later
  
  pop_total_est_ref_df <- demographic_basis_df %>%
    select(muni_code, ano, pop_total_est) %>%
    distinct()
  
  return(list(census_shares = shares_long_df, pop_total_est_ref = pop_total_est_ref_df))
}

interpolate_shares_core <- function(group_df, # Contains ano, share for one G.R.A series
                                    all_target_years_vec,
                                    interpolation_method_type = "spline") { # Default to spline
  
  complete_years_df <- tibble(ano = all_target_years_vec)
  data_to_interp <- complete_years_df %>%
    left_join(group_df, by = "ano") # group_df should only have ano, share
  
  interpolated_share_values <- rep(NA_real_, length(all_target_years_vec))
  valid_share_points <- data_to_interp %>% filter(!is.na(share))
  
  if (nrow(valid_share_points) < 2) { 
    filled_shares <- zoo::na.locf(zoo::na.locf(data_to_interp$share, na.rm = FALSE), fromLast = TRUE, na.rm = FALSE)
    interpolated_share_values <- ifelse(is.na(filled_shares), 0, filled_shares)
  } else if (interpolation_method_type == "linear") {
    interp_result <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, xout = all_target_years_vec, rule = 2)
    interpolated_share_values <- as.numeric(interp_result) 
  } else if (interpolation_method_type == "spline") {
    if (nrow(valid_share_points) >= 2) { 
      spline_fit <- tryCatch(
        stats::splinefun(x = valid_share_points$ano, y = valid_share_points$share, method = "monoH.FC"),
        error = function(e) {
          tryCatch(stats::splinefun(x = valid_share_points$ano, y = valid_share_points$share, method = "fmm"),
                   error = function(e2) { return(NULL) }) } )
      if(!is.null(spline_fit)){ interpolated_share_values <- spline_fit(all_target_years_vec)
      } else { 
        interp_result_fallback <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, xout = all_target_years_vec, rule = 2)
        interpolated_share_values <- as.numeric(interp_result_fallback) }
    } else { 
      interp_result_fallback2 <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, xout = all_target_years_vec, rule = 2)
      interpolated_share_values <- as.numeric(interp_result_fallback2) }
  } else { stop("Unknown interpolation_method_type") }
  
  interpolated_share_values <- pmax(0, pmin(1, interpolated_share_values))
  interpolated_share_values <- ifelse(is.na(interpolated_share_values), 0, interpolated_share_values)
  return(tibble(ano = all_target_years_vec, interpolated_share = interpolated_share_values))
}

# --- 3. Fetch Raw Data ---
message("\n--- 3. Fetching Raw Data ---")
plan(multisession, workers = max(1, availableCores() - 1))
raw_data_list <- setNames(
  purrr::map( .x = sidra_tables_config,
              .f = ~fetch_sidra_data(tbl_config = .x, muni_codes = target_munis_6dig),
              .options = furrr_options(seed = TRUE) ),
  names(sidra_tables_config)
)
plan(sequential)
raw_data_list <- raw_data_list[!sapply(raw_data_list, is.null)]
message(paste("\nSuccessfully fetched data for", length(raw_data_list), "tables."))
if (length(raw_data_list) == 0) stop("No data was successfully fetched.")

# --- 4. Preprocessing & Standardizing Data ---
message("\n--- 4. Preprocessing & Standardizing Data ---")
std_data_list <- purrr::map(raw_data_list, standardize_keys_types)

# --- 4a. EstimaPop ---
message("   Processing EstimaPop...")
if (!is.null(std_data_list$estimapop) && nrow(std_data_list$estimapop) > 0) {
  estimapop_processed <- std_data_list$estimapop %>%
    filter(Variável == sidra_tables_config$estimapop$target_var, !is.na(value)) %>%
    select(muni_code, ano, pop_total_est_src = value) %>%
    distinct(muni_code, ano, .keep_all = TRUE)
} else {
  warning("EstimaPop raw data is missing or empty.")
  estimapop_processed <- tibble(muni_code = character(), ano = integer(), pop_total_est_src = numeric())
}

# --- 4b. PIB ---
message("   Processing PIB...")
if (!is.null(std_data_list$pib) && nrow(std_data_list$pib) > 0) {
  pib_raw_std <- std_data_list$pib %>%
    filter(Variável %in% sidra_tables_config$pib$target_vars, !is.na(value)) %>%
    select(muni_code, ano, Variável, value) %>%
    distinct(muni_code, ano, Variável, .keep_all = TRUE)
  
  pib_pivoted <- pib_raw_std %>% pivot_wider(names_from = Variável, values_from = value)
  
  # print("PIB pivoted names (before rename):"); print(names(pib_pivoted)) # You can keep this for one more check if you like
  
  # --- THIS IS THE KEY CHANGE FOR RENAMING ---
  # We use rename_with. For each column name (.x) that is a NAME in col_rename_map (i.e., an OLD SIDRA NAME),
  # replace it with its corresponding VALUE from col_rename_map (i.e., the NEW DESIRED NAME).
  pib_processed <- pib_pivoted %>%
    rename_with(~ col_rename_map[.x], .cols = any_of(names(col_rename_map)))
  # --- END OF KEY CHANGE ---
  
  # print("PIB processed names (after rename_with):"); print(names(pib_processed)) # You can keep for one more check
  
  # Validation check
  # expected_pib_renamed uses the VALUES of col_rename_map (the new names)
  # that correspond to the original target_vars
  expected_pib_renamed <- unname(col_rename_map[sidra_tables_config$pib$target_vars])
  expected_pib_renamed <- expected_pib_renamed[!is.na(expected_pib_renamed)] # Filter out NAs if a target_var wasn't in map keys
  
  missing_pib_cols <- setdiff(expected_pib_renamed, names(pib_processed))
  if (length(missing_pib_cols) > 0) {
    warning("PIB renaming check failed. Missing: ", paste(missing_pib_cols, collapse=", "))
    message("Expected new PIB names: ", paste(expected_pib_renamed, collapse=", "))
    message("Actual names in pib_processed: ", paste(names(pib_processed), collapse=", "))
  } else { 
    message("   PIB renaming successful.") 
  }
} else {
  warning("PIB raw data is missing or empty.")
  pib_processed <- tibble(muni_code = character(), ano = integer()) 
}

# --- 4c. CEMPRE ---
message("   Processing CEMPRE...")
if ((!is.null(std_data_list$cempre_06_21) && nrow(std_data_list$cempre_06_21) > 0) || 
    (!is.null(std_data_list$cempre_22) && nrow(std_data_list$cempre_22) > 0) ) {
  cempre_target_vars <- unique(c(sidra_tables_config$cempre_06_21$target_vars, sidra_tables_config$cempre_22$target_vars))
  cempre_raw_combined <- bind_rows(std_data_list$cempre_06_21, std_data_list$cempre_22)
  
  cempre_filtered_deduped <- cempre_raw_combined %>%
    filter(Variável %in% cempre_target_vars, !is.na(value)) %>%
    select(muni_code, ano, Variável, value, Fetched_Table_ID) %>%
    group_by(muni_code, ano, Variável) %>% 
    arrange(desc(Fetched_Table_ID)) %>% slice(1) %>% ungroup() %>%
    select(-Fetched_Table_ID)
  
  cempre_pivoted <- cempre_filtered_deduped %>% pivot_wider(names_from = Variável, values_from = value)
  
  # print("CEMPRE pivoted names (before rename):"); print(names(cempre_pivoted)) # You can keep for one more check
  
  # --- THIS IS THE KEY CHANGE FOR RENAMING ---
  cempre_processed <- cempre_pivoted %>%
    rename_with(~ col_rename_map[.x], .cols = any_of(names(col_rename_map)))
  # --- END OF KEY CHANGE ---
  
  # print("CEMPRE processed names (after rename_with):"); print(names(cempre_processed)) # You can keep for one more check
  
  # Validation check
  expected_cempre_renamed <- unname(col_rename_map[cempre_target_vars])
  expected_cempre_renamed <- expected_cempre_renamed[!is.na(expected_cempre_renamed)]
  
  missing_cempre_cols <- setdiff(expected_cempre_renamed, names(cempre_processed))
  if (length(missing_cempre_cols) > 0) {
    warning("CEMPRE renaming check failed. Missing: ", paste(missing_cempre_cols, collapse=", "))
    message("Expected new CEMPRE names: ", paste(expected_cempre_renamed, collapse=", "))
    message("Actual names in cempre_processed: ", paste(names(cempre_processed), collapse=", "))
  } else { 
    message("   CEMPRE renaming successful.") 
  }
} else {
  warning("CEMPRE raw data is missing or empty.")
  cempre_processed <- tibble(muni_code = character(), ano = integer()) 
}

# --- 4d. Census Data Processing ---
message("   Processing Census data...")
census_raw_combined_list <- list()
if (!is.null(std_data_list$censo_2000) && nrow(std_data_list$censo_2000) > 0) {
  census_raw_combined_list$censo_2000 <- std_data_list$censo_2000 %>% filter(ano == 2000)
}
if (!is.null(std_data_list$censo_10_22) && nrow(std_data_list$censo_10_22) > 0) {
  census_raw_combined_list$censo_10_22 <- std_data_list$censo_10_22 %>% filter(ano %in% c(2010, 2022))
}
census_raw_combined <- bind_rows(census_raw_combined_list)

if (nrow(census_raw_combined) == 0) {
  warning("No combined census data for specified years. Skipping census processing."); 
  census_processed_for_pivot <- tibble(); raw_census_totals <- tibble()
} else {
  census_standardized <- census_raw_combined %>%
    filter(Variável == sidra_tables_config$censo_2000$target_var, !is.na(value)) %>%
    filter(if ("Situação do domicílio" %in% names(.)) (ano == 2000 & `Situação do domicílio` == "Total") | ano != 2000 else TRUE) %>%
    mutate(value = round(as.numeric(value)))
  
  census_age_filtered <- census_standardized %>%
    mutate(age_orig_raw = as.character(coalesce(Idade, `Grupo de idade`))) %>%
    filter(
      (ano == 2000 & age_orig_raw %in% c(T2093_DESIRED_AGE_STRINGS, "Total")) |
        (ano %in% c(2010, 2022) & age_orig_raw %in% c(T9606_DESIRED_AGE_STRINGS, "Total"))
    )
  
  census_harmonized <- census_age_filtered %>%
    transmute(muni_code, ano, gender_orig = Sexo, race_orig = `Cor ou raça`,
              age_orig = age_orig_raw, pop = value ) %>%
    mutate( gender = recode(gender_orig, !!!SEX_CODES, .default = NA_character_),
            race = recode(race_orig, !!!RACE_CODES, .default = NA_character_),
            age_band_target = purrr::map_chr(age_orig, harmonize_age_band_target_schema_scalar) ) %>%
    filter(!is.na(gender), !is.na(race), !is.na(age_band_target), !is.na(pop))
  
  raw_census_totals <- census_harmonized %>%
    filter(gender == "T", race == "T", age_band_target == AGE_BAND_TOTAL_CODE) %>%
    select(muni_code, ano, pop_total_original_raw = pop) %>%
    group_by(muni_code, ano) %>% 
    summarise(pop_total_original_raw = sum(pop_total_original_raw, na.rm = TRUE), .groups = "drop")
  
  # --- BEGIN: Redistribute "Ign" race for 2000 & Final Aggregation ---
  message("      Redistributing 'Sem declaração' (Ign) race for 2000 census data and aggregating...")
  census_aggregated_to_target_list <- list() # To store parts before final bind & summarise
  
  # Process 2000 data (redistribution of 'Ign')
  census_2000_data_for_redistrib <- census_harmonized %>% 
    filter(ano == 2000, gender != "T", race != "T", age_band_target %in% TARGET_AGE_BANDS)
  
  if(nrow(census_2000_data_for_redistrib) > 0 && "Ign" %in% unique(census_2000_data_for_redistrib$race)) {
    census_2000_ign_counts <- census_2000_data_for_redistrib %>% filter(race == "Ign") %>%
      select(muni_code, ano, gender, age_band_target, pop_ign = pop) %>%
      group_by(muni_code, ano, gender, age_band_target) %>% # Ensure ign counts are unique per group
      summarise(pop_ign = sum(pop_ign, na.rm = TRUE), .groups = "drop")
    
    census_2000_declared_props <- census_2000_data_for_redistrib %>% 
      filter(race != "Ign") %>%
      group_by(muni_code, ano, gender, age_band_target, race) %>% # Ensure declared counts are unique
      summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
      group_by(muni_code, ano, gender, age_band_target) %>%
      mutate(total_pop_declared_group = sum(pop, na.rm = TRUE)) %>%
      ungroup() %>%
      mutate(prop_declared_group = ifelse(total_pop_declared_group == 0, 0, pop / total_pop_declared_group))
    
    census_2000_redistributed_df <- census_2000_declared_props %>%
      left_join(census_2000_ign_counts, by = c("muni_code", "ano", "gender", "age_band_target")) %>%
      mutate(
        pop_ign = ifelse(is.na(pop_ign), 0, pop_ign),
        pop_added_from_ign = round(prop_declared_group * pop_ign),
        pop_final = pop + pop_added_from_ign
      ) %>%
      select(muni_code, ano, gender, race, age_band_target, pop = pop_final)
    
    census_aggregated_to_target_list[[ "y2000" ]] <- census_2000_redistributed_df
    message("      Finished 'Ign' redistribution for 2000.")
  } else {
    message("      Skipping 'Ign' redistribution for 2000 (no 'Ign' data or no 2000 data).")
    census_aggregated_to_target_list[[ "y2000" ]] <- census_harmonized %>%
      filter(ano == 2000, gender != "T", race != "T", age_band_target %in% TARGET_AGE_BANDS) %>%
      filter(race != "Ign") %>% 
      group_by(muni_code, ano, gender, race, age_band_target) %>%
      summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")
  }
  
  # Process other census years (2010, 2022)
  census_data_other_years_df <- census_harmonized %>%
    filter(ano %in% c(2010, 2022), gender != "T", race != "T", age_band_target %in% TARGET_AGE_BANDS) %>%
    group_by(muni_code, ano, gender, race, age_band_target) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")
  census_aggregated_to_target_list[[ "other_years" ]] <- census_data_other_years_df
  
  # Combine all parts and ensure final uniqueness by re-aggregating
  census_aggregated_to_target <- bind_rows(census_aggregated_to_target_list) %>%
    group_by(muni_code, ano, gender, race, age_band_target) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")
  # --- END: Final Aggregation ---
  
  census_processed_for_pivot <- census_aggregated_to_target
  
  message("      Running internal census validation (Sum of Target Bands vs Raw Total)...")
  if(nrow(census_aggregated_to_target) > 0 && nrow(raw_census_totals) > 0) {
    census_summed_check <- census_aggregated_to_target %>%
      group_by(muni_code, ano) %>%
      summarise(pop_summed_target_bands = sum(pop, na.rm=TRUE), .groups="drop") %>%
      left_join(raw_census_totals, by = c("muni_code", "ano")) %>%
      mutate( pop_total_original_raw = coalesce(pop_total_original_raw, 0L),
              diff = pop_summed_target_bands - pop_total_original_raw ) %>%
      filter( (ano == 2000 & abs(diff) > (length(TARGET_AGE_BANDS)*2)) | (ano != 2000 & abs(diff) > 1) ) # Adjusted 2000 diff tolerance
    
    if(nrow(census_summed_check) > 0) {
      warning("--> Internal Census Check FAILED: Sum of target bands does not match raw total for:")
      print(knitr::kable(census_summed_check))
    } else {
      message("      Internal Census Check OK.")
    }
  } else {
    message("      Skipping internal census validation: not enough data.")
  }
}

# --- 5. Base Grid & Generation of `pop_total_est` ---
# ... (Remains the same as v14.2) ...
message("\n--- 5. Base Grid & Generating `pop_total_est` (Prioritizing Census over EstimaPop) ---")
base_grid_pop <- expand_grid(muni_code = unique(target_munis_6dig), ano = min(target_years):max(target_years))
pop_combined_series <- base_grid_pop %>%
  left_join(estimapop_processed, by = c("muni_code", "ano"))

if (exists("raw_census_totals") && nrow(raw_census_totals) > 0) {
  pop_combined_series <- pop_combined_series %>%
    left_join(raw_census_totals, by = c("muni_code", "ano")) %>%
    mutate(
      pop_to_interpolate = ifelse(ano %in% CENSUS_YEARS & !is.na(pop_total_original_raw),
                                  pop_total_original_raw,
                                  pop_total_est_src)
    ) %>%
    select(muni_code, ano, pop_to_interpolate)
} else {
  warning("Raw census totals not available. `pop_total_est` will rely solely on EstimaPop.")
  pop_combined_series <- pop_combined_series %>%
    rename(pop_to_interpolate = pop_total_est_src) %>%
    select(muni_code, ano, pop_to_interpolate)
}

pop_total_est_final_df <- pop_combined_series %>%
  group_by(muni_code) %>% arrange(ano) %>%
  mutate(
    pop_total_est_decimal = zoo::na.approx(pop_to_interpolate, na.rm = FALSE, rule = 2),
    pop_total_est = round(pop_total_est_decimal) # <<<--- INTEGERIZE pop_total_est
  ) %>%
  ungroup() %>%
  filter(ano %in% target_years) %>%
  select(muni_code, ano, pop_total_est) # Keep the integerized version

if (any(is.na(pop_total_est_final_df$pop_total_est))) {
  warning("NAs found in final pop_total_est after interpolation. Check source data coverage for muni_code(s): ",
          paste(unique(pop_total_est_final_df$muni_code[is.na(pop_total_est_final_df$pop_total_est)]), collapse=", "))
}


# --- 6. Pivot Processed Census Data ---
message("\n--- 6. Pivoting Census Data to Wide Format (Target Schema) ---")
if (exists("census_processed_for_pivot") && nrow(census_processed_for_pivot) > 0) {
  # 'Ign' race should have been handled (redistributed for 2000, not present for 2010/22 from source)
  # census_processed_for_pivot should already be free of 'Ign' race category.
  # A final check / filter can be added if there's doubt.
  data_to_pivot <- census_processed_for_pivot %>% 
    filter(race != "Ign") # This should be redundant if previous steps are correct
  
  # DEBUG: Check for duplicates before pivot
  # duplicated_pivot_keys <- data_to_pivot %>%
  #    group_by(muni_code, ano, gender, race, age_band_target) %>%
  #    filter(n() > 1) %>%
  #    ungroup()
  # if (nrow(duplicated_pivot_keys) > 0) {
  #    warning("Duplicate keys found in `data_to_pivot` BEFORE creating col_name. This will lead to pivot_wider error.")
  #    print(head(duplicated_pivot_keys))
  #    # stop("Stopping due to duplicate keys before pivot.")
  # }
  
  pop_wide_census <- data_to_pivot %>%
    mutate(col_name = paste0("pop", "_", gender, "_", race, "_", age_band_target)) %>%
    # DEBUG: Check for duplicates in col_name
    #    group_by(muni_code, ano, col_name) %>%
    #    filter(n() > 1) %>%
    #    { if(nrow(.) > 0) { print("Duplicates for col_name found:"); print(head(.))}; .} %>%
    #    ungroup() %>%
    select(muni_code, ano, col_name, pop) %>%
    pivot_wider(names_from = col_name, values_from = pop, values_fill = NA_real_) # Ensure values_fill is a single value of the correct type
} else { 
  pop_wide_census <- tibble(muni_code = character(), ano = integer()) # Ensure it's a tibble for join
  warning("No processed census data to pivot.") 
}

# --- 7. Final Join ---
# ... (Remains the same as v14.2) ...
message("\n--- 7. Joining Annual Data and Wide Census Data ---")
final_base_grid <- expand_grid(muni_code = target_munis_6dig, ano = target_years)
list_to_join <- list(final_base_grid)
if (exists("pop_total_est_final_df") && nrow(pop_total_est_final_df) > 0) list_to_join <- c(list_to_join, list(pop_total_est_final_df))
if (exists("pib_processed") && nrow(pib_processed) > 0 && ncol(pib_processed) > 2) list_to_join <- c(list_to_join, list(pib_processed)) # ncol check
if (exists("cempre_processed") && nrow(cempre_processed) > 0 && ncol(cempre_processed) > 2) list_to_join <- c(list_to_join, list(cempre_processed)) # ncol check
if (exists("pop_wide_census") && nrow(pop_wide_census) > 0 && ncol(pop_wide_census) > 2) list_to_join <- c(list_to_join, list(pop_wide_census))
if (exists("raw_census_totals") && nrow(raw_census_totals) > 0) list_to_join <- c(list_to_join, list(raw_census_totals))

if (length(list_to_join) > 1) {
  demographic_basis_intermediate <- reduce(list_to_join, full_join, by = c("muni_code", "ano"))
} else {
  demographic_basis_intermediate <- final_base_grid; warning("Only base grid available for final join.")
}

# --- 8. Finalizing Data ---
# ... (Remains the same as v14.2) ...
message("\n--- 8. Finalizing Data (Adding Geo Info, Reordering) ---")
geo_info <- tryCatch({ 
  geobr::read_municipality(code_muni="all",year=2020,showProgress=FALSE) %>% 
    sf::st_drop_geometry() %>% 
    transmute(muni_code=as.character(code_muni),muni_name=name_muni,uf_code=as.character(code_state),uf_abbr=abbrev_state)
}, error=function(e){warning("Fail geobr::read_municipality: ",e$message);NULL})
state_info <- tryCatch({ 
  geobr::read_state(code_state="all",year=2020,showProgress=FALSE) %>% 
    sf::st_drop_geometry() %>% 
    transmute(uf_code=as.character(code_state),uf_name=name_state)
}, error=function(e){warning("Fail geobr::read_state: ",e$message);NULL})

if(!is.null(geo_info) && !is.null(state_info)){
  demographic_basis_final <- demographic_basis_intermediate %>%
    left_join(geo_info, by = "muni_code") %>%
    left_join(state_info, by = "uf_code") %>%
    select(-any_of("uf_code"))
} else {
  warning("Could not join geo info. Columns muni_name, uf_abbr, uf_name might be missing or all NA."); 
  demographic_basis_final <- demographic_basis_intermediate
  if(!"muni_name" %in% names(demographic_basis_final)) demographic_basis_final$muni_name <- NA_character_
  if(!"uf_abbr" %in% names(demographic_basis_final)) demographic_basis_final$uf_abbr <- NA_character_
  if(!"uf_name" %in% names(demographic_basis_final)) demographic_basis_final$uf_name <- NA_character_
}

key_cols_relocate <- c("muni_code", "muni_name", "uf_abbr", "uf_name", "ano", "pop_total_est", "pop_total_original_raw")
demographic_basis_final <- demographic_basis_final %>%
  mutate(muni_code = stringr::str_pad(muni_code, 7, "left", "0")) %>%
  relocate(any_of(key_cols_relocate))

key_cols <- c("muni_code", "muni_name", "uf_abbr", "uf_name", "ano")
pop_cols_order <- c("pop_total_est", "pop_total_original_raw")
gdp_cols <- sort(grep("^gdp_", names(demographic_basis_final), value = TRUE))
cempre_cols <- sort(grep("^cempre_", names(demographic_basis_final), value = TRUE))
declared_races_for_pattern <- RACE_CODES[RACE_CODES != "T" & RACE_CODES != "Ign"]
pop_detail_pattern_target <- paste0("^pop_[MW]_(", paste(declared_races_for_pattern, collapse="|"), ")_(", paste(TARGET_AGE_BANDS, collapse="|"), ")$")
pop_detail_cols  <- sort(grep(pop_detail_pattern_target, names(demographic_basis_final), value = TRUE))
final_col_order <- unique(c(
  intersect(key_cols, names(demographic_basis_final)), 
  intersect(pop_cols_order, names(demographic_basis_final)), 
  intersect(gdp_cols, names(demographic_basis_final)), 
  intersect(cempre_cols, names(demographic_basis_final)), 
  intersect(pop_detail_cols, names(demographic_basis_final))
))
other_cols <- setdiff(names(demographic_basis_final), final_col_order)
final_col_order <- c(final_col_order, sort(other_cols))
final_col_order_existing <- intersect(final_col_order, names(demographic_basis_final))
if (length(final_col_order_existing) > 0) { # only select if there are columns to select
  demographic_basis_final <- demographic_basis_final %>% select(all_of(final_col_order_existing))
}

message("      Converting NA to 0 for granular pop columns in CENSUS years...")
# Identify granular population columns (excluding pop_total_est and pop_total_original_raw)
granular_pop_cols_to_mutate <- names(demographic_basis_final)[grep("^pop_[MW]_", names(demographic_basis_final))]

if (length(granular_pop_cols_to_mutate) > 0) {
  demographic_basis_final <- demographic_basis_final %>%
    mutate(across(all_of(granular_pop_cols_to_mutate), 
                  ~ ifelse(ano %in% CENSUS_YEARS & is.na(.), 0, .)))
  message("      Done converting NAs to 0 for granular pop columns in census years.")
} else {
  message("      No granular pop columns found to convert NAs to 0.")
}

# --- VALIDATION Sections ---
# ... (Remain the same as v14.2) ...
message("   Validating final 'pop_total_est' against original EstimaPop source (for non-census, non-interpolated points)...")
if (exists("estimapop_processed") && "pop_total_est_src" %in% names(estimapop_processed) && 
    "pop_total_est" %in% names(demographic_basis_final)) {
  pop_est_source_check_df <- demographic_basis_final %>%
    select(muni_code, ano, pop_total_est_final = pop_total_est) %>%
    inner_join(estimapop_processed %>% filter(!is.na(pop_total_est_src)), by = c("muni_code", "ano")) %>%
    filter(!(ano %in% CENSUS_YEARS)) %>%
    mutate(diff = pop_total_est_final - pop_total_est_src) %>%
    filter(abs(diff) > 0.1)
  if(nrow(pop_est_source_check_df) > 0) {
    warning("--> VALIDATION ISSUE: Final 'pop_total_est' differs from original EstimaPop source values for some non-census, non-interpolated rows.")
    print(knitr::kable(pop_est_source_check_df))
  } else {
    message("   Validation OK: Final 'pop_total_est' consistent with original EstimaPop source values (for non-census, non-interpolated points).")
  }
} else { warning("Could not perform 'pop_total_est' vs EstimaPop source validation.") }

message("   Validating 'pop_total_est' vs 'pop_total_original_raw' for CENSUS years...")
if ("pop_total_est" %in% names(demographic_basis_final) && "pop_total_original_raw" %in% names(demographic_basis_final)) {
  census_pop_comparison_df <- demographic_basis_final %>%
    filter(ano %in% CENSUS_YEARS) %>% 
    select(muni_code, ano, pop_total_est, pop_total_original_raw) %>%
    filter(!is.na(pop_total_est) & !is.na(pop_total_original_raw)) %>%
    mutate(diff_abs = abs(pop_total_est - pop_total_original_raw),
           diff_pct = ifelse(pop_total_original_raw != 0, round(diff_abs / pop_total_original_raw * 100, 4), NA_real_))
  if(any(census_pop_comparison_df$diff_abs > 1, na.rm=TRUE) ){
    warning("--> VALIDATION ISSUE: 'pop_total_est' still differs from 'pop_total_original_raw' in CENSUS years. This should not happen.")
    print(knitr::kable(census_pop_comparison_df %>% filter(diff_abs > 1)))
  } else {
    message("   Validation OK: 'pop_total_est' perfectly matches 'pop_total_original_raw' in CENSUS years.")
  }
} else { warning("Could not perform 'pop_total_est' vs 'pop_total_original_raw' validation.") }


# --- 8b. Interpolate Granular Demographic Groups for Non-Census Years ---
message("\n--- 8b. Interpolating Granular Demographic Groups (Spline Method) ---")

# Extract Genders, Races, Age Bands from the current demographic_basis_final
# This ensures it uses the columns that actually exist after all prior processing.
current_pop_cols <- names(demographic_basis_final)[str_starts(names(demographic_basis_final), "pop_") & 
                                                     !names(demographic_basis_final) %in% c("pop_total_est", "pop_total_original_raw")]

if(length(current_pop_cols) > 0) {
  parsed_current_cols <- str_match(current_pop_cols, "^pop_([MW])_([A-Za-z]{3})_([0-9]{2}_[0-9]{2}|[0-9]{2}p|80p)$")
  # Ensure V1 (full match) is not all NA before proceeding, which na.omit would handle
  # Also ensure we have expected columns after str_match
  if (is.null(parsed_current_cols) || ncol(parsed_current_cols) < 4) {
    stop("Failed to parse G/R/A from column names in Section 8b. Check current_pop_cols and str_match regex.")
  }
  parsed_current_cols_df <- na.omit(as.data.frame(parsed_current_cols[,1:4, drop=FALSE])) # Keep as df even if one row
  names(parsed_current_cols_df) <- c("original_col", "V2", "V3", "V4") # Assign temp names for clarity
  
  if(nrow(parsed_current_cols_df) == 0 || any(!nzchar(parsed_current_cols_df$V2)) || any(!nzchar(parsed_current_cols_df$V3)) || any(!nzchar(parsed_current_cols_df$V4)) ) {
    warning("Could not reliably parse G/R/A from all pop_ columns in Section 8b. Skipping granular interpolation.")
    # To prevent erroring later, we ensure an empty tibble if things go wrong here for the 'else' part.
    # However, the main `if(length(current_pop_cols) > 0)` should catch most issues.
  } else {
    CURRENT_GENDERS <- unique(parsed_current_cols_df$V2)
    CURRENT_RACES   <- unique(parsed_current_cols_df$V3) # Should not contain 'Ign'
    CURRENT_AGES    <- unique(parsed_current_cols_df$V4)
    
    # Prepare data for interpolation (shares from census years)
    prepared_interp_data <- prepare_census_shares_for_main_script(
      demographic_basis_final, 
      CENSUS_YEARS,
      CURRENT_GENDERS, 
      CURRENT_RACES, 
      CURRENT_AGES
    )
    
    census_shares_df <- prepared_interp_data$census_shares
    pop_total_est_ref_df <- prepared_interp_data$pop_total_est_ref # This has INTEGER pop_total_est
    
    message("      Interpolating shares for each G.R.A group using spline...")
    all_interpolated_shares <- census_shares_df %>%
      # Ensure only necessary columns are grouped for reframe if census_shares_df has extra cols
      select(muni_code, gender, race, age_band, ano, share) %>% 
      group_by(muni_code, gender, race, age_band) %>%
      reframe(interpolate_shares_core(
        group_df = pick(ano, share), 
        all_target_years_vec = ALL_TARGET_YEARS, 
        interpolation_method_type = "spline"
      )) %>%
      ungroup()
    message("      Finished interpolating shares.")
    
    message("      Calculating and normalizing (decimal) interpolated counts...")
    # This produces pop_interpolated_normalized (still decimal)
    interpolated_counts_long_decimal <- all_interpolated_shares %>%
      left_join(pop_total_est_ref_df, by = c("muni_code", "ano")) %>% # pop_total_est here is INTEGER
      mutate(initial_pop = interpolated_share * pop_total_est) %>% # initial_pop is decimal
      mutate(initial_pop = ifelse(initial_pop < 0, 0, initial_pop)) %>%
      group_by(muni_code, ano) %>%
      mutate(
        current_sum_granular = sum(initial_pop, na.rm = TRUE),
        adjustment_factor = ifelse(current_sum_granular == 0 & pop_total_est == 0, 1, # Avoid 0/0 if pop_total_est is 0
                                   ifelse(current_sum_granular == 0 & pop_total_est > 0, 0, # All shares were 0
                                          pop_total_est / current_sum_granular)),
        # pop_interpolated_normalized is decimal, but its sum matches integer pop_total_est
        pop_interpolated_normalized = initial_pop * adjustment_factor 
      ) %>%
      ungroup() %>%
      # Keep pop_total_est for the integerization step
      select(muni_code, ano, gender, race, age_band, pop_interpolated_normalized, pop_total_est) 
    
    # --- BEGIN INSERTED INTEGERIZATION LOGIC ---
    message("      Integerizing granular populations while preserving total...")
    interpolated_counts_integerized <- interpolated_counts_long_decimal %>%
      group_by(muni_code, ano) %>% # pop_total_est is already grouped by muni_code, ano
      mutate(
        pop_rounded = round(pop_interpolated_normalized),
        sum_pop_rounded = sum(pop_rounded, na.rm = TRUE),
        # pop_total_est is the authoritative INTEGER total for the year/muni from pop_total_est_ref_df
        diff_to_distribute = pop_total_est - sum_pop_rounded, 
        remainder = pop_interpolated_normalized - floor(pop_interpolated_normalized)
      ) %>%
      arrange(muni_code, ano, 
              ifelse(diff_to_distribute > 0, desc(remainder), remainder), 
              desc(pop_interpolated_normalized)) %>%
      mutate(
        adjustment_value = ifelse(row_number() <= abs(diff_to_distribute), sign(diff_to_distribute), 0),
        pop_integer_final = pop_rounded + adjustment_value
      ) %>%
      ungroup() %>%
      select(muni_code, ano, gender, race, age_band, pop_integer_final)
    # --- END INSERTED INTEGERIZATION LOGIC ---
    
    # Pivot interpolated data (now integer) to wide format
    interpolated_counts_wide <- interpolated_counts_integerized %>% # Use the integerized version
      mutate(col_name = paste0("pop_", gender, "_", race, "_", age_band)) %>%
      select(muni_code, ano, col_name, pop_integer_final) %>% 
      pivot_wider(names_from = col_name, values_from = pop_integer_final)
    
    message("      Preparing to merge interpolated data back into main dataframe...")
    interp_gra_col_names <- names(interpolated_counts_wide)[!names(interpolated_counts_wide) %in% c("muni_code", "ano")]
    
    df_census_years <- demographic_basis_final %>%
      filter(ano %in% CENSUS_YEARS)
    
    df_non_census_years_base <- demographic_basis_final %>%
      filter(!(ano %in% CENSUS_YEARS)) %>%
      select(-any_of(interp_gra_col_names)) 
    
    df_non_census_years_interpolated <- df_non_census_years_base %>%
      left_join(interpolated_counts_wide, by = c("muni_code", "ano"))
    
    all_expected_cols <- names(demographic_basis_final)
    
    for(col_name in all_expected_cols) {
      if (!col_name %in% names(df_census_years)) {
        df_census_years[[col_name]] <- NA_integer_ # Use NA_integer_ for pop columns
      }
      if (!col_name %in% names(df_non_census_years_interpolated)) {
        df_non_census_years_interpolated[[col_name]] <- NA_integer_
      }
    }
    
    # Ensure consistent column types for G.R.A columns before bind_rows
    # (census year G.R.A columns are already integer from NA to 0 conversion or original data)
    # (interpolated G.R.A columns are now integer)
    for(col_name in interp_gra_col_names){
      if(col_name %in% names(df_census_years)) df_census_years[[col_name]] <- as.integer(df_census_years[[col_name]])
      if(col_name %in% names(df_non_census_years_interpolated)) df_non_census_years_interpolated[[col_name]] <- as.integer(df_non_census_years_interpolated[[col_name]])
    }
    
    df_census_years <- df_census_years %>% select(all_of(all_expected_cols))
    df_non_census_years_interpolated <- df_non_census_years_interpolated %>% select(all_of(all_expected_cols))
    
    demographic_basis_final <- bind_rows(df_census_years, df_non_census_years_interpolated) %>%
      arrange(muni_code, ano)
    
    message("      Granular demographics interpolated and merged.")
    
    message("      Performing quick validation of interpolated sums for non-census years...")
    if (length(interp_gra_col_names) > 0) {
      interp_validation <- demographic_basis_final %>%
        filter(!(ano %in% CENSUS_YEARS)) %>%
        filter(!is.na(pop_total_est))
      
      existing_gra_cols_for_sum <- intersect(interp_gra_col_names, names(interp_validation))
      
      if(length(existing_gra_cols_for_sum) > 0) {
        interp_validation <- interp_validation %>%
          mutate(sum_granular_check = rowSums(select(., all_of(existing_gra_cols_for_sum)), na.rm = TRUE)) %>%
          mutate(disc_interp = sum_granular_check - pop_total_est) # pop_total_est is now integer
        
        if(any(interp_validation$disc_interp != 0, na.rm = TRUE)){ # Should be exactly 0
          warning("--> Post-interpolation sum check FAILED for some non-census years (integer mismatch).")
          print(knitr::kable(interp_validation %>% 
                               filter(disc_interp != 0) %>% 
                               select(muni_code, ano, pop_total_est, sum_granular_check, disc_interp)))
        } else {
          message("      Post-interpolation sum check OK for non-census years (integers match).")
        }
      } else {
        message("      Skipping sum_granular_check as no granular columns were found in non-census year data for validation.")
      }
    } else {
      message("      Skipping post-interpolation sum check as no granular column names were identified for summing.")
    }
  } # End of the 'else' for G/R/A parsing check
} else { 
  message("      Skipping granular interpolation as no granular pop columns were identified in the input demographic_basis_final.")
}



# --- 9. Saving Output ---
# ... (Remains the same as v14.2) ...
# Modify filename for this new interpolated dataset
OUTPUT_FILENAME   <- "demographic_basis_interpolated_v15.rds" # Change this at the top of script
message("\n--- 9. Saving Output ---")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)
output_path <- file.path(OUTPUT_DIR, OUTPUT_FILENAME)
saveRDS(demographic_basis_final, file = output_path)
message("Saved final dataset (Target Schema) -> ", output_path); message("Dimensions: ", nrow(demographic_basis_final), " rows x ", ncol(demographic_basis_final), " columns")
if (exists("raw_census_totals") && nrow(raw_census_totals) > 0) {
  raw_totals_path <- file.path(OUTPUT_DIR, RAW_TOTALS_FILENAME); 
  saveRDS(raw_census_totals, file = raw_totals_path); 
  message("Saved raw census totals for validation -> ", raw_totals_path)
} else { message("No raw census totals available to save.") }

message("\n--- Note on 'Ignorado' Race Category (2000) ---")
message("For the 2000 census, 'Sem declaração' (Ignored) race counts were proportionally redistributed")
message("among other declared race categories. Thus, 'Ign' columns will not appear for 2000.")
message("For 2010 and 2022, 'Sem declaração' is not reported by SIDRA table 9606, so no 'Ign' columns exist.")

# --- End of Script ---
message("\n--- Demographic Basis Script Finished (v14.3 - Target Schema - PivotFix) ---")











