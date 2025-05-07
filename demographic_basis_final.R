# =======================================================================
# Demographic Basis Pipeline Function - v1.0
# =======================================================================
# Purpose:
#   Creates a comprehensive, municipality-level demographic and economic
#   basis for a specified geographic scope and time range.
#   Features:
#     - Fetches data from IBGE SIDRA.
#     - Processes population (census, EstimaPop), GDP, and CEMPRE data.
#     - Harmonizes census data to 12 target age bands.
#     - Redistributes "Sem declaração" race for 2000 census.
#     - Interpolates total population (pop_total_est) to be integer.
#     - Interpolates granular demographic groups (gender, race, age)
#       for non-census years using spline interpolation of shares,
#       followed by integerization and normalization to pop_total_est.
#     - Allows selection of geographic scope (Brazil or specific UFs)
#       and output year range.
# =======================================================================

# --- 0. Load Libraries ---
suppressPackageStartupMessages({
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
})

# --- I. Global Configurations & Constants (Defined outside the main function) ---
message("--- Loading Global Configurations ---")

# SIDRA Table Configuration
SIDRA_TABLES_CONFIG <- list(
  estimapop    = list(table=6579,  desc="EstimaPop Est. Pop. Res.", 
                      target_var="População residente estimada"),
  pib          = list(table=5938,  desc="PIBmuni2010 GDP at Current Prices",   
                      target_vars=c("Produto Interno Bruto a preços correntes", 
                                    "Valor adicionado bruto a preços correntes da agropecuária", 
                                    "Valor adicionado bruto a preços correntes da indústria", 
                                    "Valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social", 
                                    "Valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social", 
                                    "Impostos, líquidos de subsídios, sobre produtos a preços correntes")),
  cempre_06_21 = list(table=1685,  desc="CEMPRE Local Units & Enterprises (06-21)", 
                      target_vars=c("Número de unidades locais", 
                                    "Número de empresas e outras organizações atuantes", 
                                    "Pessoal ocupado total", "Pessoal ocupado assalariado", 
                                    "Salário médio mensal", "Salário médio mensal em reais", 
                                    "Salários e outras remunerações")),
  cempre_22    = list(table=9509,  desc="CEMPRE Local Units & Enterprises (22)",    
                      target_vars=c("Número de unidades locais", 
                                    "Número de empresas e outras organizações atuantes", 
                                    "Pessoal ocupado total", "Pessoal ocupado assalariado", 
                                    "Salário médio mensal", "Salário médio mensal em reais", 
                                    "Salários e outras remunerações")),
  censo_2000   = list(table=2093,  desc="CENSO 00 Pop. by Race/Age/Gender",  
                      target_var="População residente"),
  censo_10_22  = list(table=9606,  desc="CENSO 10/22 Pop. by Race/Age/Gender",  
                      target_var="População residente")
)

# Column Rename Map (Old SIDRA Name = New Desired Name)
COL_RENAME_MAP <- c(
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

# Demographic Codes
RACE_CODES <- c("Branca"="Wht", "Preta"="Blk", "Amarela"="Ylw", "Parda"="Brn", "Indígena"="Ind", "Sem declaração"="Ign", "Total"="T")
SEX_CODES  <- c("Homens"="M", "Mulheres"="W", "Total"="T")

# Age Band Definitions
TARGET_AGE_BANDS <- c("00_04", "05_09", "10_14", "15_19", "20_24", "25_29", 
                      "30_39", "40_49", "50_59", "60_69", "70_79", "80p")
AGE_BAND_TOTAL_CODE <- "Tot_Raw_Age" # For extracting raw age totals from census

# SIDRA Variables known to use '.' as decimal
VARS_WITH_POINT_DECIMAL <- c("Salário médio mensal", "Salário médio mensal em reais")

# Census Year Strings for Filtering (to avoid double counting from source tables)
T2093_DESIRED_AGE_STRINGS <- c("0 a 4 anos", "5 a 9 anos", "10 a 14 anos", "15 a 19 anos", 
                               "20 a 24 anos", "25 a 29 anos", "30 a 39 anos", "40 a 49 anos", 
                               "50 a 59 anos", "60 a 69 anos", "70 a 79 anos", "80 anos ou mais")
T9606_DESIRED_AGE_STRINGS <- c("0 a 4 anos", "5 a 9 anos", "10 a 14 anos", "15 a 19 anos", 
                               "20 a 24 anos", "25 a 29 anos", "30 a 34 anos", "35 a 39 anos", 
                               "40 a 44 anos", "45 a 49 anos", "50 a 54 anos", "55 a 59 anos", 
                               "60 a 64 anos", "65 a 69 anos", "70 a 74 anos", "75 a 79 anos", 
                               "80 a 84 anos", "85 a 89 anos", "90 a 94 anos", "95 a 99 anos", 
                               "100 anos ou mais")

# Master Census Years - these are the anchor points for interpolation of shares
MASTER_CENSUS_YEARS <- c(2000, 2010, 2022)
# Year for geobr calls to get municipality lists (contemporary)
GEOBR_MUNI_LIST_YEAR <- 2022 


# --- II. Helper Functions (Defined globally or sourced from another file) ---
message("--- Defining Helper Functions ---")

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

fetch_sidra_data_helper <- function(tbl_config, muni_codes_vec){ # Renamed to avoid conflict if script is sourced
  # Using a local message to avoid being too verbose if called many times in a loop
  # message(paste0("Attempting fetch: ", tbl_config$table, " for ", length(muni_codes_vec), " munis."))
  # Add a small delay to be nice to the API if fetching in rapid succession (e.g. by UF)
  # Sys.sleep(runif(1, 0.5, 1.5)) 
  
  # SIDRA API can be slow or error-prone for very large requests.
  # Consider chunking muni_codes_vec if length > ~500 for a single call.
  # For now, assume sidrar handles it or the number of munis per call is manageable.
  
  df <- tryCatch(
    sidrar::get_sidra(x = tbl_config$table, variable = "all", period = "all", geo = "City",
                      geo.filter = list("City" = muni_codes_vec), classific = "all", category = "all", format = 3),
    error = function(e){ 
      warning(paste0(" FAILED fetch for table: ", tbl_config$table, 
                     " (", tbl_config$desc, "). Error: ", e$message), call. = FALSE); 
      return(NULL) 
    }
  )
  if (!is.null(df) && nrow(df) > 0){
    df <- df %>% mutate(Fetched_Table_ID = as.character(tbl_config$table),
                        Fetched_Table_Desc = as.character(tbl_config$desc))
  } else if (!is.null(df) && nrow(df) == 0) {
    # message(paste0("  -> SUCCESS (0 rows) for table: ", tbl_config$table))
    # Return an empty tibble with expected minimal columns if 0 rows, so bind_rows works later
    return(tibble( # Define expected columns from get_sidra output even if empty
      `Município (Código)` = character(), Ano = character(), Valor = character(), Variável = character(),
      Fetched_Table_ID = character(), Fetched_Table_Desc = character() 
    ))
  }
  return(df)
}

standardize_keys_types_helper <- function(df, table_id_for_log = "UnknownTable"){ # Renamed
  if (is.null(df) || nrow(df) == 0) return(tibble()) # Return empty tibble if input is empty
  
  # Ensure essential SIDRA columns exist, even if df is from a failed/empty fetch
  # that somehow returned a non-null but malformed object.
  # fetch_sidra_data_helper should now return a tibble with these if result is 0 rows.
  required_sidra_cols <- c("Município (Código)", "Ano", "Valor", "Variável")
  for(col in required_sidra_cols){
    if(!col %in% names(df)) df[[col]] <- NA_character_
  }
  if (!"Fetched_Table_ID" %in% names(df)) { df$Fetched_Table_ID <- NA_character_ }
  if (!"Fetched_Table_Desc" %in% names(df)) { df$Fetched_Table_Desc <- NA_character_ }
  
  df$Fetched_Table_ID <- as.character(df$Fetched_Table_ID)
  df$Fetched_Table_Desc <- as.character(df$Fetched_Table_Desc)
  
  df$Valor_raw <- as.character(df$Valor) # Valor must exist due to check above
  
  # Use tryCatch for parse_number in case of truly malformed Valor_raw for a specific variable
  # This is belt-and-suspenders as safe_numeric itself is robust.
  df_processed <- tryCatch({
    df %>% 
      rowwise() %>% 
      mutate(value = safe_numeric(Valor_raw, Variável)) %>% 
      ungroup()
  }, error = function(e) {
    warning("Error during safe_numeric parsing in standardize_keys_types for table ", table_id_for_log, ": ", e$message, call. = FALSE)
    df %>% mutate(value = NA_real_) # Add value column as NA if parsing failed broadly
  })
  
  df_processed %>%
    rename(muni_code = `Município (Código)`, ano_chr = Ano) %>%
    mutate( muni_code = stringr::str_pad(as.character(muni_code), 7, "left", "0"),
            ano = as.integer(ano_chr), 
            Variável = trimws(as.character(Variável)) ) %>%
    select(any_of(c("muni_code", "ano", "Variável", "value", "Valor_raw", "Fetched_Table_ID", "Fetched_Table_Desc")),
           everything(), 
           -ano_chr, 
           -any_of(c("Valor", names(df)[!names(df) %in% c(required_sidra_cols,"Fetched_Table_ID","Fetched_Table_Desc","Valor_raw","value","muni_code","ano","Variável") & !str_detect(names(df), "\\.\\.\\.")])) # Clean extra SIDRA cols
    )
}

harmonize_age_band_target_schema_scalar_helper <- function(age_str_scalar){ # Renamed
  if (!is.character(age_str_scalar) || length(age_str_scalar) != 1 || is.na(age_str_scalar)) {
    return(NA_character_)
  }
  age_str <- trimws(age_str_scalar)
  case_when(
    age_str %in% c("0 a 4 anos") ~ "00_04", age_str %in% c("5 a 9 anos") ~ "05_09",
    age_str %in% c("10 a 14 anos") ~ "10_14", age_str %in% c("15 a 19 anos") ~ "15_19",
    age_str %in% c("20 a 24 anos") ~ "20_24", age_str %in% c("25 a 29 anos") ~ "25_29",
    age_str %in% c("30 a 39 anos", "30 a 34 anos", "35 a 39 anos") ~ "30_39",
    age_str %in% c("40 a 49 anos", "40 a 44 anos", "45 a 49 anos") ~ "40_49",
    age_str %in% c("50 a 59 anos", "50 a 54 anos", "55 a 59 anos") ~ "50_59",
    age_str %in% c("60 a 69 anos", "60 a 64 anos", "65 a 69 anos") ~ "60_69",
    age_str %in% c("70 a 79 anos", "70 a 74 anos", "75 a 79 anos") ~ "70_79",
    age_str %in% c("80 anos ou mais", "80 a 84 anos", "85 a 89 anos", "90 a 94 anos", "95 a 99 anos", "100 anos ou mais") ~ "80p",
    age_str == "Total" ~ AGE_BAND_TOTAL_CODE, # AGE_BAND_TOTAL_CODE must be globally accessible
    TRUE ~ NA_character_
  )
}

prepare_census_shares_for_interpolation <- function(demographic_basis_df, census_years_vec, 
                                                    all_genders, all_races, all_age_bands) {
  # This function assumes demographic_basis_df has pop_total_est (integerized) 
  # and 0-filled integer granular columns for census years.
  
  expected_gra_cols <- expand.grid(g=all_genders, r=all_races, a=all_age_bands) %>%
    mutate(col_name = paste0("pop_", g, "_", r, "_", a)) %>% pull(col_name)
  actual_gra_cols_in_data <- intersect(expected_gra_cols, names(demographic_basis_df))
  
  if(length(actual_gra_cols_in_data) == 0) stop("prepare_shares: No granular pop columns found.")
  
  shares_long_df <- demographic_basis_df %>%
    filter(ano %in% census_years_vec) %>%
    select(muni_code, ano, pop_total_est, all_of(actual_gra_cols_in_data)) %>%
    pivot_longer(cols = all_of(actual_gra_cols_in_data), 
                 names_to = "gra_column", values_to = "pop_census_val") %>%
    mutate(
      gender = str_match(gra_column, "^pop_([MW])_")[,2],
      race = str_match(gra_column, "^pop_[MW]_([A-Za-z]{3})_")[,2],
      age_band = str_match(gra_column, "^pop_[MW]_[A-Za-z]{3}_(.*)$")[,2]
    ) %>%
    filter(!is.na(gender) & !is.na(race) & !is.na(age_band)) %>%
    mutate(share = ifelse(pop_total_est > 0, pop_census_val / pop_total_est, 0),
           share = ifelse(is.na(share) & pop_total_est == 0 & pop_census_val == 0, 0, share)) %>%
    select(muni_code, ano, gender, race, age_band, share, pop_census_val)
  
  pop_total_est_ref_df <- demographic_basis_df %>% select(muni_code, ano, pop_total_est) %>% distinct()
  return(list(census_shares = shares_long_df, pop_total_est_ref = pop_total_est_ref_df))
}

interpolate_shares_core_helper <- function(group_df_shares_ano, # Dataframe with only 'ano' and 'share' columns for the group
                                           all_target_years_interpolation_span,
                                           interpolation_method_type = "spline") {
  complete_years_df <- tibble(ano = all_target_years_interpolation_span)
  data_to_interp <- complete_years_df %>% left_join(group_df_shares_ano, by = "ano")
  
  interpolated_share_values <- rep(NA_real_, length(all_target_years_interpolation_span))
  valid_share_points <- data_to_interp %>% filter(!is.na(share))
  
  if (nrow(valid_share_points) < 2) { 
    filled_shares <- zoo::na.locf(zoo::na.locf(data_to_interp$share, na.rm = FALSE), fromLast = TRUE, na.rm = FALSE)
    interpolated_share_values <- ifelse(is.na(filled_shares), 0, filled_shares)
  } else if (interpolation_method_type == "linear") {
    interp_result <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, 
                                    xout = all_target_years_interpolation_span, rule = 2)
    interpolated_share_values <- as.numeric(interp_result) 
  } else if (interpolation_method_type == "spline") {
    if (nrow(valid_share_points) >= 2) { 
      spline_fit <- tryCatch(
        stats::splinefun(x = valid_share_points$ano, y = valid_share_points$share, method = "monoH.FC"), error = function(e) {
          tryCatch(stats::splinefun(x = valid_share_points$ano, y = valid_share_points$share, method = "fmm"), error = function(e2) {NULL})})
      if(!is.null(spline_fit)){ interpolated_share_values <- spline_fit(all_target_years_interpolation_span)
      } else { 
        interp_result_fallback <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, xout = all_target_years_interpolation_span, rule = 2)
        interpolated_share_values <- as.numeric(interp_result_fallback) }
    } else { 
      interp_result_fallback2 <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, xout = all_target_years_interpolation_span, rule = 2)
      interpolated_share_values <- as.numeric(interp_result_fallback2) }}
  else { stop("Unknown interpolation_method_type in interpolate_shares_core_helper") }
  
  interpolated_share_values <- pmax(0, pmin(1, interpolated_share_values))
  interpolated_share_values <- ifelse(is.na(interpolated_share_values), 0, interpolated_share_values)
  return(tibble(ano = all_target_years_interpolation_span, interpolated_share = interpolated_share_values))
}


# --- III. Main Pipeline Function ---
create_demographic_basis_pipeline <- function(
    output_start_year,
    output_end_year,
    geo_selection = "BR", # e.g., "BR", "SP", c("RJ", "MG")
    master_census_years_param = MASTER_CENSUS_YEARS, # Allow override if needed
    geobr_year_param = GEOBR_MUNI_LIST_YEAR,
    sidra_config_param = SIDRA_TABLES_CONFIG,
    col_rename_map_param = COL_RENAME_MAP,
    base_output_dir = "output_pipeline",
    pipeline_version_tag = "v1.0"
) {
  
  message(paste0("\n======================================================================="))
  message(paste0("STARTING Demographic Basis Pipeline ", pipeline_version_tag))
  message(paste0("Scope: '", paste(geo_selection, collapse=", "), 
                 "', Output Years: ", output_start_year, "-", output_end_year))
  message(paste0("=======================================================================\n"))
  
  # --- Step 1: Determine Target Municipalities and Years ---
  message("--- Pipeline Step 1: Determining Target Municipalities and Years ---")
  
  # Ensure master_census_years_param is accessible (it's a function argument)
  interpolation_span_years <- min(master_census_years_param):max(master_census_years_param)
  user_requested_years <- output_start_year:output_end_year
  
  if(min(user_requested_years) < min(interpolation_span_years) || max(user_requested_years) > max(interpolation_span_years)) {
    message("Warning: User requested year range (", min(user_requested_years), "-", max(user_requested_years), 
            ") is outside the primary interpolation span (", min(interpolation_span_years), "-", max(interpolation_span_years), 
            "). Results outside the interpolation span will be based on extrapolation of shares.")
  }
  
  target_muni_codes_6dig <- c()
  geo_tag_for_filename <- ""
  
  if (is.character(geo_selection) && length(geo_selection) == 1 && toupper(geo_selection) == "BR") {
    message("   Fetching all municipalities for Brazil...")
    # For "BR", code_muni = "all" is the default and correct way if not specifying by state/muni
    all_munis_sf <- tryCatch(
      geobr::read_municipality(year = geobr_year_param, showProgress = FALSE), 
      error = function(e) {
        message("ERROR: Failed to fetch Brazil muni list from geobr: ", e$message)
        return(NULL)
      }
    )
    if(is.null(all_munis_sf)) stop("Could not retrieve municipality list for Brazil.")
    target_muni_codes_6dig <- as.character(all_munis_sf$code_muni)
    geo_tag_for_filename <- "BR"
    
  } else if (is.character(geo_selection) && length(geo_selection) > 0) {
    message(paste0("   Fetching municipalities for UF(s): ", paste(geo_selection, collapse=", ")))
    
    # geo_selection can be a mix of abbreviations (e.g., "AL") or numeric codes (e.g., "27")
    # For geobr::read_municipality, pass the state identifier to its 'code_muni' argument.
    
    all_munis_for_selected_ufs_list <- list()
    
    for (uf_identifier in geo_selection) { 
      message(paste0("      Attempting to fetch municipalities for UF identifier: ", uf_identifier))
      
      munis_in_uf_sf <- tryCatch(
        # Pass the UF abbreviation or code directly to the 'code_muni' argument.
        # geobr interprets this as a state filter.
        geobr::read_municipality(code_muni = uf_identifier, year = geobr_year_param, showProgress = FALSE),
        error = function(e) {
          message(paste0("ERROR: Failed to fetch muni list for UF identifier '", uf_identifier, "'. Error: ", e$message))
          return(NULL) # Return NULL if this specific UF fails
        }
      )
      
      if(!is.null(munis_in_uf_sf) && nrow(munis_in_uf_sf) > 0) {
        all_munis_for_selected_ufs_list[[uf_identifier]] <- munis_in_uf_sf
        message(paste0("      Successfully fetched ", nrow(munis_in_uf_sf), " municipalities for UF identifier: ", uf_identifier))
      } else {
        message(paste0("Warning: Could not retrieve municipalities for UF identifier: ", uf_identifier, 
                       if(!is.null(munis_in_uf_sf) && nrow(munis_in_uf_sf) == 0) " (0 rows returned by geobr)" else " (fetch attempt failed or returned NULL)"))
      }
    }
    
    if (length(all_munis_for_selected_ufs_list) > 0) {
      combined_munis_sf <- dplyr::bind_rows(all_munis_for_selected_ufs_list)
      target_muni_codes_6dig <- unique(as.character(combined_munis_sf$code_muni))
      # Use the original geo_selection for the tag, but sorted and uppercased
      geo_tag_for_filename <- paste(sort(unique(toupper(geo_selection))), collapse="-")
    } # If list is empty, target_muni_codes_6dig remains empty
    
  } else {
    stop("Invalid 'geo_selection'. Must be 'BR' (as a single string) or a character vector of UF abbreviations/codes.")
  }
  
  if (length(target_muni_codes_6dig) == 0) {
    stop("No municipalities determined for processing. Check 'geo_selection' and geobr availability/logs.")
  }
  message("   Processing for ", length(target_muni_codes_6dig), " unique municipalities in total.")
  
  # --- Step 2: Fetch Raw Data with Dynamic Chunking and Retries ---
  message("\n--- Pipeline Step 2: Fetching Raw SIDRA Data (Dynamic Chunking) ---")
  
  # Initial chunk size to try for a new group of municipalities for a table
  # This will be adapted downwards if API limit errors occur.
  ADAPTIVE_INITIAL_MUNI_CHUNK_SIZE <- 20 # Start with a reasonable general chunk size
  MIN_MUNI_CHUNK_SIZE <- 1     # Smallest chunk size to try for a single API call
  
  raw_data_list_pipeline_collected <- stats::setNames(vector("list", length(sidra_config_param)), 
                                                      names(sidra_config_param))
  
  # Loop through each table configuration
  for (table_name in names(sidra_config_param)) {
    current_tbl_config <- sidra_config_param[[table_name]]
    message(paste0("\n   PROCESSING TABLE: ", current_tbl_config$table, " (", current_tbl_config$desc, ")"))
    
    all_data_for_current_table <- list() # Stores successfully fetched dataframes for this table
    
    # Queue of work items. Each item is a list:
    # list(munis = vector_of_muni_codes, 
    #      chunk_size_to_try = N, 
    #      retries_left_for_this_group = K)
    muni_processing_queue <- list(
      list(munis = target_muni_codes_6dig, 
           chunk_size_to_try = ADAPTIVE_INITIAL_MUNI_CHUNK_SIZE, 
           retries_left_for_this_group = 5) 
    )
    
    # Keep track of municipalities whose data for this table has been successfully fetched
    # to avoid re-processing them if they appear in overlapping failed groups.
    successfully_fetched_munis_for_table <- character(0) 
    
    total_munis_for_table <- length(target_muni_codes_6dig)
    
    master_loop_count <- 0
    MAX_MASTER_LOOPS <- total_munis_for_table * 10 # Generous safety break for the while loop
    
    while(length(muni_processing_queue) > 0 && master_loop_count < MAX_MASTER_LOOPS) {
      master_loop_count <- master_loop_count + 1
      
      current_work_item <- muni_processing_queue[[1]] 
      muni_processing_queue[[1]] <- NULL # Dequeue
      
      munis_in_current_group <- setdiff(current_work_item$munis, successfully_fetched_munis_for_table)
      chunk_size_for_this_attempt <- current_work_item$chunk_size_to_try
      retries_remaining_for_group <- current_work_item$retries_left_for_this_group
      
      if (length(munis_in_current_group) == 0) {
        # message("      Skipping an empty or already processed muni group for table ", current_tbl_config$table)
        next
      }
      if (retries_remaining_for_group <= 0) {
        message(paste0("      WARNING: Max retries reached for a group of ", length(munis_in_current_group), 
                       " munis (starting with ", munis_in_current_group[1],") for table ", current_tbl_config$table, 
                       ". These will be skipped for this table."))
        next
      }
      
      # Split the current group of munis into sub-chunks for this attempt
      sub_chunks_to_process_now <- list()
      if (length(munis_in_current_group) > chunk_size_for_this_attempt) {
        sub_chunks_to_process_now <- split(munis_in_current_group, 
                                           ceiling(seq_along(munis_in_current_group) / chunk_size_for_this_attempt))
      } else {
        sub_chunks_to_process_now <- list(munis_in_current_group) 
      }
      
      message(paste0("      Table ", current_tbl_config$table, " - Master Loop ", master_loop_count, 
                     ": Attempting ", length(munis_in_current_group), " munis in ", 
                     length(sub_chunks_to_process_now), " sub-chunk(s) of size up to ", 
                     chunk_size_for_this_attempt, ". Retries left for this group: ", retries_remaining_for_group -1))
      
      num_workers <- max(1, floor(availableCores() / 2))
      plan(multisession, workers = num_workers)
      
      # message(paste0("         About to run future_map with ", length(sub_chunks_to_process_now), " items for table ", current_tbl_config$table))
      sub_chunk_fetch_results <- tryCatch({
        furrr::future_map(
          .x = sub_chunks_to_process_now,
          .f = ~fetch_sidra_data_helper(tbl_config = current_tbl_config, muni_codes_vec = .x),
          .options = furrr_options(seed = TRUE, scheduling = 1), # scheduling=1 good for varying task times
          .progress = TRUE # Show furrr's progress bar
        )
      }, error = function(e_fm) {
        message(paste0("      CRITICAL ERROR during future_map for table ", current_tbl_config$table, ": ", e_fm$message))
        return(rep(list(NULL), length(sub_chunks_to_process_now))) # Return list of NULLs on total future_map failure
      })
      plan(sequential)
      # message(paste0("         Finished future_map for this batch for table ", current_tbl_config$table))
      
      munis_requiring_retry_from_this_batch <- list() 
      
      for (k in seq_along(sub_chunk_fetch_results)) {
        fetch_result <- sub_chunk_fetch_results[[k]]
        munis_in_this_specific_sub_chunk <- sub_chunks_to_process_now[[k]]
        
        if (is.data.frame(fetch_result)) {
          all_data_for_current_table[[length(all_data_for_current_table) + 1]] <- fetch_result
          if(nrow(fetch_result) > 0 && ("Município (Código)" %in% names(fetch_result) || "muni_code" %in% names(fetch_result))){
            fetched_muni_codes_in_result <- unique(fetch_result$`Município (Código)` %||% fetch_result$muni_code)
            successfully_fetched_munis_for_table <- union(successfully_fetched_munis_for_table, fetched_muni_codes_in_result)
          }
        } else if (is.list(fetch_result) && !is.null(fetch_result$error_type) && fetch_result$error_type == "SIZE_LIMIT_EXCEEDED") {
          # This sub-chunk itself failed due to size, add its munis to be re-processed with smaller chunk size
          munis_requiring_retry_from_this_batch[[length(munis_requiring_retry_from_this_batch) + 1]] <- 
            list(munis = munis_in_this_specific_sub_chunk, 
                 error_details = fetch_result) # Store error details for this specific failure
        } else { 
          message(paste0("         Sub-chunk with ", length(munis_in_this_specific_sub_chunk), 
                         " munis (starting ", munis_in_this_specific_sub_chunk[1], 
                         ") failed for table ", current_tbl_config$table, " with other error or NULL."))
          # Optionally, add these to a separate list for munis that failed for non-size reasons.
          # For now, we can also requeue them with halved chunk size as a general retry for robustness.
          munis_requiring_retry_from_this_batch[[length(munis_requiring_retry_from_this_batch) + 1]] <- 
            list(munis = munis_in_this_specific_sub_chunk, 
                 error_details = NULL) # No specific size error details
        }
      } # End loop over sub_chunk_fetch_results
      
      # Process any groups of munis that need retrying
      if (length(munis_requiring_retry_from_this_batch) > 0) {
        for (failed_group_info in munis_requiring_retry_from_this_batch) {
          munis_to_requeue <- failed_group_info$munis
          error_details_for_group <- failed_group_info$error_details
          
          new_chunk_size_for_requeued_group <- chunk_size_for_this_attempt # Default if no specific size info
          
          if (!is.null(error_details_for_group) && error_details_for_group$error_type == "SIZE_LIMIT_EXCEEDED" &&
              !is.na(error_details_for_group$requested_size) && !is.na(error_details_for_group$limit_size) &&
              error_details_for_group$requested_size > 0 && error_details_for_group$num_munis_in_failed_chunk > 0) {
            
            num_munis_that_caused_failure = error_details_for_group$num_munis_in_failed_chunk
            calculated_size <- floor(num_munis_that_caused_failure * 
                                       (error_details_for_group$limit_size / error_details_for_group$requested_size) * 0.85) # 85% safety
            new_chunk_size_for_requeued_group <- max(MIN_MUNI_CHUNK_SIZE, calculated_size)
            
            message(paste0("      ADJUSTING CHUNK SIZE for ", length(munis_to_requeue), " munis (table ", current_tbl_config$table, 
                           ") to ", new_chunk_size_for_requeued_group, " based on API feedback (Req: ", 
                           error_details_for_group$requested_size, ", Limit: ", error_details_for_group$limit_size, 
                           " for ", num_munis_that_caused_failure, " munis)."))
          } else { # Fallback: no size error details, or other error, just halve the last attempted size for this group
            new_chunk_size_for_requeued_group <- max(MIN_MUNI_CHUNK_SIZE, floor(chunk_size_for_this_attempt / 2))
            message(paste0("      FALLBACK: Reducing chunk size for ", length(munis_to_requeue), " munis (table ", current_tbl_config$table, 
                           ") to ", new_chunk_size_for_requeued_group, " for retry."))
          }
          
          if (new_chunk_size_for_requeued_group == MIN_MUNI_CHUNK_SIZE && length(munis_to_requeue) == 1 && 
              !is.null(error_details_for_group) && error_details_for_group$error_type == "SIZE_LIMIT_EXCEEDED") {
            message(paste0("      CRITICAL: Single municipality ", munis_to_requeue[1], 
                           " failed with chunk size 1 (API limit exceeded) for table ", current_tbl_config$table, 
                           ". This municipality's data for this table is too large for a single call with current SIDRA parameters. Skipping."))
          } else {
            muni_processing_queue[[length(muni_processing_queue) + 1]] <- 
              list(munis = munis_to_requeue, 
                   chunk_size_to_try = new_chunk_size_for_requeued_group,
                   retries_left_for_this_group = retries_remaining_for_group - 1)
          }
        }
      }
      
      num_actually_fetched_for_table <- length(unique(successfully_fetched_munis_for_table))
      message(paste0("      Table ", current_tbl_config$table, " status: ~", 
                     num_actually_fetched_for_table, "/", total_munis_for_table, 
                     " munis fully fetched. Queue length: ", length(muni_processing_queue)))
      
      if (master_loop_count >= MAX_MASTER_LOOPS) {
        message(paste0("      WARNING: Max master loop iterations (", MAX_MASTER_LOOPS, 
                       ") reached for table ", current_tbl_config$table, ". Breaking fetch loop for this table."))
        break 
      }
    } # End while(length(muni_processing_queue) > 0) for current table
    
    # Final combination of successfully fetched data for the current table
    if (length(all_data_for_current_table) > 0) {
      all_data_for_current_table_filtered <- Filter(function(df) inherits(df, "data.frame") && nrow(df) > 0, all_data_for_current_table)
      if(length(all_data_for_current_table_filtered) > 0) {
        raw_data_list_pipeline_collected[[table_name]] <- dplyr::bind_rows(all_data_for_current_table_filtered)
        message(paste0("   ---> Finished processing table: ", current_tbl_config$table, 
                       ". Total rows fetched: ", nrow(raw_data_list_pipeline_collected[[table_name]])))
      } else {
        message(paste0("   ---> NOTE: No actual data rows fetched for table ", current_tbl_config$table, " after all attempts. Assigning empty template."))
        raw_data_list_pipeline_collected[[table_name]] <- tibble(
          `Município (Código)` = character(), Ano = character(), Valor = character(), Variável = character(),
          Fetched_Table_ID = as.character(current_tbl_config$table), 
          Fetched_Table_Desc = as.character(current_tbl_config$desc) )
      }
    } else {
      message(paste0("   ---> WARNING: No data collected at all for table: ", current_tbl_config$table))
      raw_data_list_pipeline_collected[[table_name]] <- tibble(
        `Município (Código)` = character(), Ano = character(), Valor = character(), Variável = character(),
        Fetched_Table_ID = as.character(current_tbl_config$table), 
        Fetched_Table_Desc = as.character(current_tbl_config$desc) )
    }
  } # End loop over table_name (for table_name in names(sidra_config_param))
  
  # ... (rest of Step 2 - final raw_data_list_pipeline assignment and checks) ...
  # This part can remain largely the same as in your provided script
  raw_data_list_pipeline <- raw_data_list_pipeline_collected
  raw_data_list_pipeline <- raw_data_list_pipeline[sapply(raw_data_list_pipeline, function(df) !is.null(df) && inherits(df, "data.frame"))]
  
  if (length(raw_data_list_pipeline) == 0) stop("No data successfully fetched from SIDRA for any table after dynamic chunking.")
  
  for(table_name_check in names(sidra_config_param)){ 
    if(!table_name_check %in% names(raw_data_list_pipeline) || 
       is.null(raw_data_list_pipeline[[table_name_check]]) || # Check for NULL entry
       nrow(raw_data_list_pipeline[[table_name_check]]) == 0){
      
      message(paste0("   FINAL NOTE: Table '", table_name_check, "' (", sidra_config_param[[table_name_check]]$desc, 
                     ") is empty or missing in the final fetched dataset."))
      
      # Ensure an empty tibble with standard structure exists for tables that completely failed
      if(!table_name_check %in% names(raw_data_list_pipeline) || is.null(raw_data_list_pipeline[[table_name_check]])){
        raw_data_list_pipeline[[table_name_check]] <- tibble(
          `Município (Código)` = character(), Ano = character(), Valor = character(), Variável = character(),
          Fetched_Table_ID = as.character(sidra_config_param[[table_name_check]]$table), 
          Fetched_Table_Desc = as.character(sidra_config_param[[table_name_check]]$desc) )
      }
    }
  }
  message("   Finished fetching all SIDRA data with dynamic chunking.")
  
  
  # --- Step 3: Preprocessing & Standardizing Data ---
  message("\n--- Pipeline Step 3: Preprocessing & Standardizing SIDRA Data ---")
  std_data_list_pipeline <- purrr::map(raw_data_list_pipeline, 
                                       ~standardize_keys_types_helper(.x, table_id_for_log = first(.x$Fetched_Table_ID %||% "Unknown")))
  
  # EstimaPop
  message("   Processing EstimaPop...")
  if (!is.null(std_data_list_pipeline$estimapop) && nrow(std_data_list_pipeline$estimapop) > 0) {
    estimapop_processed <- std_data_list_pipeline$estimapop %>%
      filter(Variável == sidra_config_param$estimapop$target_var, !is.na(value)) %>%
      select(muni_code, ano, pop_total_est_src = value) %>%
      distinct(muni_code, ano, .keep_all = TRUE)
  } else {
    message("   Warning: EstimaPop raw data missing or empty. pop_total_est will rely more on census.")
    estimapop_processed <- tibble(muni_code = character(), ano = integer(), pop_total_est_src = numeric())
  }
  
  # PIB
  message("   Processing PIB...")
  if (!is.null(std_data_list_pipeline$pib) && nrow(std_data_list_pipeline$pib) > 0) {
    pib_raw_std <- std_data_list_pipeline$pib %>%
      filter(Variável %in% sidra_config_param$pib$target_vars, !is.na(value)) %>%
      select(muni_code, ano, Variável, value) %>% distinct(muni_code, ano, Variável, .keep_all = TRUE)
    pib_pivoted <- pib_raw_std %>% pivot_wider(names_from = Variável, values_from = value)
    pib_processed <- pib_pivoted %>% rename_with(~ col_rename_map_param[.x], .cols = any_of(names(col_rename_map_param)))
    # Validation (optional here, can be done at the end)
  } else {
    message("   Warning: PIB raw data missing or empty.")
    pib_processed <- tibble(muni_code = character(), ano = integer()) 
  }
  
  # CEMPRE
  message("   Processing CEMPRE...")
  cempre_std_data <- bind_rows(std_data_list_pipeline$cempre_06_21, std_data_list_pipeline$cempre_22)
  if (nrow(cempre_std_data) > 0) {
    cempre_target_vars <- unique(c(sidra_config_param$cempre_06_21$target_vars, sidra_config_param$cempre_22$target_vars))
    cempre_filtered <- cempre_std_data %>%
      filter(Variável %in% cempre_target_vars, !is.na(value)) %>%
      select(muni_code, ano, Variável, value, Fetched_Table_ID) %>%
      group_by(muni_code, ano, Variável) %>% arrange(desc(Fetched_Table_ID)) %>% slice(1) %>% ungroup() %>% select(-Fetched_Table_ID)
    cempre_pivoted <- cempre_filtered %>% pivot_wider(names_from = Variável, values_from = value)
    cempre_processed <- cempre_pivoted %>% rename_with(~ col_rename_map_param[.x], .cols = any_of(names(col_rename_map_param)))
  } else {
    message("   Warning: CEMPRE raw data missing or empty.")
    cempre_processed <- tibble(muni_code = character(), ano = integer())
  }
  
  # --- Step 4: Process Census Data ---
  message("\n--- Pipeline Step 4: Processing Census Data ---")
  census_raw_combined <- bind_rows(
    std_data_list_pipeline$censo_2000 %>% filter(ano == 2000),
    std_data_list_pipeline$censo_10_22 %>% filter(ano %in% c(2010, 2022)) # Using master_census_years_param for flexibility
  )
  
  raw_census_totals <- tibble() # Initialize
  census_processed_for_pivot <- tibble()
  
  if (nrow(census_raw_combined) > 0) {
    census_std <- census_raw_combined %>%
      filter(Variável == sidra_config_param$censo_2000$target_var, !is.na(value)) %>%
      filter(if ("Situação do domicílio" %in% names(.)) (ano == 2000 & `Situação do domicílio` == "Total") | ano != 2000 else TRUE) %>%
      mutate(value = round(as.numeric(value)))
    
    census_age_filt <- census_std %>%
      mutate(age_orig_raw = as.character(coalesce(Idade, `Grupo de idade`))) %>%
      filter( (ano == 2000 & age_orig_raw %in% c(T2093_DESIRED_AGE_STRINGS, "Total")) |
                (ano %in% c(2010, 2022) & age_orig_raw %in% c(T9606_DESIRED_AGE_STRINGS, "Total")) ) # Ensure these constants are accessible
    
    census_harm <- census_age_filt %>%
      transmute(muni_code, ano, gender_orig = Sexo, race_orig = `Cor ou raça`, age_orig = age_orig_raw, pop = value) %>%
      mutate(gender = recode(gender_orig, !!!SEX_CODES), race = recode(race_orig, !!!RACE_CODES), # SEX_CODES, RACE_CODES global
             age_band_target = purrr::map_chr(age_orig, harmonize_age_band_target_schema_scalar_helper)) %>% # AGE_BAND_TOTAL_CODE global
      filter(!is.na(gender), !is.na(race), !is.na(age_band_target), !is.na(pop))
    
    raw_census_totals <- census_harm %>%
      filter(gender == "T", race == "T", age_band_target == AGE_BAND_TOTAL_CODE) %>%
      select(muni_code, ano, pop_total_original_raw = pop) %>%
      group_by(muni_code, ano) %>% summarise(pop_total_original_raw = sum(pop_total_original_raw, na.rm = TRUE), .groups = "drop")
    
    # Redistribute "Ign" for 2000 (TARGET_AGE_BANDS global)
    census_2000_data_re <- census_harm %>% filter(ano == 2000, gender != "T", race != "T", age_band_target %in% TARGET_AGE_BANDS)
    list_agg_target <- list()
    if(nrow(census_2000_data_re) > 0 && "Ign" %in% unique(census_2000_data_re$race)) {
      # ... (exact redistribution logic from main script v14.3 Section 4d) ...
      ign_counts <- census_2000_data_re %>% filter(race == "Ign") %>% group_by(muni_code, ano, gender, age_band_target) %>% summarise(pop_ign = sum(pop,na.rm=T), .groups="drop")
      declared_props <- census_2000_data_re %>% filter(race != "Ign") %>% group_by(muni_code,ano,gender,age_band_target,race) %>% summarise(pop=sum(pop,na.rm=T),.groups="drop") %>% group_by(muni_code,ano,gender,age_band_target) %>% mutate(total_decl=sum(pop,na.rm=T)) %>% ungroup() %>% mutate(prop_decl=ifelse(total_decl==0,0,pop/total_decl))
      redist_df <- declared_props %>% left_join(ign_counts, by=c("muni_code","ano","gender","age_band_target")) %>% mutate(pop_ign=ifelse(is.na(pop_ign),0,pop_ign), pop_add=round(prop_decl*pop_ign), pop_final=pop+pop_add) %>% select(muni_code,ano,gender,race,age_band_target,pop=pop_final)
      list_agg_target[["y2000"]] <- redist_df
    } else {
      list_agg_target[["y2000"]] <- census_harm %>% filter(ano == 2000, gender != "T", race != "T", age_band_target %in% TARGET_AGE_BANDS, race != "Ign") %>% group_by(muni_code,ano,gender,race,age_band_target) %>% summarise(pop=sum(pop,na.rm=T),.groups="drop")
    }
    list_agg_target[["other_yrs"]] <- census_harm %>% filter(ano %in% c(2010,2022), gender != "T", race != "T", age_band_target %in% TARGET_AGE_BANDS) %>% group_by(muni_code,ano,gender,race,age_band_target) %>% summarise(pop=sum(pop,na.rm=T),.groups="drop")
    census_processed_for_pivot <- bind_rows(list_agg_target) %>% group_by(muni_code,ano,gender,race,age_band_target) %>% summarise(pop=sum(pop,na.rm=T),.groups="drop")
  } else {
    message("   Warning: Combined census raw data is empty. No granular census data will be produced.")
  }
  
  # --- Step 5: Generate Authoritative pop_total_est (Integer) ---
  message("\n--- Pipeline Step 5: Generating Integer pop_total_est ---")
  base_grid_pop_est <- expand_grid(muni_code = target_muni_codes_6dig, ano = interpolation_span_years)
  
  pop_series_for_interp <- base_grid_pop_est %>%
    left_join(estimapop_processed, by = c("muni_code", "ano"))
  if (nrow(raw_census_totals) > 0) {
    pop_series_for_interp <- pop_series_for_interp %>%
      left_join(raw_census_totals, by = c("muni_code", "ano")) %>%
      mutate(pop_to_interp = ifelse(ano %in% master_census_years_param & !is.na(pop_total_original_raw), 
                                    pop_total_original_raw, pop_total_est_src))
  } else { # No census totals, rely only on estimapop
    pop_series_for_interp <- pop_series_for_interp %>%
      mutate(pop_to_interp = pop_total_est_src)
  }
  
  pop_total_est_final_df <- pop_series_for_interp %>%
    select(muni_code, ano, pop_to_interp) %>% # Ensure only these columns before group_by
    group_by(muni_code) %>% arrange(ano) %>%
    mutate(pop_total_est_decimal = zoo::na.approx(pop_to_interp, na.rm = FALSE, rule = 2),
           pop_total_est = round(pop_total_est_decimal)) %>% # INTEGERIZED
    ungroup() %>%
    select(muni_code, ano, pop_total_est) # Keep only integer final
  
  # --- Step 6: Initial Join for Main Basis ---
  message("\n--- Pipeline Step 6: Initial Join for Main Demographic Basis ---")
  demographic_basis_intermediate <- base_grid_pop_est %>% # Grid over interpolation_span_years
    left_join(pop_total_est_final_df, by = c("muni_code", "ano"))
  
  if(nrow(raw_census_totals) > 0) demographic_basis_intermediate <- demographic_basis_intermediate %>% left_join(raw_census_totals, by = c("muni_code", "ano"))
  else demographic_basis_intermediate$pop_total_original_raw <- NA_integer_
  
  if(nrow(pib_processed) > 0 && ncol(pib_processed) > 2) demographic_basis_intermediate <- demographic_basis_intermediate %>% left_join(pib_processed, by = c("muni_code", "ano"))
  if(nrow(cempre_processed) > 0 && ncol(cempre_processed) > 2) demographic_basis_intermediate <- demographic_basis_intermediate %>% left_join(cempre_processed, by = c("muni_code", "ano"))
  
  if(nrow(census_processed_for_pivot) > 0){
    pop_wide_census <- census_processed_for_pivot %>% filter(race != "Ign") %>% # Should be redundant
      mutate(col_name = paste0("pop_", gender, "_", race, "_", age_band_target)) %>%
      select(muni_code, ano, col_name, pop) %>%
      pivot_wider(names_from = col_name, values_from = pop, values_fill = NA_integer_) # Fill with integer NA
    if(nrow(pop_wide_census) > 0 && ncol(pop_wide_census) > 2) demographic_basis_intermediate <- demographic_basis_intermediate %>% left_join(pop_wide_census, by = c("muni_code", "ano"))
  }
  
  # --- Step 7: Finalize Core Data (Geo, NA to 0 for census granular) ---
  message("\n--- Pipeline Step 7: Finalizing Core Data Structure ---")
  geo_info_df <- tibble()
  state_info_df <- tibble()
  all_states_geo <- tryCatch(geobr::read_state(year=geobr_year_param, showProgress=F), error = function(e) NULL)
  if(!is.null(all_states_geo)){
    state_info_df <- all_states_sf %>% sf::st_drop_geometry() %>% transmute(uf_code=as.character(code_state), uf_name=name_state, uf_abbr=abbrev_state)
    
    # Fetch muni_info for all target_muni_codes at once if possible, or by UF then combine
    # To reduce geobr calls if many UFs are selected one by one
    unique_uf_codes_from_munis <- unique(str_sub(target_muni_codes_6dig, 1, 2))
    
    all_munis_for_scope_sf <- tryCatch(
      geobr::read_municipality(code_muni = "all", year = geobr_year_param, showProgress = FALSE) %>% 
        filter(code_muni %in% target_muni_codes_6dig), # Filter after fetching all
      error = function(e) { message("Error fetching all munis for geo_info: ", e$message); return(NULL)}
    )
    if(!is.null(all_munis_for_scope_sf) && nrow(all_munis_for_scope_sf) > 0){
      geo_info_df <- all_munis_for_scope_sf %>% sf::st_drop_geometry() %>%
        transmute(muni_code=as.character(code_muni), muni_name=name_muni, uf_code=as.character(code_state)) %>%
        left_join(state_info_df %>% select(uf_code, uf_abbr, uf_name), by="uf_code") # Add uf_abbr, uf_name
    } else {
      message("Warning: Could not build comprehensive geo_info_df.")
    }
  } else {
    message("Warning: Could not fetch state information from geobr.")
  }
  
  if(nrow(geo_info_df) > 0){
    demographic_basis_before_gra_interp <- demographic_basis_intermediate %>%
      left_join(geo_info_df %>% select(muni_code, muni_name, uf_abbr, uf_name), by = "muni_code") # Only select needed geo cols
  } else {
    message("Warning: Geo info not joined. muni_name, uf_abbr, uf_name will be NA.")
    demographic_basis_before_gra_interp <- demographic_basis_intermediate %>%
      mutate(muni_name = NA_character_, uf_abbr = NA_character_, uf_name = NA_character_)
  }
  
  key_cols_order <- c("muni_code", "muni_name", "uf_abbr", "uf_name", "ano", 
                      "pop_total_est", "pop_total_original_raw")
  demographic_basis_before_gra_interp <- demographic_basis_before_gra_interp %>%
    relocate(any_of(key_cols_order))
  
  # Convert NA granular pop in CENSUS years to 0 (should be integers)
  # Dynamically get granular column names from the dataframe
  current_gra_cols <- names(demographic_basis_before_gra_interp)[str_starts(names(demographic_basis_before_gra_interp), "pop_") & 
                                                                   !names(demographic_basis_before_gra_interp) %in% c("pop_total_est", "pop_total_original_raw", "pop_total_est_src")]
  if(length(current_gra_cols) > 0){
    demographic_basis_before_gra_interp <- demographic_basis_before_gra_interp %>%
      mutate(across(all_of(current_gra_cols), 
                    ~ ifelse(ano %in% master_census_years_param & is.na(.), 0L, as.integer(.)) )) # Ensure integer
  }
  
  # --- Step 8: Interpolate Granular Demographics (Integer) ---
  message("\n--- Pipeline Step 8: Interpolating Granular Demographics (Integer Output) ---")
  demographic_basis_final_interp <- demographic_basis_before_gra_interp # Start with this
  
  # Parse G,R,A from existing columns to ensure we use the right set
  # (current_gra_cols was defined above)
  if(length(current_gra_cols) > 0) {
    parsed_cols_for_interp <- str_match(current_gra_cols, "^pop_([MW])_([A-Za-z]{3})_([0-9]{2}_[0-9]{2}|[0-9]{2}p|80p)$")
    parsed_cols_for_interp_df <- na.omit(as.data.frame(parsed_cols_for_interp[,1:4, drop=FALSE]))
    names(parsed_cols_for_interp_df) <- c("original_col", "V2", "V3", "V4")
    
    if(nrow(parsed_cols_for_interp_df) > 0 && all(sapply(parsed_cols_for_interp_df[,2:4], function(x) any(nzchar(x))))){
      INTERP_GENDERS <- unique(parsed_cols_for_interp_df$V2)
      INTERP_RACES   <- unique(parsed_cols_for_interp_df$V3) 
      INTERP_AGES    <- unique(parsed_cols_for_interp_df$V4)
      
      prepared_interp_data_main <- prepare_census_shares_for_interpolation(
        demographic_basis_before_gra_interp, master_census_years_param,
        INTERP_GENDERS, INTERP_RACES, INTERP_AGES )
      
      all_interp_shares <- prepared_interp_data_main$census_shares %>%
        select(muni_code, gender, race, age_band, ano, share) %>%
        group_by(muni_code, gender, race, age_band) %>%
        reframe(interpolate_shares_core_helper(
          group_df_shares_ano = pick(ano, share), 
          all_target_years_interpolation_span = interpolation_span_years, 
          interpolation_method_type = "spline")) %>%
        ungroup()
      
      interp_counts_long_decimal <- all_interp_shares %>%
        left_join(prepared_interp_data_main$pop_total_est_ref, by = c("muni_code", "ano")) %>%
        mutate(initial_pop = interpolated_share * pop_total_est, # pop_total_est is integer
               initial_pop = ifelse(initial_pop < 0, 0, initial_pop)) %>%
        group_by(muni_code, ano) %>%
        mutate(current_sum_gr = sum(initial_pop, na.rm = TRUE),
               adj_factor = ifelse(current_sum_gr == 0 & pop_total_est == 0, 1,
                                   ifelse(current_sum_gr == 0 & pop_total_est > 0, 0, 
                                          pop_total_est / current_sum_gr)),
               pop_interp_norm_dec = initial_pop * adj_factor) %>%
        ungroup() %>% select(muni_code, ano, gender, race, age_band, pop_interp_norm_dec, pop_total_est) # Keep pop_total_est
      
      interp_counts_integerized <- interp_counts_long_decimal %>%
        group_by(muni_code, ano) %>%
        mutate(pop_r = round(pop_interp_norm_dec), sum_pop_r = sum(pop_r, na.rm = T),
               diff_dist = pop_total_est - sum_pop_r, 
               rem = pop_interp_norm_dec - floor(pop_interp_norm_dec)) %>%
        arrange(muni_code, ano, ifelse(diff_dist > 0, desc(rem), rem), desc(pop_interp_norm_dec)) %>%
        mutate(adj_val = ifelse(row_number() <= abs(diff_dist), sign(diff_dist), 0),
               pop_int_final = pop_r + adj_val) %>%
        ungroup() %>% select(muni_code, ano, gender, race, age_band, pop_int_final)
      
      interp_counts_wide <- interp_counts_integerized %>%
        mutate(col_name = paste0("pop_", gender, "_", race, "_", age_band)) %>%
        select(muni_code, ano, col_name, pop_int_final) %>%
        pivot_wider(names_from = col_name, values_from = pop_int_final)
      
      # Merge back: df_census_years part already has integer G.R.A columns
      df_non_census_base <- demographic_basis_before_gra_interp %>% 
        filter(!(ano %in% master_census_years_param)) %>%
        select(-any_of(current_gra_cols))
      
      df_non_census_interp <- df_non_census_base %>%
        left_join(interp_counts_wide, by = c("muni_code", "ano"))
      
      df_census_part <- demographic_basis_before_gra_interp %>% filter(ano %in% master_census_years_param)
      
      # Align columns before bind_rows
      all_cols_final_structure <- names(demographic_basis_before_gra_interp)
      for(cn in all_cols_final_structure){
        if(!cn %in% names(df_census_part)) df_census_part[[cn]] <- NA_integer_
        if(!cn %in% names(df_non_census_interp)) df_non_census_interp[[cn]] <- NA_integer_
      }
      df_census_part <- df_census_part %>% select(all_of(all_cols_final_structure))
      df_non_census_interp <- df_non_census_interp %>% select(all_of(all_cols_final_structure))
      
      demographic_basis_final_interp <- bind_rows(df_census_part, df_non_census_interp) %>% arrange(muni_code, ano)
      message("      Granular demographics interpolated to integers and merged.")
    } else {
      message("      Skipping granular interpolation: G/R/A components could not be reliably parsed from column names.")
    }
  } else {
    message("      Skipping granular interpolation: No initial granular pop columns found.")
  }
  
  # --- Step 9: Final Validation (Brief) ---
  message("\n--- Pipeline Step 9: Quick Final Validation ---")
  if(length(current_gra_cols) > 0 && exists("demographic_basis_final_interp") && 
     all( (intersect(current_gra_cols, names(demographic_basis_final_interp))) == current_gra_cols) ){ # check if cols exist
    val_check <- demographic_basis_final_interp %>%
      filter(!(ano %in% master_census_years_param)) %>% # Only non-census years
      mutate(sum_g = rowSums(select(., all_of(current_gra_cols)), na.rm = TRUE)) %>%
      filter(!is.na(pop_total_est)) %>% # Only where pop_total_est is not NA
      mutate(disc = sum_g - pop_total_est)
    if(nrow(val_check) > 0 && any(val_check$disc != 0, na.rm = TRUE)){
      warning("POST-INTERPOLATION INTEGER SUM CHECK FAILED for non-census years.")
    } else if (nrow(val_check) == 0 && any(!(interpolation_span_years %in% master_census_years_param))) {
      # This case means no non-census years to check, or pop_total_est was NA for them
      message("      Post-interpolation integer sum check: No non-census years with pop_total_est to validate, or no granular data for them.")
    } else {
      message("      Post-interpolation integer sum check OK for non-census years.")
    }
  }
  
  # --- Step 10: Trim to User-Requested Time Range ---
  message("\n--- Pipeline Step 10: Trimming to Output Years: ", output_start_year, "-", output_end_year, " ---")
  final_output_df <- demographic_basis_final_interp %>%
    filter(ano >= output_start_year & ano <= output_end_year)
  
  # Final column ordering based on initial structure
  # This attempts to match the order from the non-pipelined script version
  key_cols_final <- c("muni_code", "muni_name", "uf_abbr", "uf_name", "ano")
  pop_cols_order_final <- c("pop_total_est", "pop_total_original_raw")
  # gdp_cols and cempre_cols are dynamically generated based on what was successfully renamed/joined
  gdp_cols_final <- sort(grep("^gdp_", names(final_output_df), value = TRUE))
  cempre_cols_final <- sort(grep("^cempre_", names(final_output_df), value = TRUE))
  # Granular pop detail columns (INTERP_RACES should not have 'Ign')
  if(exists("INTERP_RACES") && exists("INTERP_GENDERS") && exists("INTERP_AGES")){
    pop_detail_cols_final  <- sort(names(final_output_df)[str_starts(names(final_output_df), "pop_") & 
                                                            !names(final_output_df) %in% pop_cols_order_final])
  } else {
    pop_detail_cols_final <- c()
  }
  
  pipeline_col_order <- unique(c(
    key_cols_final, pop_cols_order_final, gdp_cols_final, cempre_cols_final, pop_detail_cols_final
  ))
  # Add any other columns that might exist but weren't explicitly ordered
  other_cols_final <- setdiff(names(final_output_df), pipeline_col_order)
  pipeline_col_order <- c(pipeline_col_order, sort(other_cols_final))
  
  # Select only columns that actually exist in final_output_df
  final_output_df <- final_output_df %>% select(any_of(pipeline_col_order))
  
  # --- Step 11: Save Output & Return ---
  message("\n--- Pipeline Step 11: Saving Output ---")
  if (!dir.exists(base_output_dir)) dir.create(base_output_dir, recursive = TRUE)
  
  output_file_name_actual <- paste0("demographic_basis_", 
                                    geo_tag_for_filename, "_", 
                                    output_start_year, "-", output_end_year, 
                                    "_", pipeline_version_tag, ".rds")
  output_path_actual <- file.path(base_output_dir, output_file_name_actual)
  
  saveRDS(final_output_df, file = output_path_actual)
  message("   Saved final dataset -> ", output_path_actual)
  message("   Dimensions: ", nrow(final_output_df), " rows x ", ncol(final_output_df), " columns")
  
  # Optionally save raw census totals for the processed munis
  if (nrow(raw_census_totals) > 0) {
    raw_totals_filename <- paste0("raw_census_totals_", geo_tag_for_filename, "_", 
                                  pipeline_version_tag, ".rds")
    raw_totals_path_actual <- file.path(base_output_dir, raw_totals_filename)
    # Filter raw_census_totals to only include the munis processed
    saveRDS(raw_census_totals %>% filter(muni_code %in% target_muni_codes_6dig), file = raw_totals_path_actual)
    message("   Saved raw census totals for processed scope -> ", raw_totals_path_actual)
  }
  
  message(paste0("\n======================================================================="))
  message(paste0("Demographic Basis Pipeline ", pipeline_version_tag, " FINISHED SUCCESSFULLY."))
  message(paste0("=======================================================================\n"))
  
  return(invisible(final_output_df)) # Return invisibly as it's also saved
}

# --- IV. Example Usage of the Pipeline Function ---
# message("--- Example Pipeline Usage (this will run if script is sourced/run directly) ---")

#Example 1: For a specific UF (e.g., Alagoas "AL") and specific years
AL_2005_2015_basis <- create_demographic_basis_pipeline(
  output_start_year = 2022,
  output_end_year = 2022,
  geo_selection = "AL", # IBGE code for Alagoas is 27
  base_output_dir = "output_pipeline_tests",
  pipeline_version_tag = "v1.0_AL_test"
)

# Example 2: For a couple of UFs
# SP_RJ_2010_2012_basis <- create_demographic_basis_pipeline(
#   output_start_year = 2010,
#   output_end_year = 2012,
#   geo_selection = c("SP", "RJ"), # IBGE codes: SP=35, RJ=33
#   base_output_dir = "output_pipeline_tests",
#   pipeline_version_tag = "v1.0_SP-RJ_test"
# )

# Example 3: For Brazil (WARNING: VERY TIME CONSUMING, ~5570 municipalities)
# To run for Brazil, ensure you have significant time and stable internet.
# Consider running for a smaller year range first, or a single UF.
# BR_2010_2011_basis <- create_demographic_basis_pipeline(
#   output_start_year = 2010,
#   output_end_year = 2011,
#   geo_selection = "BR",
#   base_output_dir = "output_pipeline_tests_BR",
#   pipeline_version_tag = "v1.0_BR_test_small_range"
# )

# To use the function:
# 1. Source this entire script.
# 2. Then call the function in your R console, e.g.:
#    my_data <- create_demographic_basis_pipeline(output_start_year = 2008, output_end_year = 2012, geo_selection = "SC")
#    View(my_data)

message("--- Script defining create_demographic_basis_pipeline() loaded. ---")
message("--- Call the function with desired parameters to generate a basis. ---")






