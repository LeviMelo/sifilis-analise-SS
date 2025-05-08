# --- 0. Load Required Libraries ---
# Ensure all necessary packages are installed.
# install.packages(c("read.dbc", "dplyr", "geobr", "stringr", "sf", "tidyr", "skimr", "knitr"))

library(read.dbc)
library(dplyr)
library(geobr)
library(stringr)
library(sf)
library(tidyr)
library(skimr) # For rich summaries
library(knitr) # For kable (nice tables)

# --- USER CONFIGURATION ---
START_YEAR <- 2010 # Inclusive
END_YEAR <- 2022   # Inclusive
TARGET_UF_ABBREV <- "AL" # State abbreviation (e.g., "AL", "SP", "RJ") or "BR" for all Brazil
# TARGET_UF_ABBREV <- "BR" # Example for all Brazil

# --- 1. Download and Load Primary Data (Multiple Years) ---
all_sifg_data_list <- list()
base_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/PRELIM/" # Base URL for SIFG data

message(paste0("--- Processing SIFG data from ", START_YEAR, " to ", END_YEAR, " ---"))

for (year_val in START_YEAR:END_YEAR) {
  year_short <- substr(as.character(year_val), 3, 4)
  ftp_filename <- paste0("SIFGBR", year_short, ".dbc") # Gestational Syphilis for Brazil
  local_destfile <- ftp_filename
  full_url <- paste0(base_url, ftp_filename)
  
  message(paste0("\n--- Processing Year: ", year_val, " (File: ", ftp_filename, ") ---"))
  
  if (!file.exists(local_destfile)) {
    message("Downloading ", local_destfile, " from DATASUS FTP: ", full_url)
    tryCatch({
      download.file(full_url, destfile = local_destfile, mode = "wb", quiet = FALSE)
      message("Download complete for ", local_destfile, ".")
    }, error = function(e) {
      message("Error downloading ", local_destfile, ": ", e$message)
      message("Skipping this file.")
      next
    })
  } else {
    message(local_destfile, " already exists. Skipping download.")
  }
  
  if (file.exists(local_destfile)) {
    message("Reading .dbc file: ", local_destfile, "...")
    current_locale <- Sys.getlocale("LC_CTYPE")
    Sys.setlocale("LC_CTYPE", "en_US.UTF-8")
    
    sifg_year_data <- NULL
    tryCatch({
      sifg_year_data <- read.dbc::read.dbc(local_destfile)
      message("Successfully loaded data for ", year_val, " (", nrow(sifg_year_data), " rows).")
      sifg_year_data$ANO_SGB <- year_val # Year from filename, for verification/use
      all_sifg_data_list[[as.character(year_val)]] <- sifg_year_data
    }, error = function(e) {
      message("Error reading DBC file ", local_destfile, ": ", e$message)
      message("Skipping this file's data.")
    }, finally = {
      Sys.setlocale("LC_CTYPE", current_locale)
    })
  }
}

if (length(all_sifg_data_list) == 0) {
  stop("No SIFG data could be loaded. Aborting.")
}

message("\nCombining data from all loaded years...")
sifg_data_raw_all_ufs <- dplyr::bind_rows(all_sifg_data_list)
message("Combined raw data (all UFs initially) contains ", nrow(sifg_data_raw_all_ufs), " rows from ",
        length(unique(sifg_data_raw_all_ufs$ANO_SGB)), " year(s).")
# rm(all_sifg_data_list, sifg_year_data); gc()


# --- 2. Prepare Geographic Lookups (Early, for UF filtering if needed) ---
message("\nPreparing geographic lookups (geobr)...")
munis_geo_lookup <- NULL
states_geo_lookup <- NULL
tryCatch({
  munis_geo_lookup <- geobr::read_municipality(year = 2020) %>%
    select(code_muni, name_muni) %>%
    sf::st_drop_geometry() %>%
    mutate(code_muni = as.character(code_muni))
  
  states_geo_lookup <- geobr::read_state(year = 2020) %>%
    select(code_state, name_state, abbrev_state) %>%
    sf::st_drop_geometry() %>%
    mutate(code_state = as.character(code_state))
  message("Lean geographic lookups created.")
}, error = function(e) {
  message("Warning: Error preparing geobr lookups: ", e$message)
  if(is.null(munis_geo_lookup)) munis_geo_lookup <- tibble(code_muni=character(), name_muni=character())
  if(is.null(states_geo_lookup)) states_geo_lookup <- tibble(code_state=character(), name_state=character(), abbrev_state=character())
})

# --- 3. Filter Data for Target UF (if not "BR") ---
sifg_data_filtered_untranslated <- sifg_data_raw_all_ufs # Keep a copy before potential UF filtering

if (TARGET_UF_ABBREV != "BR") {
  message(paste0("\nFiltering data for UF: ", TARGET_UF_ABBREV, "..."))
  
  target_uf_info <- states_geo_lookup %>% filter(abbrev_state == toupper(TARGET_UF_ABBREV))
  
  if (nrow(target_uf_info) == 0) {
    stop(paste0("UF abbreviation '", TARGET_UF_ABBREV, "' not found in geobr state lookup. Please use a valid 2-letter abbreviation or 'BR'."))
  }
  
  target_uf_code <- as.character(target_uf_info$code_state[1])
  message(paste0("Filtering by SG_UF_NOT == '", target_uf_code, "' (for ", TARGET_UF_ABBREV, ")."))
  
  # Ensure SG_UF_NOT is character for comparison
  sifg_data_filtered_untranslated <- sifg_data_raw_all_ufs %>%
    mutate(SG_UF_NOT = as.character(SG_UF_NOT)) %>% # Ensure character type
    filter(SG_UF_NOT == target_uf_code)
  
  if(nrow(sifg_data_filtered_untranslated) == 0) {
    warning(paste0("No data found for UF '", TARGET_UF_ABBREV, "' (code '", target_uf_code, "') within the selected years. ",
                   "Proceeding with empty dataset for this UF."))
  } else {
    message("Filtered data for ", TARGET_UF_ABBREV, " (untranslated) contains ", nrow(sifg_data_filtered_untranslated), " rows.")
  }
} else {
  message("\nProcessing data for all of Brazil (TARGET_UF_ABBREV = 'BR'). No UF-specific filtering applied at this stage.")
}
# rm(sifg_data_raw_all_ufs); gc()


# --- 4. Initial Exploration of Filtered UNTRANSLATED Data ---
message("\n\n--- Section 4: Initial Exploration of Filtered UNTRANSLATED Data ---")
if (nrow(sifg_data_filtered_untranslated) > 0) {
  message("Exploring the dataset BEFORE translation for UF: ", TARGET_UF_ABBREV)
  message("Dimensions: ", nrow(sifg_data_filtered_untranslated), " rows, ", ncol(sifg_data_filtered_untranslated), " columns.")
  
  message("\nFirst few rows (structure and raw values):")
  print(head(sifg_data_filtered_untranslated))
  
  message("\nColumn names and data types (using glimpse):")
  glimpse(sifg_data_filtered_untranslated)
  
  message("\nSummary of missing values (Top 10 columns with most NAs):")
  missing_summary_raw <- sifg_data_filtered_untranslated %>%
    summarise(across(everything(), ~sum(is.na(.)))) %>%
    tidyr::pivot_longer(everything(), names_to = "variable", values_to = "n_missing") %>%
    mutate(pct_missing = round((n_missing / nrow(sifg_data_filtered_untranslated)) * 100, 2)) %>%
    filter(n_missing > 0) %>%
    arrange(desc(pct_missing))
  print(kable(head(missing_summary_raw, 10), caption = "Top 10 Columns with Missing Values (Untranslated Data)"))
  
  message("\nExploring unique values in key RAW coded fields (examples):")
  key_raw_fields <- c("TPEVIDENCI", "TPTESTE1", "CS_GESTANT", "CS_RACA", "CS_ESCOL_N", "ID_MUNICIP", "SG_UF_NOT")
  for (field in key_raw_fields) {
    if (field %in% names(sifg_data_filtered_untranslated)) {
      message("\nRaw codes and counts for: ", field)
      # Ensure field is factor or character for table
      counts <- sifg_data_filtered_untranslated %>%
        mutate(!!sym(field) := as.character(!!sym(field))) %>%
        count(!!sym(field), sort = TRUE, name = "Frequency") %>%
        mutate(Percentage = round(Frequency / sum(Frequency) * 100, 2))
      
      if (n_distinct(sifg_data_filtered_untranslated[[field]]) > 15) {
        message("(Showing Top 10 due to high cardinality)")
        print(kable(head(counts, 10), caption = paste("Top 10 Raw Codes for", field)))
      } else {
        print(kable(counts, caption = paste("Raw Codes for", field)))
      }
    }
  }
  message("\nInitial exploration of untranslated data finished.")
} else {
  message("Filtered untranslated data is empty. Skipping initial exploration.")
}


# --- 5. Define Translation Mappings & Helper Functions ---
message("\nDefining translation maps and helper functions...")
# --- 5a. Custom 7-Digit IBGE Code Function ---
calculate_ibge_digit_user <- function(cod6) {
  if (is.na(cod6) || !is.character(cod6) || nchar(cod6) != 6 || !str_detect(cod6, "^[0-9]{6}$")) { return(NA_character_) }
  tryCatch({ digits <- as.numeric(strsplit(cod6, "")[[1]]); a <- digits[1]; p2 <- digits[2] * 2; b <- (p2 %% 10) + (p2 %/% 10); c <- digits[3]; p4 <- digits[4] * 2; d <- (p4 %% 10) + (p4 %/% 10); e <- digits[5]; p6 <- digits[6] * 2; f <- (p6 %% 10) + (p6 %/% 10); soma_mod10 <- (a + b + c + d + e + f) %% 10; digit <- (10 - soma_mod10) %% 10; return(as.character(digit)) }, error = function(e) { return(NA_character_) })
}
generate_code7_user <- function(code6) {
  if (is.na(code6)) return(NA_character_); digit <- calculate_ibge_digit_user(code6); if (is.na(digit)) { return(NA_character_) } else { return(paste0(code6, digit)) }
}

# --- 5b. Translation Maps ---
tpevidenci_map <- c('1'='1-Primária','2'='2-Secundária','3'='3-Terciária','4'='4-Latente','9'='9-Ignorado')
tpteste1_map <- c('1'='1-Reagente','2'='2-Não reagente','3'='3-Não realizado','9'='9-Ignorado')
tpconfirma_map <- c('1'='1-Reagente','2'='2-Não reagente','3'='3-Não realizado','9'='9-Ignorado')
tpesquema_map <- c('1'='1-Pen G benz 2.400.000UI','2'='2-Pen G benz 4.800.000UI','3'='3-Pen G benz 7.200.000UI','4'='4-Outro esquema','5'='5-Não realizado','9'='9-Ignorado')
tratparc_map <- c('1'='1-Sim','2'='2-Não','9'='9-Ignorado')
tpesqpar_map <- tpesquema_map # Same map
tpmotparc_map <- c('1'='1-Sem contato posterior','2'='2-Não comunicado/convocado','3'='3-Comunicado/convocado, não compareceu','4'='4-Comunicado/convocado, recusou tratamento','5'='5-Sorologia não reagente','6'='6-Outro motivo')
cs_gestant_map <- c('1'='1-1ºTrimestre','2'='2-2ºTrimestre','3'='3-3ºTrimestre','4'='4-Idade gestacional ignorada','9'='9-Ignorado')
cs_raca_map <- c('1'='1-Branca','2'='2-Preta','3'='3-Amarela','4'='4-Parda','5'='5-Indígena','9'='9-Ignorado')
cs_escol_n_map <- c('00'='0-Analfabeto','0'='0-Analfabeto','01'='1-EF Incompleto(1-4)','1'='1-EF Incompleto(1-4)','02'='2-EF Completo(4)','2'='2-EF Completo(4)','03'='3-EF Incompleto(5-8)','3'='3-EF Incompleto(5-8)','04'='4-EF Completo','4'='4-EF Completo','05'='5-EM Incompleto','5'='5-EM Incompleto','06'='6-EM Completo','6'='6-EM Completo','07'='7-Superior Incompleto','7'='7-Superior Incompleto','08'='8-Superior Completo','8'='8-Superior Completo','9'='9-Ignorado','09'='9-Ignorado','10'='10-Não se aplica')

all_mappings_list <- list(
  'TPEVIDENCI' = tpevidenci_map, 'TPTESTE1' = tpteste1_map, 'TPCONFIRMA' = tpconfirma_map,
  'TPESQUEMA' = tpesquema_map, 'TRATPARC' = tratparc_map, 'TPESQPAR' = tpesqpar_map,
  'TPMOTPARC' = tpmotparc_map, 'CS_GESTANT' = cs_gestant_map, 'CS_RACA' = cs_raca_map,
  'CS_ESCOL_N' = cs_escol_n_map
)
explanatory_comments <- list(
  'TPEVIDENCI'="Clinical classification", 'TPTESTE1'="Non-treponemal test result",
  'TPCONFIRMA'="Confirmatory test result", 'TPESQUEMA'="Woman's treatment scheme",
  'TRATPARC'="Partner treated?", 'TPESQPAR'="Partner's treatment scheme",
  'TPMOTPARC'="Reason partner not treated", 'CS_GESTANT'="Gestational age at diagnosis",
  'CS_RACA'="Race/color", 'CS_ESCOL_N'="Schooling level"
)

# --- 6. Definitive Translation Function ---
translate_sifilis_data_definitive_r <- function(df, muni_geo_lookup, state_geo_lookup, mappings_list) {
  message("\nStarting definitive translation process...")
  if(nrow(df) == 0) {
    message("Input dataframe is empty. Returning empty dataframe.")
    return(df)
  }
  
  # Ensure key columns for joins/recodes are character
  cols_to_char <- c("ID_MUNICIP", "ID_MN_RESI", "PRE_MUNIRE", "SG_UF_NOT", "SG_UF", "PRE_UFREL",
                    names(mappings_list))
  df_translated <- df %>%
    mutate(across(any_of(cols_to_char), as.character))
  
  # Geo Names
  message("Adding Municipality/State names (geobr) and updating codes to 7-digits...") # Message updated
  muni_cols_6_digit <- list("ID_MUNICIP"="NOME_MUNICIP_NOT", "ID_MN_RESI"="NOME_MUNICIP_RESI", "PRE_MUNIRE"="NOME_MUNICIP_PRENATAL")
  if (nrow(muni_geo_lookup) > 0) {
    muni_geo_lookup_distinct <- muni_geo_lookup %>% distinct(code_muni, .keep_all = TRUE)
    for (col6 in names(muni_cols_6_digit)) {
      new_col_name <- muni_cols_6_digit[[col6]]
      if (col6 %in% names(df_translated)) {
        df_translated <- df_translated %>%
          rowwise() %>%
          mutate(code7_temp = generate_code7_user(!!sym(col6))) %>% # Generate 7-digit temp code
          ungroup() %>%
          left_join(muni_geo_lookup_distinct, by = c("code7_temp" = "code_muni"), relationship = "many-to-one") %>%
          rename(!!new_col_name := name_muni) %>%
          # *** NEW/MODIFIED STEP: Overwrite the original 6-digit column with the 7-digit code ***
          mutate(!!sym(col6) := code7_temp) %>% 
          relocate(!!sym(new_col_name), .after = !!sym(col6)) %>%
          select(-code7_temp) # Remove temporary code, original col6 is now 7-digit
      }
    }
  } else { message("  Skipping municipality name translation and 7-digit code update (empty lookup).")}
  
  if (nrow(state_geo_lookup) > 0) {
    state_lookup_temp <- state_geo_lookup %>% distinct(code_state, .keep_all = TRUE)
    join_and_rename_state <- function(data, col_fk, new_name_col, new_abbrev_col) {
      if (col_fk %in% names(data)) {
        data <- data %>%
          left_join(state_lookup_temp, by = setNames("code_state", col_fk), relationship = "many-to-one") %>%
          rename(!!new_name_col := name_state, !!new_abbrev_col := abbrev_state) %>%
          relocate(!!sym(new_name_col), !!sym(new_abbrev_col), .after = !!sym(col_fk))
      }
      return(data)
    }
    df_translated <- df_translated %>%
      join_and_rename_state("SG_UF_NOT", "NOME_UF_NOT", "ABBREV_UF_NOT") %>%
      join_and_rename_state("SG_UF", "NOME_UF_RESI", "ABBREV_UF_RESI") %>%
      join_and_rename_state("PRE_UFREL", "NOME_UF_PRENATAL", "ABBREV_UF_PRENATAL")
  } else { message("  Skipping state name translation (empty lookup).")}
  message("  Geo names addition and municipality code update process completed.") # Message updated
  
  # Parse Age (NU_IDADE_N)
  message("Parsing age (NU_IDADE_N)...")
  if ("NU_IDADE_N" %in% names(df_translated)) {
    df_translated <- df_translated %>%
      mutate(
        NU_IDADE_N_char = as.character(NU_IDADE_N),
        AGE_UNIT_CODE = substr(NU_IDADE_N_char, 1, 1),
        AGE_VALUE_RAW = substr(NU_IDADE_N_char, 2, 4),
        AGE_VALUE = suppressWarnings(as.numeric(AGE_VALUE_RAW)),
        AGE_YEARS = case_when(
          AGE_UNIT_CODE == "4" & !is.na(AGE_VALUE) ~ AGE_VALUE,
          AGE_UNIT_CODE == "3" & !is.na(AGE_VALUE) ~ AGE_VALUE / 12,
          AGE_UNIT_CODE == "2" & !is.na(AGE_VALUE) ~ AGE_VALUE / 365.25,
          TRUE ~ NA_real_
        ),
        AGE_CALCULATED_UNIT = case_when(
          AGE_UNIT_CODE == "4" ~ "Anos", AGE_UNIT_CODE == "3" ~ "Meses",
          AGE_UNIT_CODE == "2" ~ "Dias", AGE_UNIT_CODE == "1" ~ "Horas",
          TRUE ~ "Ignorado/Inválido"
        )
      ) %>%
      relocate(AGE_YEARS, AGE_CALCULATED_UNIT, AGE_VALUE, .after = NU_IDADE_N) %>%
      select(-AGE_UNIT_CODE, -NU_IDADE_N_char, -AGE_VALUE_RAW)
    message("  Added AGE_YEARS, AGE_CALCULATED_UNIT, AGE_VALUE columns.")
  } else { message("  Column NU_IDADE_N not found for age parsing.") }
  
  # Translate Categorical Codes
  message("Translating coded fields...")
  for (col_name in names(mappings_list)) {
    if (col_name %in% names(df_translated)) {
      message("  Translating: ", col_name) # Removed explanatory comment from here for brevity
      mapping_vector <- mappings_list[[col_name]]
      df_translated <- df_translated %>%
        mutate(!!sym(col_name) := dplyr::recode(!!sym(col_name), !!!mapping_vector, .default = NA_character_))
    }
  }
  message("  Categorical fields translation completed.")
  message("\nDefinitive translation process finished.")
  return(df_translated)
}

# --- 7. Run the Definitive Translation ---
if (exists("sifg_data_filtered_untranslated") && nrow(sifg_data_filtered_untranslated) > 0) {
  sifg_translated_data <- translate_sifilis_data_definitive_r(
    sifg_data_filtered_untranslated,
    munis_geo_lookup,
    states_geo_lookup,
    all_mappings_list
  )
} else {
  message("\nNo data to translate (sifg_data_filtered_untranslated is empty or does not exist).")
  sifg_translated_data <- tibble() 
}


# --- 8. Exhaustive Exploration of TRANSLATED Data ---
message("\n\n--- Section 8: Exhaustive Exploration of TRANSLATED Data ---")

if (nrow(sifg_translated_data) > 0) {
  message("Exploring the TRANSLATED dataset for UF: ", TARGET_UF_ABBREV, 
          " (Years: ", min(sifg_translated_data$ANO_SGB, na.rm=T), "-", max(sifg_translated_data$ANO_SGB, na.rm=T), ")")
  
  # --- 8.1. Overall Dataset Characteristics ---
  message("\n--- 8.1. Overall Dataset Characteristics ---")
  message("Dimensions: ", nrow(sifg_translated_data), " rows and ", ncol(sifg_translated_data), " columns.")
  message("Column Names: ")
  print(names(sifg_translated_data))
  
  message("\nData types and head preview (glimpse):")
  glimpse(sifg_translated_data)
  
  message("\nFull summary statistics with skimr::skim():")
  # skim_with(numeric = list(hist = NULL)) # To disable histograms if output is too verbose
  print(skimr::skim(sifg_translated_data))
  
  # --- 8.2. Missing Data Profile ---
  message("\n--- 8.2. Missing Data Profile (Translated Data) ---")
  missing_summary_translated <- sifg_translated_data %>%
    summarise(across(everything(), ~sum(is.na(.)))) %>%
    tidyr::pivot_longer(everything(), names_to = "variable", values_to = "n_missing") %>%
    mutate(pct_missing = round((n_missing / nrow(sifg_translated_data)) * 100, 2)) %>%
    filter(n_missing >= 0) %>% # Keep all, even those with 0 missing
    arrange(desc(pct_missing))
  
  message("Variables sorted by percentage of missing values:")
  print(kable(missing_summary_translated, caption = "Missing Data Summary (Translated Data)"))
  
  # --- 8.3. Detailed View of Selected Key Variables ---
  message("\n--- 8.3. Detailed View of Selected Key Variables ---")
  
  # Helper function to describe a column
  describe_column <- function(df, col_name) {
    if (!col_name %in% names(df)) {
      message("Column '", col_name, "' not found in the dataframe.")
      return()
    }
    
    message("\n--- Detailed Exploration for Column: ", col_name, " ---")
    
    col_data <- df[[col_name]]
    col_type <- class(col_data)[1] # Get primary class
    
    message("Data Type: ", col_type)
    
    n_missing <- sum(is.na(col_data))
    pct_missing <- round((n_missing / length(col_data)) * 100, 2)
    message("Missing Values: ", n_missing, " (", pct_missing, "%)")
    
    n_unique <- n_distinct(col_data, na.rm = TRUE)
    message("Number of Unique Values (excluding NA): ", n_unique)
    
    if (col_type %in% c("character", "factor")) {
      message("Frequency of Unique Values (Top 10 if >10, else all):")
      counts <- as.data.frame(table(col_data, useNA = "ifany"))
      colnames(counts) <- c("Value", "Frequency")
      counts <- counts %>% 
        mutate(Percentage = round(Frequency / sum(Frequency) * 100, 2)) %>%
        arrange(desc(Frequency))
      
      if (nrow(counts) > 10) {
        print(kable(head(counts, 10), row.names = FALSE, caption=paste("Top 10 values for",col_name)))
        if (nrow(counts) > 20) { # Also show bottom if very diverse
          message("Bottom 5 values:")
          print(kable(tail(counts, 5), row.names = FALSE, caption=paste("Bottom 5 values for",col_name)))
        }
      } else {
        print(kable(counts, row.names = FALSE, caption=paste("Values for",col_name)))
      }
    } else if (col_type %in% c("numeric", "integer")) {
      message("Descriptive Statistics:")
      stats <- summary(col_data)
      print(stats)
      # Additional stats if useful
      message("Std Deviation: ", round(sd(col_data, na.rm = TRUE),2))
      message("Range: ", min(col_data, na.rm=TRUE), " - ", max(col_data, na.rm=TRUE))
    } else if (col_type %in% c("Date", "POSIXct")) {
      message("Date Range: ", min(col_data, na.rm = TRUE), " to ", max(col_data, na.rm = TRUE))
      message("Frequency by Year (if applicable):")
      if(length(unique(format(col_data, "%Y"))) > 1) print(table(format(col_data, "%Y")))
    }
    # You can add more type-specific details here
  }
  
  # Columns for detailed exploration:
  # Geographic, Demographic, Clinical, Treatment, Derived, Key IDs
  cols_for_deep_dive <- c(
    "ANO_SGB", # Year from filename
    "DT_NOTIFIC", "SEM_NOT", # Notification date, week (often raw dates)
    "SG_UF_NOT", "NOME_UF_NOT", "ABBREV_UF_NOT", # Notification UF
    "ID_MUNICIP", "NOME_MUNICIP_NOT",           # Notification Municipality
    "NU_IDADE_N", "AGE_YEARS", "AGE_CALCULATED_UNIT", "AGE_VALUE", # Age
    "CS_SEXO", # Sex (should be mostly F for SIFG)
    "CS_GESTANT", "CS_RACA", "CS_ESCOL_N", # Key demographics
    "ID_OCUPA_N", # Occupation code (untranslated)
    "ID_MN_RESI", "NOME_MUNICIP_RESI", # Residence municipality
    "SG_UF", "NOME_UF_RESI", # Residence UF
    "TPEVIDENCI", "TPTESTE1", "DSTITULO1", "TPCONFIRMA", "DSTITULO2", # Clinical and lab
    "TPESQUEMA", "TRATPARC", "TPESQPAR", "TPMOTPARC" # Treatment
    # Add any other columns you deem critical for initial understanding
  )
  
  # Filter out columns not present in the dataset to avoid errors
  cols_for_deep_dive_existing <- cols_for_deep_dive[cols_for_deep_dive %in% names(sifg_translated_data)]
  
  for (col in cols_for_deep_dive_existing) {
    describe_column(sifg_translated_data, col)
  }
  
  # --- 8.4. Cross-checks and Specific Verifications ---
  message("\n--- 8.4. Cross-checks and Specific Verifications ---")
  
  # Example: Check if ANO_SGB (from filename) aligns with year from DT_NOTIFIC (if DT_NOTIFIC is parsed)
  if ("ANO_SGB" %in% names(sifg_translated_data) && "DT_NOTIFIC" %in% names(sifg_translated_data)) {
    if (inherits(sifg_translated_data$DT_NOTIFIC, "Date")) { # Check if DT_NOTIFIC is indeed a Date object
      sifg_translated_data_temp <- sifg_translated_data %>%
        mutate(
          YEAR_FROM_DT_NOTIFIC = as.numeric(format(DT_NOTIFIC, "%Y")) # Correct way to get year
        )
      
      if("YEAR_FROM_DT_NOTIFIC" %in% names(sifg_translated_data_temp) &&
         sum(!is.na(sifg_translated_data_temp$YEAR_FROM_DT_NOTIFIC)) > 0) {
        year_comparison <- table(sifg_translated_data_temp$ANO_SGB,
                                 sifg_translated_data_temp$YEAR_FROM_DT_NOTIFIC,
                                 dnn = c("ANO_SGB (Filename)", "Year from DT_NOTIFIC"),
                                 useNA="ifany")
        message("\nCross-tabulation of Year from Filename (ANO_SGB) vs. Year from DT_NOTIFIC:")
        print(knitr::kable(year_comparison, caption="Year Alignment Check"))
      } else {
        message("\nYEAR_FROM_DT_NOTIFIC could not be derived reliably or has too many NAs.")
      }
    } else {
      message("\nDT_NOTIFIC is not in Date format, skipping year comparison.")
    }
  }
  
  message("\nUntranslated codes check (example: ID_OCUPA_N - Occupation):")
  if("ID_OCUPA_N" %in% names(sifg_translated_data)){
    message("Top 10 ID_OCUPA_N codes (CBO - Brazilian Classification of Occupations):")
    ocupa_counts <- sifg_translated_data %>%
      filter(!is.na(ID_OCUPA_N) & ID_OCUPA_N != "") %>% # Exclude true NAs or empty strings
      count(ID_OCUPA_N, sort = TRUE)
    print(kable(head(ocupa_counts, 10), caption="Top 10 Untranslated Occupation Codes (ID_OCUPA_N)"))
  }
  
  message("\nFinal sanity check for some translated fields (e.g., CS_RACA):")
  if("CS_RACA" %in% names(sifg_translated_data)){
    original_codes_present_raca <- sifg_translated_data %>%
      filter(str_detect(CS_RACA, "^[0-9]$")) %>% # Check if any single digit (original code) remains
      count(CS_RACA)
    if(nrow(original_codes_present_raca) > 0){
      message("\nWarning: Potential untranslated numeric codes remaining in CS_RACA (expected 'Code-Description'):")
      print(kable(original_codes_present_raca, caption="Untranslated CS_RACA codes"))
    } else {
      message("CS_RACA appears to be correctly translated (no remaining single numeric codes detected).")
    }
  }
  
} else {
  message("\nTranslated data is empty. Skipping exhaustive data structure exploration.")
}

message("\n\n--- SCRIPT EXECUTION FINISHED ---")
message("The final translated dataset is 'sifg_translated_data'.")
if (nrow(sifg_translated_data) == 0) {
  message("WARNING: The final dataset 'sifg_translated_data' is empty. Please check configurations, data availability, or filtering steps.")
}

save(sifg_translated_data, file = "sifg_alagoas_2010_2022_processed.RData")