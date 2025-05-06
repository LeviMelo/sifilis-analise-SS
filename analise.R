# --- 0. Load Required Libraries ---
# install.packages(c("read.dbc", "dplyr", "geobr", "stringr", "sf", "tidyr"))

library(read.dbc)
library(dplyr)
library(geobr)
library(stringr)
library(sf)
library(tidyr)

# --- 1. Download and Load Primary Data ---
url_sifg <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/PRELIM/SIFGBR19.dbc"
destfile_sifg <- "SIFGBR19.dbc"

if (!file.exists(destfile_sifg)) {
  message("Downloading ", destfile_sifg, " from DATASUS FTP...")
  tryCatch({
    download.file(url_sifg, destfile = destfile_sifg, mode = "wb", quiet = FALSE)
    message("Download complete.")
  }, error = function(e) {
    message("Error downloading file: ", e$message)
    stop("Aborting due to download error.")
  })
} else {
  message(destfile_sifg, " already exists. Skipping download.")
}

message("Reading .dbc file...")
# Attempt to set locale for reading, restore afterwards
current_locale <- Sys.getlocale("LC_CTYPE")
Sys.setlocale("LC_CTYPE", "en_US.UTF-8") # Or try "pt_BR.UTF-8" or ""
tryCatch({
  sifg_data <- read.dbc::read.dbc(destfile_sifg)
  message("Successfully loaded data (", nrow(sifg_data), " rows).")
}, error = function(e) {
  message("Error reading DBC file: ", e$message)
  stop("Aborting due to read error.")
}, finally = {
  Sys.setlocale("LC_CTYPE", current_locale) # Restore original locale
})

# --- 2. Filter Data for Alagoas ---
alagoas_code <- "27"
message("Filtering data for Alagoas (using SG_UF_NOT == '", alagoas_code, "')...")
sifg_data_al <- sifg_data %>%
  filter(as.character(SG_UF_NOT) == alagoas_code)

if(nrow(sifg_data_al) == 0) {
  stop("No data found for Alagoas (SG_UF_NOT == '", alagoas_code, "'). Check the column and code.")
}
message("Filtered data contains ", nrow(sifg_data_al), " rows for Alagoas.")
# rm(sifg_data); gc() # Optional cleanup


# --- 3. Prepare Geographic Lookups (Using geobr) ---
# --- 3a. Custom 7-Digit Code Function (Needed for geobr join) ---
calculate_ibge_digit_user <- function(cod6) {
  if (is.na(cod6) || !is.character(cod6) || nchar(cod6) != 6 || !str_detect(cod6, "^[0-9]{6}$")) { return(NA_character_) }
  tryCatch({ digits <- as.numeric(strsplit(cod6, "")[[1]]); a <- digits[1]; p2 <- digits[2] * 2; b <- (p2 %% 10) + (p2 %/% 10); c <- digits[3]; p4 <- digits[4] * 2; d <- (p4 %% 10) + (p4 %/% 10); e <- digits[5]; p6 <- digits[6] * 2; f <- (p6 %% 10) + (p6 %/% 10); soma_mod10 <- (a + b + c + d + e + f) %% 10; digit <- (10 - soma_mod10) %% 10; return(as.character(digit)) }, error = function(e) { return(NA_character_) })
}
generate_code7_user <- function(code6) {
  if (is.na(code6)) return(NA_character_); digit <- calculate_ibge_digit_user(code6); if (is.na(digit)) { return(NA_character_) } else { return(paste0(code6, digit)) }
}

# --- 3b. Prepare Lean Geographic Lookups (from geobr) ---
message("Preparing optimized geographic lookups (geobr)...")
munis_geo_lookup <- NULL
states_geo_lookup <- NULL
tryCatch({
  munis_geo_lookup <- geobr::read_municipality(year = 2020) %>%
    select(code_muni, name_muni) %>%
    sf::st_drop_geometry() %>%
    mutate(code_muni = as.character(code_muni)) # Use 7-digit code as key
  
  states_geo_lookup <- geobr::read_state(year = 2020) %>%
    select(code_state, name_state, abbrev_state) %>%
    sf::st_drop_geometry() %>%
    mutate(code_state = as.character(code_state)) # Use 2-digit code as key
  message("Lean geographic lookups created.")
}, error = function(e) {
  message("Warning: Error preparing geobr lookups: ", e$message, ". Geographic translation may fail.")
  # Define empty lookups to prevent errors later
  if(is.null(munis_geo_lookup)) munis_geo_lookup <- tibble(code_muni=character(), name_muni=character())
  if(is.null(states_geo_lookup)) states_geo_lookup <- tibble(code_state=character(), name_state=character(), abbrev_state=character())
})


# --- 4. Define Translation Mappings ---
message("Defining translation maps...")
# Syphilis Specific
tpevidenci_map <- c('1' = '1 - Primária','2' = '2 - Secundária','3' = '3 - Terciária','4' = '4 - Latente','9' = '9 - Ignorado')
tpteste1_map <- c('1' = '1 - Reagente','2' = '2 - Não reagente','3' = '3 - Não realizado','9' = '9 - Ignorado')
tpconfirma_map <- c('1' = '1 - Reagente','2' = '2 - Não reagente','3' = '3 - Não realizado','9' = '9 - Ignorado')
tpesquema_map <- c('1' = '1 - Pen G benz 2.400.000UI','2' = '2 - Pen G benz 4.800.000UI','3' = '3 - Pen G benz 7.200.000UI','4' = '4 - Outro esquema','5' = '5 - Não realizado','9' = '9 - Ignorado')
tratparc_map <- c('1' = '1 - Sim','2' = '2 - Não','9' = '9 - Ignorado')
tpesqpar_map <- tpesquema_map
tpmotparc_map <- c('1' = '1 - Sem contato posterior','2' = '2 - Não comunicado/convocado','3' = '3 - Comunicado/convocado, não compareceu','4' = '4 - Comunicado/convocado, recusou tratamento','5' = '5 - Sorologia não reagente','6' = '6 - Outro motivo')
# Demographics
cs_gestant_map <- c('1' = '1 - 1º Trimestre','2' = '2 - 2º Trimestre','3' = '3 - 3º Trimestre','4' = '4 - Idade gestacional ignorada','9' = '9 - Ignorado')
cs_raca_map <- c('1' = '1 - Branca','2' = '2 - Preta','3' = '3 - Amarela','4' = '4 - Parda','5' = '5 - Indígena','9' = '9 - Ignorado')
cs_escol_n_map <- c('00'='0 - Analfabeto','0'='0 - Analfabeto','01'='1 - EF Incompleto (1-4)','1'='1 - EF Incompleto (1-4)','02'='2 - EF Completo (4)', '2'='2 - EF Completo (4)', '03'='3 - EF Incompleto (5-8)','3'='3 - EF Incompleto (5-8)','04'='4 - EF Completo','4'='4 - EF Completo','05'='5 - EM Incompleto','5'='5 - EM Incompleto','06'='6 - EM Completo','6'='6 - EM Completo','07'='7 - Superior Incompleto','7'='7 - Superior Incompleto','08'='8 - Superior Completo','8'='8 - Superior Completo','9'='9 - Ignorado','09'='9 - Ignorado','10'='10 - Não se aplica') # Shortened labels


# --- 5. Definitive Translation Function ---
translate_sifilis_data_definitive_r <- function(df, muni_geo_lookup, state_geo_lookup) {
  message("Starting definitive translation process...")
  df_translated <- df %>%
    # Ensure character type for keys before joins/recodes
    mutate(across(any_of(c("ID_MUNICIP", "ID_MN_RESI", "PRE_MUNIRE", "SG_UF_NOT",
                           "SG_UF", "PRE_UFREL", "TPEVIDENCI", "TPTESTE1", "TPCONFIRMA",
                           "TPESQUEMA", "TRATPARC", "TPESQPAR", "TPMOTPARC",
                           "CS_GESTANT", "CS_RACA", "CS_ESCOL_N")), as.character))
  
  # --- Add Geo Names (from geobr) & Relocate ---
  message("Adding Municipality/State names (geobr) and relocating...")
  # (Municipality - requires generating 7-digit code for join)
  muni_cols_6_digit <- list("ID_MUNICIP" = "NOME_MUNICIP_NOT", "ID_MN_RESI" = "NOME_MUNICIP_RESI", "PRE_MUNIRE" = "NOME_MUNICIP_PRENATAL")
  muni_geo_lookup_distinct <- muni_geo_lookup %>% distinct(code_muni, .keep_all = TRUE) # Key is 7-digit
  
  for (col6 in names(muni_cols_6_digit)) {
    new_col_name <- muni_cols_6_digit[[col6]]
    if (col6 %in% names(df_translated)) {
      # Generate 7-digit code on the fly for joining
      df_translated <- df_translated %>%
        rowwise() %>%
        mutate(code7_temp = generate_code7_user(!!sym(col6))) %>%
        ungroup() %>%
        left_join(muni_geo_lookup_distinct, by = c("code7_temp" = "code_muni")) %>%
        rename(!!new_col_name := name_muni) %>%
        relocate(!!sym(new_col_name), .after = !!sym(col6)) %>%
        select(-code7_temp) # Remove temporary code
    }
  }
  # (State - uses 2-digit code directly)
  state_lookup_temp <- state_geo_lookup %>% distinct(code_state, .keep_all = TRUE)
  if ("SG_UF_NOT" %in% names(df)) { df_translated <- df_translated %>% left_join(state_lookup_temp, by = c("SG_UF_NOT" = "code_state")) %>% rename(NOME_UF_NOT=name_state, ABBREV_UF_NOT=abbrev_state) %>% relocate(NOME_UF_NOT, ABBREV_UF_NOT, .after = SG_UF_NOT) }
  if ("SG_UF" %in% names(df)) { df_translated <- df_translated %>% left_join(state_lookup_temp, by = c("SG_UF" = "code_state")) %>% rename(NOME_UF_RESI=name_state, ABBREV_UF_RESI=abbrev_state) %>% relocate(NOME_UF_RESI, ABBREV_UF_RESI, .after = SG_UF) }
  if ("PRE_UFREL" %in% names(df)) { df_translated <- df_translated %>% left_join(state_lookup_temp, by = c("PRE_UFREL" = "code_state")) %>% rename(NOME_UF_PRENATAL=name_state, ABBREV_UF_PRENATAL=abbrev_state) %>% relocate(NOME_UF_PRENATAL, ABBREV_UF_PRENATAL, .after = PRE_UFREL) }
  message("  Municipality/State names added and relocated.")
  
  # --- Parse Age (NU_IDADE_N) & Relocate ---
  message("Parsing age (NU_IDADE_N)...")
  if ("NU_IDADE_N" %in% names(df_translated)) {
    df_translated <- df_translated %>%
      mutate(
        NU_IDADE_N_char = as.character(NU_IDADE_N),
        AGE_UNIT_CODE = substr(NU_IDADE_N_char, 1, 1),
        AGE_VALUE = suppressWarnings(as.numeric(substr(NU_IDADE_N_char, 2, 4))),
        AGE_YEARS = case_when( AGE_UNIT_CODE == "4" ~ AGE_VALUE, AGE_UNIT_CODE == "3" ~ AGE_VALUE / 12, AGE_UNIT_CODE == "2" ~ AGE_VALUE / 365.25, TRUE ~ NA_real_ ),
        AGE_CALCULATED_UNIT = case_when( AGE_UNIT_CODE == "4" ~ "Anos", AGE_UNIT_CODE == "3" ~ "Meses", AGE_UNIT_CODE == "2" ~ "Dias", AGE_UNIT_CODE == "1" ~ "Horas", TRUE ~ "Ignorado/Inválido" )
      ) %>%
      relocate(AGE_YEARS, AGE_CALCULATED_UNIT, AGE_VALUE, .after = NU_IDADE_N) %>%
      select(-AGE_UNIT_CODE, -NU_IDADE_N_char)
    message("  Added AGE_YEARS, AGE_CALCULATED_UNIT, AGE_VALUE columns.")
  } else { message("  Column NU_IDADE_N not found.") }
  
  # --- Translate Categorical Codes (Overwrite Original Column) ---
  message("Translating coded fields (overwriting original columns)...")
  mappings_list <- list( 'TPEVIDENCI' = tpevidenci_map, 'TPTESTE1' = tpteste1_map, 'TPCONFIRMA' = tpconfirma_map, 'TPESQUEMA' = tpesquema_map, 'TRATPARC' = tratparc_map, 'TPESQPAR' = tpesqpar_map, 'TPMOTPARC' = tpmotparc_map, 'CS_GESTANT' = cs_gestant_map, 'CS_RACA' = cs_raca_map, 'CS_ESCOL_N' = cs_escol_n_map )
  explanatory_comments <- list( 'TPEVIDENCI'="# Clinical classification", 'TPTESTE1'="# Non-treponemal test", 'TPCONFIRMA'="# Confirmatory test", 'TPESQUEMA'="# Woman treatment", 'TRATPARC'="# Partner treated?", 'TPESQPAR'="# Partner treatment", 'TPMOTPARC'="# Reason partner not treated", 'CS_GESTANT'="# Gestational age", 'CS_RACA'="# Race/color", 'CS_ESCOL_N'="# Schooling" )
  for (col_name in names(mappings_list)) { if (col_name %in% names(df_translated)) { message("  Translating: ", col_name, " ", explanatory_comments[[col_name]]); mapping_vector <- mappings_list[[col_name]]; df_translated <- df_translated %>% mutate( !!sym(col_name) := dplyr::recode(!!sym(col_name), !!!mapping_vector, .default = NA_character_) ) } }
  message("  Categorical fields translated.")
  
  # --- Final Explanatory Notes ---
  message("\nFinal Explanatory Notes:")
  message(" - Age parsed from NU_IDADE_N into AGE_YEARS, AGE_CALCULATED_UNIT, AGE_VALUE.")
  message(" - Municipality and State names/abbreviations added using 'geobr' package.")
  message(" - Categorical fields (Demographics, Clinical, Treatment) translated, overwriting original codes.")
  message(" - Fields requiring external DATASUS auxiliary files (Health Region, Occupation, Country Name) were NOT translated in this version due to download/processing issues.")
  if ('ID_OCUPA_N' %in% names(df_translated)) message(" - ID_OCUPA_N: CBO code remains untranslated.")
  if ('ID_REGIONA' %in% names(df_translated)) message(" - ID_REGIONA / ID_RG_RESI: Health Region codes remain untranslated.")
  if ('ID_PAIS' %in% names(df_translated)) message(" - ID_PAIS: Country code remains untranslated (likely '1' for Brasil).")
  if ('DSTITULO1' %in% names(df_translated)) message(" - DSTITULO1: VDRL/RPR titration result (interpretation needed).")
  if ('DSMOTIVO' %in% names(df_translated)) message(" - DSMOTIVO: Text for 'Other reason' (TPMOTPARC=6).")
  message(" - Locale warnings during glimpse/summary might occur due to character encoding nuances.")
  
  message("\nDefinitive translation process finished.")
  return(df_translated)
}


# --- 6. Run the Definitive Translation on Filtered Data ---
sifg_al_translated_definitive <- translate_sifilis_data_definitive_r(
  sifg_data_al,
  munis_geo_lookup,       # From geobr
  states_geo_lookup      # From geobr
)


# --- 7. Explore DEFINITIVE Translated Results ---
message("\n--- Definitive Translated Alagoas Data Structure ---")
# Use glimpse for a concise overview of columns and types
glimpse(sifg_al_translated_definitive)

message("\n--- Definitive Translated Alagoas Data Head (selected columns) ---")
# Show key translated fields and their context
cols_to_show_definitive <- c(
  'ID_MUNICIP', 'NOME_MUNICIP_NOT',         # Example municipality
  'SG_UF_NOT', 'NOME_UF_NOT', 'ABBREV_UF_NOT', # Example state
  'NU_IDADE_N', 'AGE_YEARS', 'AGE_CALCULATED_UNIT', # Age fields
  'CS_RACA', 'CS_ESCOL_N', 'CS_GESTANT', # Translated Demographics
  'ID_OCUPA_N', # Untranslated Occupation code
  'ID_REGIONA', # Untranslated Region code
  'ID_PAIS',    # Untranslated Country code
  'TPEVIDENCI', # Translated Syphilis fields
  'TPTESTE1',
  'TPESQUEMA'
)
cols_to_show_definitive <- cols_to_show_definitive[cols_to_show_definitive %in% names(sifg_al_translated_definitive)]
print(head(sifg_al_translated_definitive[, cols_to_show_definitive]))

# --- 8. (Optional) Basic Analysis on Definitive Data ---
# You can re-run the plotting code from the previous step using 'sifg_al_translated_definitive'
# Example: Summary of translated Schooling
message("\n--- Summary of Schooling Level (Definitive Data) ---")
if("CS_ESCOL_N" %in% names(sifg_al_translated_definitive)) {
  print(summary(factor(sifg_al_translated_definitive$CS_ESCOL_N)))
}