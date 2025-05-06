# =======================================================================
# --- Deep Analysis & Validation Script for Demographic Basis (v6) ---
# =======================================================================
# Purpose: Loads the final 'demographic_basis_final' table (v10 - T2093 Schema)
#          and performs extensive checks. Consolidates findings.
#
# Key Changes in v6:
#   - Corrected syntax errors from v5.
#   - Updated expected column patterns (`pop_detail_pattern_t2093`) to match
#     the T2093-derived schema output by Builder v10.
#   - Adjusted interpretation of Check 8b (Sum vs Raw Total): Should now be
#     very close for 2000; differences expected for 2010/2022 due to coarsening.
#   - Added check for presence of CEMPRE columns explicitly.
#   - General code cleanup and improved commenting.
# =======================================================================

# --- 0. Setup: Load Libraries & Config ---
message("\n--- Starting Deep Analysis & Validation Script (v6) ---")
library(dplyr)
library(stringr)
library(tidyr)
library(knitr)
# library(ggplot2) # Uncomment for optional plotting checks

# --- Configuration ---
# !! IMPORTANT: Match this to the OUTPUT filenames of the builder script v10 !!
INPUT_FILENAME           <- "output/demographic_basis_wide_v10_T2093schema.rds"
RAW_TOTALS_FILENAME      <- "output/raw_census_totals_for_validation_v10.rds"

CENSUS_YEARS                 <- c(2000, 2010, 2022)
SUM_DIFF_THRESHOLD_ABS       <- 2    # Allow small absolute diff
ESTIMATE_VS_CENSUS_PCT_THRESHOLD <- 5.0
NON_POSITIVE_IGNORE_COLS     <- c("gdp_agr_nom_brl1k")

# --- Helper Functions ---
validation_results <- list()
add_validation_result <- function(check_name, status, details = "") {
  validation_results[[check_name]] <<- list(status = status, details = details)
}
format_details_df <- function(df, max_rows = 10) {
  if (is.null(df) || nrow(df) == 0) return("None")
  df_to_print  <- as.data.frame(head(df, max_rows))
  kable_output <- knitr::kable(df_to_print, format = "pipe", digits = 2)
  extra_msg    <- if (nrow(df) > max_rows)
    paste("\n... (and", nrow(df) - max_rows, "more rows)")
  else ""
  paste(c("", kable_output, extra_msg), collapse = "\n")
}

# --- 1. Load Data ---
message("--- 1. Loading and Basic Checks ---")
if (!file.exists(INPUT_FILENAME)) stop("Input file '", INPUT_FILENAME, "' not found.")
demographic_basis <- readRDS(INPUT_FILENAME)
if (is.null(demographic_basis) || nrow(demographic_basis) == 0)
  stop("Loaded data is NULL or empty.")
add_validation_result(
  "Data Loading", "OK",
  paste("Loaded", nrow(demographic_basis), "rows and", ncol(demographic_basis),
        "columns from", INPUT_FILENAME)
)

raw_census_totals_loaded <- NULL
if (file.exists(RAW_TOTALS_FILENAME)) {
  raw_census_totals_loaded <- readRDS(RAW_TOTALS_FILENAME)
  if (!is.null(raw_census_totals_loaded) && nrow(raw_census_totals_loaded) > 0) {
    message("Successfully loaded raw census totals from: ", RAW_TOTALS_FILENAME)
  } else {
    warning("Loaded raw census totals file is empty or NULL: ", RAW_TOTALS_FILENAME)
    raw_census_totals_loaded <- NULL
  }
} else {
  warning("Raw census totals file not found: ", RAW_TOTALS_FILENAME, ". Check 8b will be skipped.")
}

# --- 2. Column Structure & Naming Convention ---
message("--- 2. Checking Column Structure & Naming ---")
all_cols                <- names(demographic_basis)
key_geo_cols_expected   <- c("muni_code", "muni_name", "uf_abbr", "uf_name", "ano")
key_geo_cols_present    <- intersect(key_geo_cols_expected, all_cols)
missing_key_geo         <- setdiff(key_geo_cols_expected, key_geo_cols_present)
if (length(missing_key_geo) > 0) {
  add_validation_result("Key/Geo Columns", "FAIL",
                        paste("Missing:", paste(missing_key_geo, collapse = ", ")))
} else {
  add_validation_result("Key/Geo Columns", "OK")
}

pop_est_col                  <- "pop_total_est"
gdp_pattern                  <- "^gdp_"
cempre_pattern               <- "^cempre_"
pop_detail_pattern_t2093     <- "^pop_[MW]_(Wht|Blk|Ylw|Brn|Ind|Ign)_([0-9]{2}_[0-9]{2}|100p|Tot)$"

pop_est_present              <- pop_est_col %in% all_cols
gdp_cols_present             <- grep(gdp_pattern, all_cols, value = TRUE)
cempre_cols_present          <- grep(cempre_pattern, all_cols, value = TRUE)
pop_detail_cols_present      <- grep(pop_detail_pattern_t2093, all_cols, value = TRUE)

identified_cols <- c(
  key_geo_cols_present,
  if (pop_est_present) pop_est_col else NULL,
  gdp_cols_present,
  cempre_cols_present,
  pop_detail_cols_present
)
unexpected_cols <- setdiff(all_cols, identified_cols)

if (length(unexpected_cols) > 0) {
  add_validation_result("Column Naming", "WARN",
                        paste("Unexpected/Unclassified columns found:",
                              paste(unexpected_cols, collapse = ", ")))
} else {
  add_validation_result("Column Naming", "OK",
                        "All columns match expected patterns or key identifiers.")
}

if (!pop_est_present)
  add_validation_result("Core Data Presence", "FAIL", "Missing 'pop_total_est' column.")
if (length(gdp_cols_present) == 0)
  add_validation_result("Core Data Presence (GDP)", "WARN", "No GDP columns ('gdp_*') found.")
else
  add_validation_result("Core Data Presence (GDP)", "OK",
                        paste("Found", length(gdp_cols_present), "GDP columns."))
if (length(cempre_cols_present) == 0)
  add_validation_result("Core Data Presence (CEMPRE)", "WARN", "No CEMPRE columns ('cempre_*') found.")
else
  add_validation_result("Core Data Presence (CEMPRE)", "OK",
                        paste("Found", length(cempre_cols_present), "CEMPRE columns."))
if (length(pop_detail_cols_present) == 0)
  add_validation_result("Core Data Presence (Pop Detail)", "WARN",
                        "No granular pop columns matching T2093 schema found.")
else
  add_validation_result("Core Data Presence (Pop Detail)", "OK",
                        paste("Found", length(pop_detail_cols_present), "granular pop columns."))

# --- 3. Identifier Integrity ---
message("--- 3. Checking Identifier Integrity ---")
if (length(key_geo_cols_present) > 0) {
  na_keys_summary <- demographic_basis %>%
    summarise(across(all_of(key_geo_cols_present), ~sum(is.na(.)))) %>%
    select(where(~ . > 0))
  if (ncol(na_keys_summary) > 0) {
    add_validation_result("Identifier NAs", "FAIL",
                          paste("NAs found:", format_details_df(na_keys_summary)))
  } else {
    add_validation_result("Identifier NAs", "OK")
  }
  
  if ("muni_code" %in% key_geo_cols_present) {
    invalid_muni_codes <- demographic_basis %>%
      filter(is.na(muni_code) | !str_detect(muni_code, "^[0-9]{7}$")) %>%
      distinct(muni_code)
    if (nrow(invalid_muni_codes) > 0) {
      add_validation_result("Identifier Format (muni_code)", "FAIL",
                            paste("Invalid muni_codes:", format_details_df(invalid_muni_codes)))
    } else {
      add_validation_result("Identifier Format (muni_code)", "OK")
    }
  }
  
  if (all(c("muni_code", "ano") %in% key_geo_cols_present)) {
    duplicates <- demographic_basis %>%
      group_by(muni_code, ano) %>%
      filter(n() > 1) %>%
      ungroup()
    if (nrow(duplicates) > 0) {
      add_validation_result("Identifier Uniqueness", "FAIL",
                            paste(nrow(duplicates), "duplicate muni_code/ano. Example:",
                                  format_details_df(duplicates %>% select(muni_code, ano))))
    } else {
      add_validation_result("Identifier Uniqueness", "OK")
    }
  }
} else {
  add_validation_result("Identifier Checks", "SKIPPED", "Key geo columns missing.")
}

# --- 4. Data Type Checks ---
message("--- 4. Checking Data Types ---")
numeric_cols_expected <- c(
  if (pop_est_present) pop_est_col else NULL,
  gdp_cols_present,
  cempre_cols_present,
  pop_detail_cols_present
)
char_cols_expected <- setdiff(key_geo_cols_present, "ano")
issues <- character()

for (col in numeric_cols_expected) {
  if (col %in% names(demographic_basis)) {
    if (!is.numeric(demographic_basis[[col]])) {
      issues <- c(issues,
                  paste0(col, ": Expected numeric, got ", class(demographic_basis[[col]])[1]))
    }
  }
}

for (col in char_cols_expected) {
  if (col %in% names(demographic_basis)) {
    if (!is.character(demographic_basis[[col]])) {
      issues <- c(issues,
                  paste0(col, ": Expected character, got ", class(demographic_basis[[col]])[1]))
    }
  }
}

if ("ano" %in% names(demographic_basis)) {
  if (!is.numeric(demographic_basis[["ano"]]) && !is.integer(demographic_basis[["ano"]])) {
    issues <- c(issues,
                paste0("ano: Expected integer/numeric, got ",
                       class(demographic_basis[["ano"]])[1]))
  }
}

if (length(issues) > 0) {
  add_validation_result("Data Types", "FAIL",
                        paste("Type mismatches found:", paste(issues, collapse = "; ")))
} else {
  add_validation_result("Data Types", "OK")
}

# --- 5. Interpolated Population ('pop_total_est') Analysis ---
message("--- 5. Analyzing Interpolated Population Estimate ---")
if (pop_est_present) {
  pop_col       <- pop_est_col
  na_pop        <- sum(is.na(demographic_basis[[pop_col]]))
  if (na_pop > 0) {
    add_validation_result("Pop Estimate NAs", "FAIL",
                          paste(na_pop, "NAs in", pop_col))
  } else {
    add_validation_result("Pop Estimate NAs", "OK")
  }
  nonpos_pop    <- sum(demographic_basis[[pop_col]] <= 0, na.rm = TRUE)
  if (nonpos_pop > 0) {
    add_validation_result("Pop Estimate Non-Positive", "FAIL",
                          paste(nonpos_pop, "non-positive in", pop_col))
  } else {
    add_validation_result("Pop Estimate Non-Positive", "OK")
  }
  pop_trend_issues <- demographic_basis %>%
    filter(!is.na(.data[[pop_col]])) %>%
    group_by(muni_code) %>%
    arrange(ano) %>%
    mutate(
      pop_lag      = lag(.data[[pop_col]]),
      pop_diff_pct = (.data[[pop_col]] - pop_lag) / pop_lag * 100
    ) %>%
    filter(pop_diff_pct < -10) %>%
    ungroup() %>%
    select(muni_code, ano, .data[[pop_col]], pop_lag, pop_diff_pct)
  
  if (nrow(pop_trend_issues) > 0) {
    add_validation_result("Pop Estimate Trend", "WARN",
                          paste(nrow(pop_trend_issues),
                                ">10% decreases. Details:",
                                format_details_df(pop_trend_issues)))
  } else {
    add_validation_result("Pop Estimate Trend", "OK")
  }
} else {
  add_validation_result("Pop Estimate Analysis", "SKIPPED",
                        paste("Column '", pop_est_col, "' not found.", sep=""))
}

# --- 6. Annual Indicators (GDP, CEMPRE) Analysis ---
message("--- 6. Analyzing Annual Indicators (GDP, CEMPRE) ---")
annual_cols_check <- c(gdp_cols_present, cempre_cols_present)
if (length(annual_cols_check) > 0) {
  na_annual_summary <- demographic_basis %>%
    summarise(across(all_of(annual_cols_check), ~sum(is.na(.)))) %>%
    select(where(~ . > 0))
  if (ncol(na_annual_summary) > 0) {
    df_long <- na_annual_summary %>% pivot_longer(everything(),
                                                  names_to="column",
                                                  values_to="na_count")
    add_validation_result("Annual Indicator NAs", "INFO",
                          paste("NAs present (expected). Counts:",
                                format_details_df(df_long)))
  } else {
    add_validation_result("Annual Indicator NAs", "OK", "No NAs found.")
  }
  
  cols_for_nonpos_check <- setdiff(annual_cols_check, NON_POSITIVE_IGNORE_COLS)
  if (length(cols_for_nonpos_check) > 0) {
    nonpos_annual_summary <- demographic_basis %>%
      summarise(across(all_of(cols_for_nonpos_check), ~sum(.x <= 0, na.rm=TRUE))) %>%
      select(where(~ . > 0))
    if (ncol(nonpos_annual_summary) > 0) {
      df_long2 <- nonpos_annual_summary %>% pivot_longer(everything(),
                                                         names_to="column",
                                                         values_to="nonpos_count")
      add_validation_result("Annual Indicator Non-Positive", "WARN",
                            paste("Non-positive values found:",
                                  format_details_df(df_long2)))
    } else {
      add_validation_result("Annual Indicator Non-Positive", "OK")
    }
  } else {
    add_validation_result("Annual Indicator Non-Positive", "SKIPPED", "No applicable columns.")
  }
} else {
  add_validation_result("Annual Indicator Analysis", "SKIPPED",
                        "No applicable GDP or CEMPRE columns found.")
}

# --- 7. Granular Census Data Structure ---
message("--- 7. Analyzing Granular Census Data Structure (T2093 Schema) ---")
if (length(pop_detail_cols_present) > 0) {
  na_outside_census <- demographic_basis %>%
    filter(!ano %in% CENSUS_YEARS) %>%
    summarise(across(all_of(pop_detail_cols_present), ~sum(!is.na(.)))) %>%
    select(where(~ . > 0))
  if (ncol(na_outside_census) > 0) {
    df_long3 <- na_outside_census %>% pivot_longer(everything(),
                                                   names_to="column",
                                                   values_to="non_na_count")
    add_validation_result("Census Data NAs (Outside Census)", "WARN",
                          paste("Non-NA outside census years:",
                                format_details_df(df_long3)))
  } else {
    add_validation_result("Census Data NAs (Outside Census)", "OK",
                          "NA outside census years as expected.")
  }
  
  na_within_census <- demographic_basis %>%
    filter(ano %in% CENSUS_YEARS) %>%
    summarise(across(all_of(pop_detail_cols_present), ~sum(is.na(.)))) %>%
    select(where(~ . > 0))
  if (ncol(na_within_census) > 0) {
    df_long4 <- na_within_census %>% pivot_longer(everything(),
                                                  names_to="column",
                                                  values_to="na_count")
    add_validation_result("Census Data NAs (Within Census)", "FAIL",
                          paste("NAs found within census years:",
                                format_details_df(df_long4)))
  } else {
    add_validation_result("Census Data NAs (Within Census)", "OK")
  }
  
  neg_within_census_summary <- demographic_basis %>%
    filter(ano %in% CENSUS_YEARS) %>%
    summarise(across(all_of(pop_detail_cols_present), ~sum(.x < 0, na.rm=TRUE))) %>%
    select(where(~ . > 0))
  if (ncol(neg_within_census_summary) > 0) {
    df_long5 <- neg_within_census_summary %>% pivot_longer(everything(),
                                                           names_to="column",
                                                           values_to="neg_count")
    add_validation_result("Census Data Negatives (Within Census)", "FAIL",
                          paste("Negative values found:",
                                format_details_df(df_long5)))
  } else {
    add_validation_result("Census Data Negatives (Within Census)", "OK")
  }
} else {
  add_validation_result("Granular Census Structure", "SKIPPED", "No granular population columns found.")
}

# --- 8. Granular Census Data Consistency (Census Years Only) ---
message("--- 8. Analyzing Granular Census Data Consistency (Census Years) ---")
census_year_data <- demographic_basis %>% filter(ano %in% CENSUS_YEARS)

if (length(pop_detail_cols_present) > 0 && nrow(census_year_data) > 0) {
  
  # 8a: Sum vs Estimate
  if (pop_est_present) {
    summ_vs_est <- census_year_data %>%
      mutate(
        sum_calc = round(rowSums(select(., all_of(pop_detail_cols_present)), na.rm=TRUE)),
        estimate = .data[[pop_est_col]],
        diff_abs = abs(sum_calc - estimate),
        diff_pct = ifelse(estimate>0, round(diff_abs/estimate*100,2), NA_real_)
      ) %>%
      filter(diff_pct > ESTIMATE_VS_CENSUS_PCT_THRESHOLD |
               (diff_abs > SUM_DIFF_THRESHOLD_ABS*10 & estimate < 1000)) %>%
      select(muni_code, ano, sum_calc, estimate, diff_abs, diff_pct) %>%
      arrange(desc(diff_abs))
    
    note_8a <- "Sum of detailed vs interpolated estimate; differences expected."
    if (nrow(summ_vs_est)>0) {
      add_validation_result("Sum vs Estimate (Census Years)", "INFO",
                            paste(note_8a, "Found", nrow(summ_vs_est),
                                  "rows. Details:", format_details_df(summ_vs_est)))
    } else {
      add_validation_result("Sum vs Estimate (Census Years)", "OK",
                            paste(note_8a, "All within threshold."))
    }
  } else {
    add_validation_result("Sum vs Estimate (Census Years)", "SKIPPED",
                          paste("Column '", pop_est_col, "' not found."))
  }
  
  # 8b: Sum vs Original Raw
  if (!is.null(raw_census_totals_loaded)) {
    summ_vs_raw <- census_year_data %>%
      left_join(raw_census_totals_loaded, by=c("muni_code","ano")) %>%
      filter(!is.na(pop_total_original_raw)) %>%
      mutate(
        sum_calc = round(rowSums(select(., all_of(pop_detail_cols_present)), na.rm=TRUE)),
        diff_abs = abs(sum_calc - pop_total_original_raw)
      ) %>%
      filter(diff_abs > SUM_DIFF_THRESHOLD_ABS) %>%
      select(muni_code, ano, sum_calc, pop_total_original_raw, diff_abs) %>%
      arrange(ano, desc(diff_abs))
    
    note_8b <- "Sum vs original raw census total; checks mapping consistency."
    if (nrow(summ_vs_raw)>0) {
      status8b <- if (any(summ_vs_raw$ano != 2000)) "FAIL" else "INFO"
      add_validation_result("Sum vs Original Raw Total", status8b,
                            paste(note_8b, "Found", nrow(summ_vs_raw),
                                  "rows. Details:", format_details_df(summ_vs_raw)))
    } else {
      add_validation_result("Sum vs Original Raw Total", "OK",
                            paste(note_8b, "All within tolerance."))
    }
  } else {
    add_validation_result("Sum vs Original Raw Total", "SKIPPED",
                          "Raw census totals not loaded.")
  }
  
} else {
  add_validation_result("Granular Census Consistency", "SKIPPED",
                        "No granular pop cols or census-year data.")
}

# --- 9. Final Report ---
message("\n\n======================================================")
message("--- Final Validation Report (v6 - T2093 Schema) ---")
message("======================================================")
message("Input File: ", INPUT_FILENAME)
message("Raw Totals File: ",
        if (!is.null(raw_census_totals_loaded)) RAW_TOTALS_FILENAME else "None")
message("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
message("\n--- Summary of Checks ---")
if (length(validation_results)>0) {
  statuses       <- sapply(validation_results, function(x) x$status)
  status_summary <- table(factor(statuses,
                                 levels=c("OK","INFO","WARN","FAIL","SKIPPED")))
  print(status_summary)
} else {
  message("No validation checks recorded.")
}

message("\n--- Detailed Findings ---")
if (length(validation_results)>0) {
  for (name in names(validation_results)) {
    res <- validation_results[[name]]
    cat(sprintf("\n[%s] %s\n", res$status, name))
    if (nzchar(res$details) && res$details!="None") {
      lines <- strwrap(res$details, width=100, prefix="   ", initial="   ")
      cat(paste(lines, collapse="\n"), "\n")
    }
  }
} else {
  message("No detailed findings available.")
}

message("\n======================================================")
message("--- Validation Script Finished ---")
message("======================================================")
