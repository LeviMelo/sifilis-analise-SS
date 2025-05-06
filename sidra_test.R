# ===========================================
# Enhanced Inspect SIDRA Fetches Script (v2)
# ===========================================
# Purpose:
#   Fetch SIDRA data and provide a detailed report for each table,
#   including glimpse, key column unique values, value summary,
#   variable/unit cross-tab, and summaries of all potential
#   classifier columns.
# ===========================================

# ---------------------------
# 1. Load Necessary Libraries
# ---------------------------
library(purrr)
library(sidrar)
library(dplyr)
library(furrr)
library(future)
library(readr)
library(stringr)
library(knitr) # For formatted tables in output

# ---------------------------
# 2. Configuration Section
# ---------------------------
municipality_code <- "2700300" # Arapiraca, Alagoas
fetch_full_data <- TRUE
sidra_tables <- list(
  list(table = 6579, description = "EstimaPop Est. Pop. Res."),
  list(table = 9509, description = "CEMPRE Local Units & Enterprises (22)"),
  list(table = 1685, description = "CEMPRE Local Units & Enterprises (06-21)"),
  list(table = 5938, description = "PIBmuni2010 GDP at Current Prices"),
  list(table = 9606, description = "CENSO 22/10 Pop. by Race/Age/Gender"),
  list(table = 2093, description = "CENSO 00/10 Pop. by Race/Age/Gender")
)

# ---------------------------
# 3. Setup Parallel Processing
# ---------------------------
available_cores <- future::availableCores()
workers <- max(1, available_cores - 1)
message(paste("Setting up parallel processing with", workers, "workers."))
future::plan(multisession, workers = workers)

# ---------------------------
# 4. Define Data Fetching Function (Simplified Logging)
# ---------------------------
fetch_sidra_data_for_inspect <- function(table_info, municipality_code, fetch_full_data) {
  table_id <- table_info$table
  description <- table_info$description
  message(paste("Attempting fetch for table:", table_id, "-", description))
  period <- if (fetch_full_data) "all" else "last"
  data <- tryCatch({
    # Use suppressMessages to avoid the "Considering all categories..." message
    suppressMessages(sidrar::get_sidra( x = table_id, variable = "all", period = period,
                                        geo = "City", geo.filter = list("City" = municipality_code), format = 4 ))
  }, error = function(e) {
    message(paste("  -> FAILED Fetch for table:", table_id, "-", description, "Error:", e$message)); return(NULL)
  })
  if (!is.null(data) && nrow(data) > 0) {
    message(paste("  -> SUCCESS Fetch for table:", table_id, "-", description, "(", nrow(data), "rows )"))
    # Convert factors potentially created by older sidrar versions to character
    data <- data %>% mutate(across(where(is.factor), as.character))
    # Add metadata
    data <- data %>% mutate( Fetched_Table_ID = table_id, Fetched_Table_Desc = description ) %>%
      select(Fetched_Table_ID, Fetched_Table_Desc, everything())
    return(data)
  } else if (!is.null(data) && nrow(data) == 0) {
    message(paste("  -> SUCCESS Fetch for table:", table_id, "-", description, "( 0 rows returned )")); return(NULL)
  } else { return(NULL) }
}

# ---------------------------
# 5. Parallel Data Fetching
# ---------------------------
message("\nStarting parallel data fetch...")
table_datasets_raw <- future_map(
  .x = sidra_tables,
  .f = ~ fetch_sidra_data_for_inspect(table_info = .x, municipality_code = municipality_code, fetch_full_data = fetch_full_data ),
  .options = furrr_options(seed = TRUE)
)
names(table_datasets_raw) <- paste0("table_", purrr::map_int(sidra_tables, "table"))
table_datasets_raw <- table_datasets_raw[!sapply(table_datasets_raw, is.null)]
message(paste("\nSuccessfully fetched data for", length(table_datasets_raw), "tables."))

# ---------------------------
# 6. Enhanced Inspection of Fetched Data Structures
# ---------------------------
message("\n--- Enhanced Inspection of Structure & Content ---")

# Helper function for limited printing of unique values
print_unique_limited <- function(vals, limit = 20) {
  if (length(vals) > limit) {
    print(head(vals, limit))
    cat("... (and", length(vals) - limit, "more unique values)\n")
  } else {
    print(vals)
  }
}

if (length(table_datasets_raw) > 0) {
  purrr::iwalk(table_datasets_raw, ~{
    data <- .x # Current dataframe
    table_name <- .y # Name like "table_6579"
    
    cat("\n\n=======================================================================\n")
    cat(paste("Detailed Inspection for:", table_name, "-", unique(data$Fetched_Table_Desc)))
    cat("\n=======================================================================\n")
    
    # --- 1. Basic Info & Glimpse ---
    cat(paste("\n--- 1. Overview ---\n"))
    cat(paste("Dimensions:", nrow(data), "rows x", ncol(data), "columns\n"))
    cat("Glimpse:\n")
    dplyr::glimpse(data)
    cat("\n")
    
    # --- 2. Key Identifier Columns ---
    cat(paste("\n--- 2. Key Identifiers ---\n"))
    cat("Unique Years ('Ano'):\n")
    print(sort(unique(data$Ano)))
    
    # Check for consistency in Muni code (should be one)
    muni_codes <- unique(data$`Município (Código)`)
    cat("\nUnique Municipality Codes ('Município (Código)'):\n")
    print(muni_codes)
    if(length(muni_codes) > 1) {
      cat(">>> WARNING: More than one Municipality Code found in this table fetch!\n")
    }
    
    # --- 3. Value Column Summary ---
    cat(paste("\n--- 3. Value Column ('Valor') Summary ---\n"))
    if ("Valor" %in% names(data) && is.numeric(data$Valor)) {
      print(summary(data$Valor))
    } else {
      cat("Column 'Valor' not found or is not numeric.\n")
    }
    
    # --- 4. Variable and Unit Analysis ---
    cat(paste("\n--- 4. Variable and Unit ('Variável', 'Unidade de Medida') ---\n"))
    if ("Variável" %in% names(data) && "Unidade de Medida" %in% names(data)) {
      var_unit_summary <- data %>%
        count(Variável, `Unidade de Medida`, name = "RowCount") %>%
        arrange(Variável)
      cat("Variable / Unit Combinations Present:\n")
      print(knitr::kable(var_unit_summary, format = "pipe")) # Formatted table
    } else {
      cat("Columns 'Variável' or 'Unidade de Medida' not found.\n")
    }
    
    # --- 5. Classifier Column Analysis ---
    cat(paste("\n--- 5. Potential Classifier Columns ---\n"))
    # Define core ID/Value/Metadata columns to exclude from classifiers
    core_cols <- c("Fetched_Table_ID", "Fetched_Table_Desc",
                   "Nível Territorial (Código)", "Nível Territorial",
                   "Unidade de Medida (Código)", "Unidade de Medida",
                   "Valor", "Município (Código)", "Município",
                   "Ano (Código)", "Ano", "Variável (Código)", "Variável")
    
    # Identify potential classifiers (character or factor cols not in core_cols)
    potential_classifiers <- data %>%
      select(where(is.character), -any_of(core_cols)) %>%
      names()
    
    if (length(potential_classifiers) > 0) {
      cat("Found potential classifiers:", paste(potential_classifiers, collapse=", "), "\n")
      for (col in potential_classifiers) {
        cat(paste("\n--- Summary for Classifier:", col, "---\n"))
        # Unique Values
        unique_vals <- unique(data[[col]])
        cat("Unique Values:\n")
        print_unique_limited(unique_vals, limit = 25) # Limit print length
        # Frequency Counts
        cat("\nFrequency Counts:\n")
        freq_table <- data %>% count(!!sym(col), name = "Count") %>% arrange(desc(Count))
        print(knitr::kable(freq_table, format="pipe"))
      }
    } else {
      cat("No additional potential classifier columns identified.\n")
    }
    cat("\n") # End of section for this table
  })
} else {
  message("No data was successfully fetched. Cannot inspect structures.")
}

# ---------------------------
# 7. End of Inspection Script
# ---------------------------
message("\n--- Enhanced Inspection Script Finished ---")