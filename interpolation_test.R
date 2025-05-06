# =======================================================================
# Granular Demographic Interpolation - Testing Script - v1
# =======================================================================
# Purpose:
#   Tests and benchmarks different methods for interpolating granular
#   demographic groups (gender, race, age_band) between census years.
#   Ensures that the sum of interpolated groups matches the authoritative
#   pop_total_est for all years.
#
# Methods to Test:
#   1. Linear interpolation of shares.
#   2. Monotonic spline interpolation of shares.
# =======================================================================

# --- 0. Setup ---
message("--- 0. Setup ---")
library(dplyr)
library(tidyr)
library(purrr)
library(zoo)
library(ggplot2)
library(stringr)
library(knitr) # For nice tables in logs

# Load the final dataset from the main builder script
BASIS_FILEPATH <- "output/demographic_basis_wide_v14.3_TargetSchema_PivotFix.rds" # UPDATE IF YOUR FILENAME IS DIFFERENT
if (!file.exists(BASIS_FILEPATH)) {
  stop("ERROR: Main demographic basis file not found at: ", BASIS_FILEPATH)
}
demographic_basis_raw <- readRDS(BASIS_FILEPATH)
message("Loaded demographic basis: ", nrow(demographic_basis_raw), " rows, ", ncol(demographic_basis_raw), " cols.")

# --- 1. Configuration & Constants ---
message("--- 1. Configuration & Constants ---")
CENSUS_YEARS <- c(2000, 2010, 2022)
ALL_TARGET_YEARS <- min(demographic_basis_raw$ano):max(demographic_basis_raw$ano)

# Define Genders, Races, Age Bands based on your final dataset structure
# (excluding 'Ign' as it was redistributed/not present)
# Extract from column names to be robust, or define manually if preferred
pop_cols <- names(demographic_basis_raw)[str_starts(names(demographic_basis_raw), "pop_") & 
                                           !names(demographic_basis_raw) %in% c("pop_total_est", "pop_total_original_raw")]
if(length(pop_cols) == 0) stop("No granular population columns (pop_G_R_A) found in the input data.")

# Derive GENDERS, RACES, AGES from the column names
# Example col: pop_M_Wht_00_04
parsed_cols <- str_match(pop_cols, "^pop_([MW])_([A-Za-z]{3})_([0-9]{2}_[0-9]{2}|[0-9]{2}p|80p)$") # Adjusted regex for 80p too
parsed_cols_df <- na.omit(as.data.frame(parsed_cols)) # Remove NAs from non-matching cols
if(nrow(parsed_cols_df) == 0) stop("Could not parse G/R/A from column names. Check pop_cols and regex.")

GENDERS <- unique(parsed_cols_df$V2)
RACES   <- unique(parsed_cols_df$V3)
AGES    <- unique(parsed_cols_df$V4) # These are your TARGET_AGE_BANDS

message("Identified Genders: ", paste(GENDERS, collapse=", "))
message("Identified Races: ", paste(RACES, collapse=", "))
message("Identified Age Bands: ", paste(AGES, collapse=", "))

# --- 2. Data Preparation Function ---
message("--- 2. Data Preparation Function ---")
prepare_census_shares <- function(demographic_basis_df) {
  message("   Preparing long format shares from census years...")
  
  # Construct the pattern for granular columns dynamically
  # Ensure this matches exactly how pop_cols were identified if GENDERS,RACES,AGES were manually set
  # For safety, re-filter based on parsed GENDERS, RACES, AGES
  
  # Create all combinations of G,R,A to form expected column names
  expected_gra_cols <- expand.grid(g=GENDERS, r=RACES, a=AGES) %>%
    mutate(col_name = paste0("pop_", g, "_", r, "_", a)) %>%
    pull(col_name)
  
  # Filter pop_cols to only those that match our G/R/A pattern and exist
  actual_gra_cols_in_data <- intersect(expected_gra_cols, names(demographic_basis_df))
  
  if(length(actual_gra_cols_in_data) == 0) {
    stop("In prepare_census_shares: No granular population columns found after constructing from G/R/A.")
  }
  
  shares_long_df <- demographic_basis_df %>%
    filter(ano %in% CENSUS_YEARS) %>%
    select(muni_code, ano, pop_total_original_raw, all_of(actual_gra_cols_in_data)) %>%
    pivot_longer(
      cols = all_of(actual_gra_cols_in_data),
      names_to = "gra_column",
      values_to = "pop_census_val"
    ) %>%
    mutate(
      # Parse gender, race, age_band from gra_column
      gender = str_match(gra_column, "^pop_([MW])_")[,2],
      race = str_match(gra_column, "^pop_[MW]_([A-Za-z]{3})_")[,2],
      age_band = str_match(gra_column, "^pop_[MW]_[A-Za-z]{3}_(.*)$")[,2]
    ) %>%
    filter(!is.na(gender) & !is.na(race) & !is.na(age_band)) %>% # Ensure parsing worked
    mutate(
      share = ifelse(pop_total_original_raw > 0, pop_census_val / pop_total_original_raw, 0)
    ) %>%
    # Ensure share is not NA if pop_total_original_raw was 0 but pop_census_val was also 0.
    # If pop_total_original_raw is 0 and pop_census_val > 0, it's an inconsistency.
    # The ifelse above handles the main case. is.na(share) should be addressed if it occurs.
    mutate(share = ifelse(is.na(share) & pop_total_original_raw == 0 & pop_census_val == 0, 0, share)) %>%
    select(muni_code, ano, gender, race, age_band, share, pop_census_val)
  
  # Dataframe of authoritative total population for all years
  pop_total_est_df <- demographic_basis_df %>%
    select(muni_code, ano, pop_total_est) %>%
    distinct()
  
  message("   Finished preparing census shares. Rows: ", nrow(shares_long_df))
  return(list(census_shares = shares_long_df, pop_total_est = pop_total_est_df))
}

# --- 3. Interpolation Core Function ---
message("--- 3. Interpolation Core Function ---")
interpolate_and_normalize_shares <- function(muni_shares_df, # df with ano, share for a single muni & G,R,A
                                             all_target_years,
                                             interpolation_method = "linear") { # "linear" or "spline"
  
  complete_years_df <- tibble(ano = all_target_years)
  data_to_interp <- complete_years_df %>%
    left_join(muni_shares_df, by = "ano") 
  
  interpolated_share_values <- rep(NA_real_, length(all_target_years)) # Initialize
  
  valid_share_points <- data_to_interp %>% filter(!is.na(share))
  
  if (nrow(valid_share_points) < 2) { 
    # Constant extrapolation or fill with 0 if all NA
    filled_shares <- zoo::na.locf(zoo::na.locf(data_to_interp$share, na.rm = FALSE), fromLast = TRUE, na.rm = FALSE)
    interpolated_share_values <- ifelse(is.na(filled_shares), 0, filled_shares)
    
  } else if (interpolation_method == "linear") {
    # --- MODIFIED HERE ---
    interp_result <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, xout = all_target_years, rule = 2)
    # zoo::na.approx with xout directly returns the interpolated values aligned with xout
    interpolated_share_values <- as.numeric(interp_result) 
    # --- END MODIFICATION ---
    
  } else if (interpolation_method == "spline") {
    if (nrow(valid_share_points) >= 2) { 
      spline_fit <- tryCatch(
        stats::splinefun(x = valid_share_points$ano, y = valid_share_points$share, method = "monoH.FC"),
        error = function(e) {
          tryCatch(stats::splinefun(x = valid_share_points$ano, y = valid_share_points$share, method = "fmm"),
                   error = function(e2) { return(NULL) })
        }
      )
      if(!is.null(spline_fit)){
        interpolated_share_values <- spline_fit(all_target_years)
      } else { 
        # --- MODIFIED HERE (FALLBACK) ---
        interp_result_fallback <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, xout = all_target_years, rule = 2)
        interpolated_share_values <- as.numeric(interp_result_fallback)
        # --- END MODIFICATION ---
      }
    } else { 
      # --- MODIFIED HERE (FALLBACK) ---
      interp_result_fallback2 <- zoo::na.approx(data_to_interp$share, x = data_to_interp$ano, xout = all_target_years, rule = 2)
      interpolated_share_values <- as.numeric(interp_result_fallback2)
      # --- END MODIFICATION ---
    }
  } else {
    stop("Unknown interpolation_method: ", interpolation_method)
  }
  
  interpolated_share_values <- pmax(0, pmin(1, interpolated_share_values))
  interpolated_share_values <- ifelse(is.na(interpolated_share_values), 0, interpolated_share_values)
  
  return(tibble(ano = all_target_years, interpolated_share = interpolated_share_values))
}

# --- 4. Main Interpolation Execution Function ---
message("--- 4. Main Interpolation Execution Function ---")
run_interpolation_method <- function(prepared_data, # list from prepare_census_shares
                                     all_target_years,
                                     method_name_id, # e.g., "linear_interp"
                                     interpolation_type # "linear" or "spline"
) {
  message(paste0("   Running interpolation method: ", method_name_id, " (type: ", interpolation_type, ")"))
  
  census_shares_df <- prepared_data$census_shares
  pop_total_est_df <- prepared_data$pop_total_est
  
  # Group by muni_code, gender, race, age_band and apply interpolation
  message("     Interpolating shares for each G.R.A group...")
  all_interpolated_shares <- census_shares_df %>%
    select(muni_code, gender, race, age_band, ano, share, pop_census_val) %>% # pop_census_val for later merging census year actuals
    group_by(muni_code, gender, race, age_band) %>%
    reframe(interpolate_and_normalize_shares(
      muni_shares_df = pick(ano, share), # pick() ensures only ano, share are passed for interpolation
      all_target_years = all_target_years,
      interpolation_method = interpolation_type
    )) %>%
    ungroup()
  message("     Finished interpolating shares.")
  
  # Join with pop_total_est to calculate initial counts
  message("     Calculating initial interpolated counts...")
  initial_counts_df <- all_interpolated_shares %>%
    left_join(pop_total_est_df, by = c("muni_code", "ano")) %>%
    mutate(initial_pop = interpolated_share * pop_total_est) %>%
    # Non-negativity (should be largely handled by share clipping, but as a safeguard)
    mutate(initial_pop = ifelse(initial_pop < 0, 0, initial_pop))
  
  # Normalize to pop_total_est
  message("     Normalizing counts to pop_total_est...")
  normalized_counts_df <- initial_counts_df %>%
    group_by(muni_code, ano) %>%
    mutate(
      current_sum_granular = sum(initial_pop, na.rm = TRUE),
      adjustment_factor = ifelse(current_sum_granular == 0 & pop_total_est == 0, 1, # Avoid 0/0 -> NaN, keep 0s
                                 ifelse(current_sum_granular == 0 & pop_total_est > 0, 0, # All shares were 0, can't distribute
                                        pop_total_est / current_sum_granular)),
      # If adjustment_factor is 0 because all shares were 0 but pop_total_est > 0, this is problematic.
      # This implies an inconsistency or a need for a different distribution strategy for such cases.
      # For now, initial_pop * 0 will make them 0.
      interpolated_pop = initial_pop * adjustment_factor
    ) %>%
    ungroup() %>%
    select(muni_code, ano, gender, race, age_band, interpolated_share, interpolated_pop) %>%
    mutate(method = method_name_id)
  
  # For census years, overwrite interpolated_pop with original pop_census_val
  # This ensures census years reflect the true (0-filled) numbers.
  # The interpolation is for non-census years.
  message("     Restoring original census year values...")
  
  # Get original census values in a compatible long format
  original_census_long <- census_shares_df %>% 
    select(muni_code, ano, gender, race, age_band, pop_census_val)
  
  final_output_df <- normalized_counts_df %>%
    left_join(original_census_long, by = c("muni_code", "ano", "gender", "race", "age_band")) %>%
    mutate(
      final_pop = ifelse(ano %in% CENSUS_YEARS, pop_census_val, interpolated_pop)
    ) %>%
    select(muni_code, ano, gender, race, age_band, interpolated_share, final_pop, method) %>%
    rename(pop = final_pop) # Consistent column name
  
  message(paste0("   Finished method: ", method_name_id, ". Rows: ", nrow(final_output_df)))
  return(final_output_df)
}


# --- 5. Execute Benchmarking ---
message("--- 5. Execute Benchmarking ---")
prepared_data_for_interp <- prepare_census_shares(demographic_basis_raw)

all_results_list <- list()

# Method 1: Linear Interpolation
all_results_list[["linear"]] <- run_interpolation_method(
  prepared_data = prepared_data_for_interp,
  all_target_years = ALL_TARGET_YEARS,
  method_name_id = "Linear",
  interpolation_type = "linear"
)

# Method 2: Spline Interpolation
all_results_list[["spline"]] <- run_interpolation_method(
  prepared_data = prepared_data_for_interp,
  all_target_years = ALL_TARGET_YEARS,
  method_name_id = "Spline (monoH.FC/fmm)",
  interpolation_type = "spline"
)

# Combine all results
combined_results_df <- bind_rows(all_results_list)
message("--- All interpolation methods executed. Total rows in combined_results_df: ", nrow(combined_results_df), " ---")


# --- 6. Validation and Consistency Checks ---
message("--- 6. Validation and Consistency Checks ---")
perform_consistency_checks <- function(results_df, pop_total_est_reference_df) {
  message("   Performing consistency checks for each method...")
  
  summary_checks <- results_df %>%
    group_by(method, muni_code, ano) %>%
    summarise(
      sum_granular_interpolated = sum(pop, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    left_join(pop_total_est_reference_df, by = c("muni_code", "ano")) %>%
    mutate(
      discrepancy = sum_granular_interpolated - pop_total_est,
      disc_pct = ifelse(pop_total_est == 0, 0, (discrepancy / pop_total_est) * 100)
    )
  
  message("\n   Summary of Discrepancies (Sum of Granular vs. pop_total_est):")
  discrepancy_report <- summary_checks %>%
    group_by(method) %>%
    summarise(
      n_obs = n(),
      n_discrepant_abs_gt_1 = sum(abs(discrepancy) > 1, na.rm = TRUE), # Allow for tiny float issues
      max_abs_discrepancy = max(abs(discrepancy), na.rm = TRUE),
      mean_abs_discrepancy = mean(abs(discrepancy), na.rm = TRUE),
      max_abs_disc_pct = max(abs(disc_pct), na.rm = TRUE)
    )
  print(knitr::kable(discrepancy_report, digits = 4, caption = "Discrepancy Report by Method"))
  
  significant_discrepancies <- summary_checks %>% filter(abs(discrepancy) > 1) # Threshold for "significant"
  if (nrow(significant_discrepancies) > 0) {
    message("\n   WARNING: Significant discrepancies found (>1 person):")
    print(knitr::kable(head(significant_discrepancies), digits = 2))
  } else {
    message("\n   Consistency Check OK: Sum of granular components matches pop_total_est within tolerance for all methods.")
  }
  
  # Recursive aggregation check (as per your validation script logic)
  message("\n   Performing recursive aggregation checks (sum by G, R, A should all match sum_granular_interpolated)...")
  recursive_check_df <- results_df %>%
    group_by(method, muni_code, ano) %>%
    summarise(
      sum_granular = sum(pop, na.rm = TRUE),
      sum_by_gender_detail = sum(pop[gender %in% GENDERS]), # Should be same as sum_granular
      sum_by_race_detail = sum(pop[race %in% RACES]),     # Should be same as sum_granular
      sum_by_age_detail = sum(pop[age_band %in% AGES]), # Should be same as sum_granular
      .groups = "drop"
    ) %>%
    mutate(
      diff_gender = abs(sum_granular - sum_by_gender_detail),
      diff_race = abs(sum_granular - sum_by_race_detail),
      diff_age = abs(sum_granular - sum_by_age_detail)
    )
  
  failed_recursive_checks <- recursive_check_df %>% 
    filter(diff_gender > 1e-6 | diff_race > 1e-6 | diff_age > 1e-6) # Check for small float diffs
  
  if(nrow(failed_recursive_checks) > 0) {
    message("\n   WARNING: Recursive aggregation checks failed for some method/muni/year combinations:")
    print(knitr::kable(head(failed_recursive_checks)))
  } else {
    message("\n   Recursive Aggregation Checks OK: Sums by G, R, A are consistent with total granular sum.")
  }
  
  return(summary_checks) # Return for potential further inspection
}

validation_summary <- perform_consistency_checks(combined_results_df, prepared_data_for_interp$pop_total_est)

# --- 7. Plotting Results ---
message("--- 7. Plotting Results ---")

# Select a specific municipality for detailed plotting if multiple exist
# For now, assumes one muni or plots all overlaid which might be messy.
# Let's pick the first muni_code if multiple.
PLOTTING_MUNI_CODE <- unique(combined_results_df$muni_code)[1]
message(paste0("   Generating plots for muni_code: ", PLOTTING_MUNI_CODE))

plot_df <- combined_results_df %>% filter(muni_code == PLOTTING_MUNI_CODE)
pop_total_est_plot_df <- prepared_data_for_interp$pop_total_est %>% filter(muni_code == PLOTTING_MUNI_CODE)
census_shares_plot_df <- prepared_data_for_interp$census_shares %>% filter(muni_code == PLOTTING_MUNI_CODE)


# A. Plot: Sum of interpolated granular vs. pop_total_est
plot_sum_vs_total <- ggplot() +
  geom_line(data = validation_summary %>% filter(muni_code == PLOTTING_MUNI_CODE), 
            aes(x = ano, y = sum_granular_interpolated, color = method, linetype = "Interpolated Sum"), size=1) +
  geom_line(data = pop_total_est_plot_df, 
            aes(x = ano, y = pop_total_est, linetype = "Authoritative pop_total_est"), color = "black", size=1.2, alpha=0.7) +
  labs(title = paste("Sum of Interpolated Granular Pop vs. Authoritative Total\nMuni:", PLOTTING_MUNI_CODE),
       x = "Year", y = "Population", color = "Interpolation Method", linetype = "Series") +
  theme_minimal() +
  scale_linetype_manual(values = c("Interpolated Sum" = "solid", "Authoritative pop_total_est" = "dashed"))
print(plot_sum_vs_total)
ggsave(file.path(OUTPUT_DIR, "plot_sum_vs_total.png"), plot_sum_vs_total, width=10, height=6)


# B. Plot: Time series for a few selected granular groups
example_groups <- plot_df %>%
  distinct(gender, race, age_band) %>%
  sample_n(min(4, nrow(.)), replace = FALSE) # Sample up to 4 groups for plotting

message("   Plotting time series for example groups: ")
print(example_groups)

for(i in 1:nrow(example_groups)) {
  eg <- example_groups[i,]
  
  # Original census points for this specific group
  original_pts <- census_shares_plot_df %>% 
    filter(gender == eg$gender, race == eg$race, age_band == eg$age_band) %>%
    select(ano, pop_census_val)
  
  p_group <- ggplot(plot_df %>% filter(gender == eg$gender, race == eg$race, age_band == eg$age_band), 
                    aes(x = ano, y = pop, color = method)) +
    geom_line(alpha=0.8, size=1) +
    geom_point(data = original_pts, aes(x=ano, y=pop_census_val), color="black", size=3, shape=1) + # Show original census points
    labs(title = paste("Interpolated Population: ", eg$gender, eg$race, eg$age_band, "\nMuni:", PLOTTING_MUNI_CODE),
         x = "Year", y = "Population", color = "Method") +
    theme_minimal()
  print(p_group)
  ggsave(file.path(OUTPUT_DIR, paste0("plot_group_", eg$gender, "_", eg$race, "_", eg$age_band, ".png")), p_group, width=10, height=6)
}

# C. Plot: Interpolated Shares for example groups
for(i in 1:nrow(example_groups)) {
  eg <- example_groups[i,]
  original_share_pts <- census_shares_plot_df %>% 
    filter(gender == eg$gender, race == eg$race, age_band == eg$age_band) %>%
    select(ano, share)
  
  p_share <- ggplot(plot_df %>% filter(gender == eg$gender, race == eg$race, age_band == eg$age_band), 
                    aes(x = ano, y = interpolated_share, color = method)) +
    geom_line(alpha=0.8, size=1) +
    geom_point(data = original_share_pts, aes(x=ano, y=share), color="black", size=3, shape=1) +
    labs(title = paste("Interpolated Share: ", eg$gender, eg$race, eg$age_band, "\nMuni:", PLOTTING_MUNI_CODE),
         x = "Year", y = "Share of Total Population", color = "Method") +
    ylim(0, NA) + # Ensure y-axis starts at 0 for shares
    theme_minimal()
  print(p_share)
  ggsave(file.path(OUTPUT_DIR, paste0("plot_share_", eg$gender, "_", eg$race, "_", eg$age_band, ".png")), p_share, width=10, height=6)
}


# D. Plot: Race/Gender Compositions for a sample non-census year
SAMPLE_NON_CENSUS_YEAR <- floor(mean(ALL_TARGET_YEARS[!ALL_TARGET_YEARS %in% CENSUS_YEARS]))
if(is.na(SAMPLE_NON_CENSUS_YEAR) & length(ALL_TARGET_YEARS[!ALL_TARGET_YEARS %in% CENSUS_YEARS]) > 0) {
  SAMPLE_NON_CENSUS_YEAR <- ALL_TARGET_YEARS[!ALL_TARGET_YEARS %in% CENSUS_YEARS][1]
}


if(!is.na(SAMPLE_NON_CENSUS_YEAR)){
  message(paste0("   Generating composition plots for example non-census year: ", SAMPLE_NON_CENSUS_YEAR))
  
  composition_df_sample_year <- plot_df %>% filter(ano == SAMPLE_NON_CENSUS_YEAR)
  
  # Race composition
  race_comp <- composition_df_sample_year %>%
    group_by(method, race) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")
  
  p_race_comp <- ggplot(race_comp, aes(x = method, y = pop, fill = race)) +
    geom_bar(stat = "identity", position = "fill") + # position="fill" for proportions
    labs(title = paste("Race Composition by Method, Year:", SAMPLE_NON_CENSUS_YEAR, "\nMuni:", PLOTTING_MUNI_CODE),
         x = "Interpolation Method", y = "Proportion of Population", fill = "Race") +
    theme_minimal() +  
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  print(p_race_comp)
  ggsave(file.path(OUTPUT_DIR, paste0("plot_race_comp_year", SAMPLE_NON_CENSUS_YEAR, ".png")), p_race_comp, width=10, height=7)
  
  # Gender composition
  gender_comp <- composition_df_sample_year %>%
    group_by(method, gender) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")
  
  p_gender_comp <- ggplot(gender_comp, aes(x = method, y = pop, fill = gender)) +
    geom_bar(stat = "identity", position = "fill") +
    labs(title = paste("Gender Composition by Method, Year:", SAMPLE_NON_CENSUS_YEAR, "\nMuni:", PLOTTING_MUNI_CODE),
         x = "Interpolation Method", y = "Proportion of Population", fill = "Gender") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  print(p_gender_comp)
  ggsave(file.path(OUTPUT_DIR, paste0("plot_gender_comp_year", SAMPLE_NON_CENSUS_YEAR, ".png")), p_gender_comp, width=10, height=7)
} else {
  message("   Skipping composition plots as no non-census year available for sampling.")
}


message("--- Interpolation Testing Script Finished ---")
message("--- Check the '", OUTPUT_DIR, "' directory for plots. ---")












library(dplyr)
library(tidyr)
library(ggplot2)

# 1) parameters
years   <- c(2000, 2010, 2022)
ages    <- c("00_04","05_09","10_14","15_19","20_24",
             "25_29","30_39","40_49","50_59","60_69",
             "70_79","80p")
genders <- c("M","W")
races   <- c("Blk","Brn","Ind","Wht","Ylw")

# 2) grab exactly the 5×2×12 granular cols
pattern <- paste0("^pop_(", paste(genders, collapse="|"), ")_(",
                  paste(races, collapse="|"), ")_(",
                  "(?:[0-9]{2}_[0-9]{2})|80p)$")
granular_cols <- grep(pattern, names(demographic_basis_final), value = TRUE)

# 3) pivot longer into one table
long_df <- demographic_basis_final %>%
  filter(ano %in% years) %>%
  select(muni_code, muni_name, uf_abbr, ano, all_of(granular_cols)) %>%
  pivot_longer(
    cols        = all_of(granular_cols),
    names_to    = c("gender","race","age"),
    names_pattern = "^pop_([MW])_([A-Za-z]+)_((?:[0-9]{2}_[0-9]{2})|80p)$",
    values_to   = "pop"
  )

# 4) compute each of the four sums per muni–year
total_df <- long_df %>%
  group_by(muni_code, muni_name, uf_abbr, ano) %>%
  summarise(
    sum_granular = sum(pop, na.rm = TRUE),
    .groups = "drop"
  )

race_sum_df <- long_df %>%
  group_by(muni_code, muni_name, uf_abbr, ano, race) %>%
  summarise(race_pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
  group_by(muni_code, muni_name, uf_abbr, ano) %>%
  summarise(sum_by_race = sum(race_pop), .groups = "drop")

gender_sum_df <- long_df %>%
  group_by(muni_code, muni_name, uf_abbr, ano, gender) %>%
  summarise(gender_pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
  group_by(muni_code, muni_name, uf_abbr, ano) %>%
  summarise(sum_by_gender = sum(gender_pop), .groups = "drop")

age_sum_df <- long_df %>%
  group_by(muni_code, muni_name, uf_abbr, ano, age) %>%
  summarise(age_pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
  group_by(muni_code, muni_name, uf_abbr, ano) %>%
  summarise(sum_by_age = sum(age_pop), .groups = "drop")

# 5) assemble all sums with the official total, compute discrepancies
report_df <- total_df %>%
  left_join(race_sum_df,   by = c("muni_code","muni_name","uf_abbr","ano")) %>%
  left_join(gender_sum_df, by = c("muni_code","muni_name","uf_abbr","ano")) %>%
  left_join(age_sum_df,    by = c("muni_code","muni_name","uf_abbr","ano")) %>%
  left_join(
    demographic_basis_final %>%
      filter(ano %in% years) %>%
      select(muni_code, muni_name, uf_abbr, ano, pop_total_est),
    by = c("muni_code","muni_name","uf_abbr","ano")
  ) %>%
  mutate(
    disc_granular = sum_granular - pop_total_est,
    disc_race     = sum_by_race  - pop_total_est,
    disc_gender   = sum_by_gender- pop_total_est,
    disc_age      = sum_by_age   - pop_total_est
  ) %>%
  select(
    muni_code, muni_name, uf_abbr, ano,
    sum_granular, sum_by_race, sum_by_gender, sum_by_age,
    pop_total_est,
    disc_granular, disc_race, disc_gender, disc_age
  )

# 6) print full report
print(report_df)

# 7) print only rows where ANY discrepancy ≠ 0
report_df %>%
  filter(
    disc_granular != 0 |
      disc_race     != 0 |
      disc_gender   != 0 |
      disc_age      != 0
  ) %>%
  print(n = Inf)

# 8) plot racial and gender compositions for sanity check
#    (race_pop & gender_pop from earlier dfs)

race_sums <- long_df %>%
  group_by(ano, race) %>%
  summarise(race_pop = sum(pop, na.rm = TRUE), .groups = "drop")

gender_sums <- long_df %>%
  group_by(ano, gender) %>%
  summarise(gender_pop = sum(pop, na.rm = TRUE), .groups = "drop")

p_race <- ggplot(race_sums, aes(race, race_pop, fill = race)) +
  geom_col() +
  facet_wrap(~ ano) +
  labs(title = "Race Composition by Census Year",
       x = "Race", y = "Population") +
  theme_minimal() + theme(legend.position="none")

p_gender <- ggplot(gender_sums, aes(gender, gender_pop, fill = gender)) +
  geom_col() +
  facet_wrap(~ ano) +
  labs(title = "Gender Composition by Census Year",
       x = "Gender", y = "Population") +
  theme_minimal() + theme(legend.position="none")

print(p_race)
print(p_gender)


# afsfgalwkgbgnalkgbaehgkbjahd (testing git again)
