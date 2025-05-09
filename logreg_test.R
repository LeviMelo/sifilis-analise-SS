# --- Forest Plot Manual REFINED (broom.helpers focus) ---
cat("5.4 Gráfico — Forest Plot dos Resultados da Regressão (broom.helpers focus)...\n")
plot_forest <- NULL

tryCatch({
  if (is.null(modelo_final_glm)) stop("Objeto 'modelo_final_glm' é NULL.")
  
  # 1. extract tidy data
  tidy_data <- broom.helpers::tidy_plus_plus(
    model              = modelo_final_glm,
    exponentiate       = TRUE,
    conf.int           = TRUE,
    intercept          = FALSE,
    add_reference_rows = TRUE,
    variable_labels    = var_labels_vector
  )
  
  # 2. prep for plotting
  plot_df <- tidy_data %>%
    mutate(
      var_label = factor(var_label, levels = unname(var_labels_vector)),
      significance_group = if_else(p.value < 0.05, "p < 0,05", "p ≥ 0,05")
    ) %>%
    group_by(var_label) %>%
    arrange(var_label, desc(reference_row), label) %>%
    ungroup() %>%
    mutate(
      y_axis_level = fct_inorder(paste(var_label, label, sep = "_"))
    )
  
  # 3. dynamic x breaks
  min_ci <- min(c(0.05, plot_df$conf.low), na.rm = TRUE)
  max_ci <- max(c(8,    plot_df$conf.high), na.rm = TRUE)
  if (min_ci <= 0)           min_ci <- 0.05
  if (max_ci <= min_ci)      max_ci <- max(8, min_ci * 2)
  breaks <- c(0.1,0.25,0.5,1,2,4,8,16)
  x_breaks <- breaks[breaks >= min_ci*0.8 & breaks <= max_ci*1.2]
  if (length(x_breaks) < 3)  x_breaks <- c(0.25,0.5,1,2,4)
  
  # 4. build plot
  plot_forest <- ggplot(plot_df, aes(x = estimate, y = fct_rev(y_axis_level))) +
    geom_vline(xintercept = 1, linetype="dashed", color="grey70", linewidth=0.6) +
    geom_errorbarh(aes(xmin = conf.low, xmax = conf.high, color = significance_group),
                   height=0.2, linewidth=0.7, na.rm=TRUE) +
    geom_point(aes(fill = significance_group, shape = reference_row),
               size=2.8, color="black", stroke=0.4) +
    # … up through the x‐axis …
    scale_x_log10(
      breaks = x_breaks,
      limits = c(min_ci*0.8, max_ci*1.2),
      labels = label_number(accuracy=0.1, decimal.mark=",")
    ) +
    
    # move factor‐level labels to the right
    scale_y_discrete(
      position = "right",
      labels = function(id) sapply(strsplit(id, "_"), `[`, 2)
    ) +
    
    # — replace these three scales with the cleaner, split legends —
    
    # p‐value groups in color
    scale_color_manual(
      name         = "Significância",
      values       = c("p < 0,05" = cores_principais[1],
                       "p ≥ 0,05" = "grey30"),
      labels       = c("p < 0,05", "p ≥ 0,05"),
      na.translate = FALSE
    ) +
    
    # same fills but drop the fill legend entirely
    scale_fill_manual(
      values = c("p < 0,05" = cores_principais[1],
                 "p ≥ 0,05" = "white"),
      guide  = "none"
    ) +
    
    # reference vs. comparison in shape
    scale_shape_manual(
      name         = "Tipo",
      values       = c(`FALSE` = 21,  # comparison
                       `TRUE`  = 22), # reference
      labels       = c("Comparação", "Referência"),
      na.translate = FALSE
    ) +
    
    # force “Tipo” first, then “Significância”
    guides(
      shape = guide_legend(order = 1),
      color = guide_legend(order = 2)
    ) +
    
    # … then your facet + labs + theme as before …
    facet_grid(
      var_label ~ ., 
      scales = "free_y", 
      space  = "free_y", 
      switch = "y"
    ) +
    labs(
      title    = "Fatores Associados ao Tratamento Inadequado ou Não Tratado",
      subtitle = sprintf(
        "%s, %d–%d  |  Desfecho: Inadequado/Não Tratado vs. Adequado",
        UF_ESTUDO, ANO_INICIO, ANO_FIM
      ),
      x       = "Odds Ratio Ajustada (Escala Logarítmica)",
      y       = NULL,
      caption = "OR < 1 favorece tratamento adequado; OR > 1 favorece tratamento inadequado/não tratamento."
    ) +
    theme_minimal(base_size = 11) +
    theme(
      strip.placement    = "outside",
      strip.text.y.left  = element_text(angle=0, face="bold", hjust=1),
      panel.grid.major.y = element_blank(),
      panel.grid.minor.x = element_blank(),
      axis.title.x       = element_text(face="bold", margin=margin(t=8)),
      axis.text.y.right  = element_text(hjust=0, size=rel(0.9)),
      # ensure bold legend titles and proper ordering
      legend.position    = "top",
      legend.box         = "horizontal",
      legend.title       = element_text(face="bold"),
      legend.text        = element_text(size=rel(0.85)),
      plot.title         = element_text(hjust=0.5, face="bold"),
      plot.subtitle      = element_text(hjust=0.5, margin=margin(b=6)),
      plot.caption       = element_text(hjust=0.5, face="italic",
                                        size=rel(0.8), margin=margin(t=6))
    )
  
  print(plot_forest)
  salvar_grafico_final(
    plot_forest,
    "plot_forest_trat_inadequado_broomhelpers",
    width = 10, height = 12
  )
  
}, error = function(e) {
  cat("!! ERRO ao gerar Forest Plot !!\n", e$message, "\n")
  if (exists("tidy_data")) cat("--- tidy_data structure ---\n"); str(tidy_data)
  if (exists("plot_df"   )) cat("--- plot_df  structure ---\n"); str(plot_df)
})
cat("\n")
