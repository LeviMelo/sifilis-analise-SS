# --- ANÁLISE EPIDEMIOLÓGICA DA SÍFILIS GESTACIONAL EM ALAGOAS (2010-2022) ---
# Autor: Levi de Melo Amorim (FAMED/UFAL)
# Data: `r format(Sys.Date(), '%d/%m/%Y')`
# Formato: Script R puro com saída para console e arquivos

# --- 1. CONFIGURAÇÃO INICIAL ---

# --- 1.1. Bibliotecas Necessárias ---
# Certifique-se de que todas estão instaladas! Use install.packages("nome_pacote")
cat("Carregando bibliotecas...\n")
suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(scales)
  library(gtsummary)
  library(finalfit) 
  library(openxlsx)
  library(here)
  library(sf)
  library(geobr) 
  library(patchwork)
  library(viridisLite) 
  library(broom.helpers) 
})
cat("Bibliotecas carregadas.\n\n")

# --- 1.2. Configurações Globais ---
# Locale para Português (datas, decimais)
tryCatch({
  Sys.setlocale("LC_TIME", "pt_BR.UTF-8")
  Sys.setlocale("LC_ALL", "pt_BR.UTF-8") 
  cat("Locale definido para pt_BR.UTF-8\n")
}, error = function(e) {
  message("Aviso: Locale pt_BR.UTF-8 não disponível. Usando locale padrão do sistema.")
})

# Definir tema padrão do gtsummary para melhor aparência e formatação PT-BR
theme_gtsummary_language("pt", decimal.mark = ",", big.mark = ".") 
theme_gtsummary_compact() 

# Diretório de saída para relatórios
dir_relatorio <- here::here("relatorio")
if (!dir.exists(dir_relatorio)) {
  dir.create(dir_relatorio, recursive = TRUE)
  cat("Diretório de saída criado em:", dir_relatorio, "\n")
} else {
  cat("Diretório de saída:", dir_relatorio, "\n")
}

# --- 1.3. Tema Padrão para Gráficos ggplot2 ---
theme_set(
  theme_minimal(base_size = 11) + 
    theme(
      plot.title = element_text(hjust = 0.5, face = "bold", size = rel(1.2)),
      plot.subtitle = element_text(hjust = 0.5, size = rel(1.0), margin = margin(b=10)),
      axis.title = element_text(face = "bold", size = rel(1.0)),
      axis.text = element_text(size = rel(0.9)),
      legend.title = element_text(face = "bold"),
      legend.position = "bottom",
      strip.text = element_text(face = "bold", size = rel(1.0)),
      panel.grid.minor = element_blank(),
      plot.caption = element_text(hjust = 0, face = "italic", size=rel(0.8))
    )
)
options(ggplot2.discrete.fill = viridisLite::viridis,
        ggplot2.discrete.colour = viridisLite::viridis)
cores_principais <- viridisLite::viridis(6) 

# --- 1.4. Funções Auxiliares ---
salvar_grafico_final <- function(plot_obj, nome_arquivo_base, width = 8, height = 6, dpi = 200) {
  tryCatch({
    dest_path_png <- here::here(dir_relatorio, paste0(nome_arquivo_base, ".png"))
    ggsave(filename = dest_path_png, 
           plot = plot_obj, width = width, height = height, dpi = dpi, bg = "white")
    cat("-> Gráfico salvo:", dest_path_png, "\n")
  }, error = function(e){
    cat("!! Erro ao salvar gráfico", nome_arquivo_base, ":", e$message, "\n")
  })
}

salvar_tabela_xlsx <- function(tabela_df, nome_arquivo_base, nome_planilha = "Dados") {
  tryCatch({
    wb <- createWorkbook()
    addWorksheet(wb, nome_planilha)
    
    if (inherits(tabela_df, "gtsummary")) {
      df_para_salvar <- gtsummary::as_tibble(tabela_df, col_labels = TRUE) 
      df_para_salvar <- df_para_salvar %>% mutate(across(everything(), ~str_replace_all(., "\\*\\*", "")))
    } else if (!is.data.frame(tabela_df)) {
      df_para_salvar <- as.data.frame(tabela_df)
    } else {
      df_para_salvar <- tabela_df
    }
    
    writeData(wb, sheet = nome_planilha, x = df_para_salvar, startCol = 1, startRow = 1)
    dest_path_xlsx <- here::here(dir_relatorio, paste0(nome_arquivo_base, ".xlsx"))
    saveWorkbook(wb, file = dest_path_xlsx, overwrite = TRUE)
    cat("-> Tabela salva:", dest_path_xlsx, "\n")
  }, error = function(e){
    cat("!! Erro ao salvar tabela", nome_arquivo_base, ":", e$message, "\n")
  })
}

# --- 1.5. Carregamento e Preparação dos Dados ---
cat("\nCarregando dados processados...\n")
data_file <- "sifg_alagoas_2010_2022_processed.RData" # Defina o nome do seu arquivo .RData
if (!file.exists(data_file)) {
  stop("ERRO CRÍTICO: Arquivo de dados '", data_file, "' não encontrado no diretório de trabalho (", getwd(), "). Execute o script de processamento primeiro.")
}
load(data_file) 
if (!exists("sifg_translated_data") || !is.data.frame(sifg_translated_data) || nrow(sifg_translated_data) == 0) {
  stop("ERRO CRÍTICO: Dataframe 'sifg_translated_data' não encontrado ou vazio após carregar '", data_file, "'.")
}
cat("Dados carregados com sucesso.\n")

df_analise <- sifg_translated_data 
df_analise$ANO_SGB_fct <- factor(df_analise$ANO_SGB)

# Corrigir e formatar variáveis
cat("Formatando variáveis para análise...\n")
df_analise <- df_analise %>%
  mutate(
    DSTITULO1_numeric = suppressWarnings(as.numeric(as.character(DSTITULO1))),
    DSTITULO1_numeric = ifelse(is.finite(DSTITULO1_numeric), DSTITULO1_numeric, NA),
    CS_RACA = factor(CS_RACA, levels = c("1-Branca", "2-Preta", "3-Amarela", "4-Parda", "5-Indígena", "9-Ignorado")),
    CS_ESCOL_N = factor(CS_ESCOL_N, levels = c("0-Analfabeto", "1-EF Incompleto(1-4)", "2-EF Completo(4)", 
                                               "3-EF Incompleto(5-8)", "4-EF Completo", "5-EM Incompleto", 
                                               "6-EM Completo", "7-Superior Incompleto", "8-Superior Completo", 
                                               "9-Ignorado", "10-Não se aplica")),
    CS_GESTANT = factor(CS_GESTANT, levels = c("1-1ºTrimestre", "2-2ºTrimestre", "3-3ºTrimestre", 
                                               "4-Idade gestacional ignorada", "9-Ignorado")),
    TPEVIDENCI = factor(TPEVIDENCI, levels = c("1-Primária", "2-Secundária", "3-Terciária", "4-Latente", "9-Ignorado")),
    TPESQUEMA = factor(TPESQUEMA, levels = c("1-Pen G benz 2.400.000UI", "2-Pen G benz 4.800.000UI",
                                             "3-Pen G benz 7.200.000UI", "4-Outro esquema", 
                                             "5-Não realizado", "9-Ignorado")),
    TRATPARC = factor(TRATPARC, levels = c("1-Sim", "2-Não", "9-Ignorado"))
  )

# Definir constantes
ANO_INICIO <- min(df_analise$ANO_SGB, na.rm = TRUE)
ANO_FIM <- max(df_analise$ANO_SGB, na.rm = TRUE)
UF_ESTUDO <- unique(df_analise$NOME_UF_NOT)[1] 
UF_CODIGO <- unique(df_analise$SG_UF_NOT)[1]
TOTAL_CASOS <- nrow(df_analise)

# Labels PT-BR
labels_pt <- list(
  AGE_YEARS ~ "Idade Materna (anos)",
  CS_RACA ~ "Raça/Cor Autodeclarada",
  CS_ESCOL_N ~ "Nível de Escolaridade",
  CS_GESTANT ~ "Idade Gestacional no Diagnóstico",
  TPEVIDENCI ~ "Classificação Clínica da Sífilis",
  TPTESTE1 ~ "Teste Não Treponêmico (VDRL/RPR)",
  TPCONFIRMA ~ "Teste Confirmatório",
  TPESQUEMA ~ "Esquema de Tratamento (Gestante)",
  TRATPARC ~ "Parceiro Tratado Concomitantemente",
  NOME_MUNICIP_RESI ~ "Município de Residência",
  TRAT_GESTANTE_ADEQUADO ~ "Adequação do Tratamento (Gestante)",
  # Labels para variáveis categorizadas na regressão
  `Faixa Etária (anos)` ~ "Faixa Etária (anos)",
  `Raça/Cor` ~ "Raça/Cor",
  `Escolaridade` ~ "Escolaridade",
  `Classificação Clínica` ~ "Classificação Clínica",
  `Idade Gestacional (Diagnóstico)` ~ "Idade Gestacional (Diagnóstico)",
  `Ano da Notificação` ~ "Ano da Notificação"
)
cat("Setup inicial concluído.\n")

# --- 2. ANÁLISE DESCRITIVA ---
cat("\n\n--- INICIANDO ANÁLISE DESCRITIVA ---\n\n")

# --- 2.1. Perfil Geral e Qualidade dos Dados ---
cat("--- 2.1. Perfil Geral e Qualidade dos Dados ---\n")
cat("Período:", ANO_INICIO, "-", ANO_FIM, "\n")
cat("UF:", UF_ESTUDO, "(Código:", UF_CODIGO, ")\n")
cat("Total de Casos Notificados:", format(TOTAL_CASOS, big.mark = "."), "\n")

cat("\nCalculando dados ausentes...\n")
tabela_missing_full <- df_analise %>%
  summarise(across(everything(), ~sum(is.na(.)))) %>%
  pivot_longer(everything(), names_to = "Variável", values_to = "Nº Ausentes") %>%
  mutate(`% Ausente` = (`Nº Ausentes` / TOTAL_CASOS)) %>%
  filter(`Nº Ausentes` > 0) %>%
  arrange(desc(`% Ausente`))

tabela_missing_top15 <- head(tabela_missing_full, 15) %>% 
  mutate(`% Ausente Formatado` = scales::percent(`% Ausente`, accuracy = 0.1, decimal.mark = ","))

cat("\n--- Tabela: 15 Variáveis com Maior % de Dados Ausentes ---\n")
print(tabela_missing_top15 %>% select(Variável, `Nº Ausentes`, `% Ausente Formatado`), n=15)
cat("-----------------------------------------------------------\n")
salvar_tabela_xlsx(tabela_missing_full %>% mutate(`% Ausente` = round(`% Ausente`*100, 1)), 
                   "tabela_dados_ausentes_completa")

# --- 2.2. Distribuição por Características Pessoais ---
cat("\n--- 2.2. Distribuição por Características Pessoais ---\n")

cat("\n--- Tabela: Características Sociodemográficas e Gestacionais ---\n")
tbl_caracteristicas <- NULL
tryCatch({
  colunas_desejadas <- c("AGE_YEARS", "CS_RACA", "CS_ESCOL_N", "CS_GESTANT", "TPEVIDENCI")
  colunas_existentes <- colunas_desejadas[colunas_desejadas %in% names(df_analise)]
  if (length(colunas_existentes) == 0) stop("Nenhuma coluna válida selecionada.")
  
  tbl_caracteristicas <- df_analise %>%
    select(all_of(colunas_existentes)) %>% 
    tbl_summary(
      label = labels_pt, 
      type = list(AGE_YEARS ~ "continuous2"), 
      statistic = list(all_continuous() ~ c("{mean} ({sd})", "{median} ({p25}, {p75})", "{min}, {max}"),
                       all_categorical() ~ "{n} ({p}%)"),
      digits = list(all_continuous() ~ 1, all_categorical(stat = "p") ~ 1), 
      missing_text = "Ignorado/Ausente"
    ) %>%
    bold_labels() %>%
    modify_table_styling(columns = label, footnote = paste0("N = ", TOTAL_CASOS))
  
  # Imprimir como tibble para melhor visualização no console
  print(tbl_caracteristicas %>% as_tibble(col_labels = TRUE)) 
  cat("--------------------------------------------------------------\n")
  salvar_tabela_xlsx(tbl_caracteristicas, "tabela_caracteristicas_pessoais_clinicas")
  
}, error = function(e) {
  cat("!! ERRO ao gerar Tabela de Características Pessoais !!\n", e$message, "\n")
})

# --- Gráfico: Idade Materna ---
cat("\nGerando gráfico: Distribuição da Idade Materna...\n")
tryCatch({
  plot_idade <- ggplot(data = df_analise, mapping = aes(x = AGE_YEARS)) +
    geom_histogram(mapping = aes(y = after_stat(density)), binwidth = 1, 
                   fill = "#0072B2", color = "white", alpha = 0.8) +
    geom_density(color = "#D55E00", linewidth = 1.1) +
    geom_vline(xintercept = mean(df_analise$AGE_YEARS, na.rm = TRUE), 
               linetype = "dashed", color = "red", linewidth = 0.8) +
    geom_text(mapping = aes(x = mean(df_analise$AGE_YEARS, na.rm=TRUE) * 1.05, y = 0.065, 
                            label = paste("Média =", sprintf("%.1f", mean(AGE_YEARS, na.rm = TRUE)))), 
              color = "red", size = 3.5, hjust = 0) +
    labs(title = "Distribuição da Idade Materna",
         subtitle = paste("Alagoas,", ANO_INICIO, "-", ANO_FIM, "(N =", TOTAL_CASOS, ")"),
         x = "Idade Materna (anos)", y = "Densidade") +
    scale_x_continuous(breaks = seq(10, 50, by = 5), limits = c(min(df_analise$AGE_YEARS, na.rm=T)-1, max(df_analise$AGE_YEARS, na.rm=T)+1) ) + 
    scale_y_continuous(labels = label_percent(accuracy = 1, decimal.mark = ",")) 
  
  print(plot_idade) 
  salvar_grafico_final(plot_idade, "plot_distribuicao_idade_materna")
  
}, error = function(e) {
  cat("!! ERRO ao gerar Gráfico de Idade Materna !!\n", e$message, "\n")
})

# --- Gráfico: Idade Materna por Período ---
cat("\nGerando gráfico: Distribuição da Idade Materna por Período...\n")
tryCatch({
  df_analise_periodo <- df_analise %>%
    mutate(
      Periodo = case_when(
        ANO_SGB %in% 2010:2014 ~ "2010-2014",
        ANO_SGB %in% 2015:2018 ~ "2015-2018",
        ANO_SGB %in% 2019:2022 ~ "2019-2022",
        TRUE ~ "Outro" ),
      Periodo = factor(Periodo, levels = c("2010-2014", "2015-2018", "2019-2022")) ) %>%
    filter(!is.na(Periodo), !is.na(AGE_YEARS), Periodo != "Outro") 
  
  idade_stats_por_periodo <- df_analise_periodo %>%
    group_by(Periodo) %>%
    summarise( N = n(), Media = mean(AGE_YEARS, na.rm = TRUE), DP = sd(AGE_YEARS, na.rm = TRUE),
               Mediana = median(AGE_YEARS, na.rm = TRUE), Q1 = quantile(AGE_YEARS, 0.25, na.rm = TRUE),
               Q3 = quantile(AGE_YEARS, 0.75, na.rm = TRUE), Min = min(AGE_YEARS, na.rm = TRUE),
               Max = max(AGE_YEARS, na.rm = TRUE) ) %>%
    mutate(across(where(is.numeric) & !c(Periodo, N), ~round(., 1)))
  
  cat("\n--- Tabela: Estatísticas de Idade por Período ---\n")
  print(idade_stats_por_periodo)
  cat("--------------------------------------------------\n")
  salvar_tabela_xlsx(idade_stats_por_periodo, "tabela_idade_stats_por_periodo")
  
  if (nrow(df_analise_periodo) > 0) {
    plot_idade_periodo <- ggplot(df_analise_periodo, aes(x = Periodo, y = AGE_YEARS)) +
      geom_violin(mapping = aes(fill = Periodo), color = NA, alpha = 0.7, trim = TRUE, 
                  scale = "width", draw_quantiles = NULL) +                
      geom_boxplot(width = 0.15, fill = "white", color = "grey20", outlier.shape = 21, 
                   outlier.size = 1.5, outlier.fill = "grey80", alpha = 0.9, coef = 1.5) + 
      stat_summary(fun = median, geom = "point", shape = 23, size = 3.5, 
                   fill = "firebrick", color = "white", stroke = 1) + 
      scale_fill_viridis_d(option = "plasma") + 
      scale_y_continuous(breaks = seq(10, 50, by = 5), limits = c(5, 55)) + 
      labs(title = "Tendência da Idade Materna por Período de Notificação",
           subtitle = "Distribuição (violino), IQR (caixa) e Mediana (losango)",
           x = "Período da Notificação", y = "Idade Materna (anos)") +
      theme(legend.position = "none") 
    
    print(plot_idade_periodo)
    salvar_grafico_final(plot_idade_periodo, "plot_distribuicao_idade_por_periodo", width=9, height=6)
  } else { cat("Não há dados para gerar gráfico de idade por período.\n") }
  
}, error = function(e) {
  cat("!! ERRO ao gerar Gráfico de Idade por Período !!\n", e$message, "\n")
})

# --- Gráficos: Raça/Cor e Escolaridade (Barras Horizontais) ---
cat("\nGerando gráficos: Raça/Cor e Escolaridade...\n")

# Dados Raça/Cor
plot_raca_data <- df_analise %>%
  mutate(CS_RACA = fct_explicit_na(CS_RACA, na_level = "9-Ignorado")) %>% 
  count(CS_RACA) %>% filter(!is.na(CS_RACA)) %>%
  mutate( N_Total = sum(n), Porcentagem = n / N_Total,
          CS_RACA_ORD = fct_reorder(CS_RACA, n, .desc = TRUE), 
          LabelNoPrefix = gsub("^.-", "", CS_RACA) ) %>%
  arrange(desc(n))
cat("\n--- Dados para Gráfico: Raça/Cor ---\n")
print(plot_raca_data %>% select(Raça_Cor=CS_RACA, Contagem=n, Porcentagem=round(Porcentagem*100,1)), n=Inf)
cat("------------------------------------\n")
salvar_tabela_xlsx(plot_raca_data %>% select(CS_RACA, LabelNoPrefix, n, Porcentagem), "dados_plot_raca_final")

# Gráfico Raça/Cor
tryCatch({
  plot_raca <- ggplot(plot_raca_data, aes(x = n, y = fct_rev(LabelNoPrefix), fill = CS_RACA_ORD)) +
    geom_col(show.legend = FALSE, alpha = 0.9) +
    geom_text(aes(label = paste0(n, " (", scales::percent(Porcentagem, accuracy = 0.1, decimal.mark = ","), ")")), 
              hjust = -0.05, size = 3.0, fontface = "bold", color = "black") +
    scale_fill_viridis_d(guide = "none") +
    scale_x_continuous(expand = expansion(mult = c(0.01, 0.18))) +
    labs(title = "Raça/Cor Autodeclarada", x = "Número de Casos", y = NULL) +
    theme(panel.grid.major.x = element_line(color="grey90", linetype = "dashed"), 
          panel.grid.major.y = element_blank())
  print(plot_raca)
  salvar_grafico_final(plot_raca, "plot_raca_barras", width=8, height=5)
}, error = function(e){ cat("!! ERRO ao gerar Gráfico de Raça/Cor !!\n", e$message, "\n") })


# Dados Escolaridade
niveis_escolaridade_ordem <- c( "0-Analfabeto", "1-EF Incompleto(1-4)", "2-EF Completo(4)", 
                                "3-EF Incompleto(5-8)", "4-EF Completo", "5-EM Incompleto", "6-EM Completo", 
                                "7-Superior Incompleto", "8-Superior Completo", "9-Ignorado")
plot_escol_data <- df_analise %>%
  mutate(CS_ESCOL_N = ifelse(is.na(CS_ESCOL_N), "9-Ignorado", as.character(CS_ESCOL_N))) %>%
  filter(CS_ESCOL_N != "10-Não se aplica") %>%
  mutate(CS_ESCOL_N_ORD = factor(CS_ESCOL_N, levels = niveis_escolaridade_ordem, ordered = TRUE)) %>%
  count(CS_ESCOL_N_ORD, .drop = FALSE) %>% filter(!is.na(CS_ESCOL_N_ORD)) %>%
  mutate( N_Total = sum(n), Porcentagem = n / N_Total,
          EscolaridadeLabel = gsub("^.{1,2}-", "", CS_ESCOL_N_ORD),
          LabelPos = paste0(n, " (", scales::percent(Porcentagem, accuracy = 0.1, decimal.mark = ","), ")") ) %>%
  arrange(CS_ESCOL_N_ORD)
cat("\n--- Dados para Gráfico: Escolaridade ---\n")
print(plot_escol_data %>% select(Escolaridade_Ord=CS_ESCOL_N_ORD, Label_Eixo=EscolaridadeLabel, Contagem=n, Porcentagem=round(Porcentagem*100,1)), n=Inf)
cat("---------------------------------------\n")
salvar_tabela_xlsx(plot_escol_data %>% select(CS_ESCOL_N_ORD, EscolaridadeLabel, n, Porcentagem), "dados_plot_escolaridade_final")

# Gráfico Escolaridade
tryCatch({
  plot_escolaridade <- ggplot(plot_escol_data, aes(x = n, y = fct_rev(EscolaridadeLabel), fill = Porcentagem)) + 
    geom_col(show.legend = TRUE, alpha = 0.9) +
    geom_text(aes(label = LabelPos), hjust = -0.05, size = 3.0, fontface = "bold", color = "black") +
    scale_fill_viridis_c(option = "cividis", name = "% Casos", labels = label_percent(decimal.mark = ",")) + 
    scale_x_continuous(expand = expansion(mult = c(0.01, 0.18))) + 
    labs(title = "Nível de Escolaridade", x = "Número de Casos", y = NULL) +
    theme(legend.position = c(0.85, 0.25), legend.key.height = unit(0.8, 'cm'),
          panel.grid.major.x = element_line(color="grey90", linetype = "dashed"),
          panel.grid.major.y = element_blank())
  print(plot_escolaridade)
  salvar_grafico_final(plot_escolaridade, "plot_escolaridade_barras", width=9, height=6)
}, error = function(e){ cat("!! ERRO ao gerar Gráfico de Escolaridade !!\n", e$message, "\n") })


# --- 2.3. Distribuição Temporal ---
cat("\n--- 2.3. Distribuição Temporal ---\n")
cat("\nGerando gráfico: Tendência Temporal dos Casos...\n")
tryCatch({
  casos_por_ano <- df_analise %>% group_by(ANO_SGB) %>% summarise(N_Casos = n(), .groups = 'drop')
  plot_tendencia <- ggplot(casos_por_ano, aes(x = ANO_SGB, y = N_Casos)) +
    geom_line(color = cores_principais[1], linewidth = 1.2) + 
    geom_point(color = cores_principais[1], size = 3, shape=21, fill="white", stroke=1.2) + 
    geom_text(aes(label = N_Casos), vjust = -1.0, size = 3.2, color = "black", fontface="bold") +
    scale_x_continuous(breaks = ANO_INICIO:ANO_FIM) +
    scale_y_continuous(limits = c(0, max(casos_por_ano$N_Casos) * 1.15), labels = label_number(big.mark = ".")) +
    labs(title = "Tendência Temporal dos Casos Notificados de Sífilis Gestacional",
         subtitle = paste("Alagoas,", ANO_INICIO, "-", ANO_FIM),
         x = "Ano da Notificação", y = "Número de Casos") +
    theme(axis.text.x = element_text(angle = 45, hjust = 1, vjust=1))
  print(plot_tendencia)
  salvar_grafico_final(plot_tendencia, "plot_tendencia_temporal_casos")
  salvar_tabela_xlsx(casos_por_ano, "dados_tendencia_temporal_casos")
}, error = function(e){ cat("!! ERRO ao gerar Gráfico de Tendência Temporal !!\n", e$message, "\n") })

# --- 2.4. Distribuição Espacial ---
cat("\n--- 2.4. Distribuição Espacial ---\n")
cat("\nGerando mapa: Casos por Município de Residência...\n")
shape_al <- NULL
tryCatch({
  shape_al <- read_municipality(code_muni = UF_CODIGO, year = 2020) 
}, error = function(e){ message("Aviso: Erro ao baixar shapefile de Alagoas com geobr: ", e$message) })

if (!is.null(shape_al)) {
  tryCatch({
    casos_por_municipio_mapa <- df_analise %>%
      filter(!is.na(ID_MN_RESI)) %>% 
      group_by(code_muni = ID_MN_RESI) %>% 
      summarise(N_Casos = n(), .groups = 'drop')
    
    mapa_dados <- shape_al %>%
      mutate(code_muni = as.character(code_muni)) %>% 
      left_join(casos_por_municipio_mapa, by = "code_muni") %>%
      mutate(N_Casos = ifelse(is.na(N_Casos), 0, N_Casos)) 
    
    plot_mapa_casos <- ggplot() +
      geom_sf(data = mapa_dados, aes(fill = N_Casos), color = "gray70", linewidth = 0.1) +
      scale_fill_viridis_c(option = "plasma", name = "Nº Casos\n(Absoluto)", direction = -1, 
                           labels=label_number(big.mark="."), breaks=pretty_breaks(n=6),
                           guide = guide_colorbar(barwidth = 0.8, barheight = 10)) +
      labs(title = "Número de Casos de Sífilis Gestacional por Município de Residência",
           subtitle = paste("Alagoas,", ANO_INICIO, "-", ANO_FIM, "(Total =", format(sum(mapa_dados$N_Casos), big.mark="."), "casos mapeados)"),
           caption = "Fonte: SINAN/DATASUS. Municípios sem casos em cinza claro.") +
      theme_void(base_size=10) + 
      theme(plot.title = element_text(hjust = 0.5, face="bold"), plot.subtitle = element_text(hjust = 0.5),
            legend.position = c(0.88, 0.35), legend.title = element_text(size=9, face="bold"),
            legend.text = element_text(size=8))
    
    print(plot_mapa_casos)
    salvar_grafico_final(plot_mapa_casos, "mapa_casos_municipio_residencia", width=8, height=7)
    salvar_tabela_xlsx(mapa_dados %>% sf::st_drop_geometry() %>% select(code_muni, name_muni, N_Casos) %>% arrange(desc(N_Casos)), 
                       "dados_mapa_casos_municipio_residencia")
  }, error = function(e){ cat("!! ERRO ao gerar Mapa de Casos !!\n", e$message, "\n") })
} else {
  cat("-> Mapa não gerado: shapefile de Alagoas não carregado.\n")
}

# --- 3. AVALIAÇÃO DO DIAGNÓSTICO E TRATAMENTO ---
cat("\n\n--- INICIANDO AVALIAÇÃO DO DIAGNÓSTICO E TRATAMENTO ---\n\n")

# --- 3.1. Diagnóstico Laboratorial ---
cat("--- 3.1. Diagnóstico Laboratorial ---\n")
cat("\n--- Tabela: Resultados dos Testes Diagnósticos ---\n")
tbl_testes <- NULL
tryCatch({
  colunas_desejadas_testes <- c("TPTESTE1", "TPCONFIRMA")
  colunas_existentes_testes <- colunas_desejadas_testes[colunas_desejadas_testes %in% names(df_analise)]
  if(length(colunas_existentes_testes) > 0) {
    tbl_testes <- df_analise %>%
      select(all_of(colunas_existentes_testes)) %>%
      tbl_summary( label = labels_pt, missing_text = "Ignorado/Ausente",
                   statistic = list(all_categorical() ~ "{n} ({p}%)"),
                   digits = list(all_categorical(stat = "p") ~ 1) ) %>%
      modify_header(label ~ "**Tipo de Teste**") %>% bold_labels()
    print(tbl_testes %>% as_tibble(col_labels = TRUE))
    cat("--------------------------------------------------\n")
    salvar_tabela_xlsx(tbl_testes, "tabela_testes_diagnosticos")
  } else {
    cat("Colunas de testes (TPTESTE1, TPCONFIRMA) não encontradas.\n")
  }
}, error = function(e){ cat("!! ERRO ao gerar Tabela de Testes Diagnósticos !!\n", e$message, "\n") })


# --- 3.2. Adequação do Tratamento da Gestante ---
cat("\n--- 3.2. Adequação do Tratamento da Gestante ---\n")
cat("Definindo variável de adequação do tratamento...\n")
# Definição da variável de adequação do tratamento da gestante
# Critérios baseados no PCDT IST 2022 (simplificado para dados SINAN)
# 1. Adequado: Primária/Secundária/Latente Recente (TPEVIDENCI 1,2,4) com TPESQUEMA=1 (2.4M); Terciária (TPEVIDENCI 3) com TPESQUEMA=3 (7.2M)
# 2. Inadequado: Clínica definida (1,2,3,4) com esquema diferente do esperado; TPESQUEMA=4 (Outro)
# 3. Não Tratada: TPESQUEMA=5
# 4. Ignorado: TPESQUEMA=9 ou NA
# 5. Indeterminada: TPEVIDENCI=9 ou NA
tryCatch({
  df_analise <- df_analise %>%
    mutate(
      TRAT_GESTANTE_ADEQUADO = case_when(
        TPESQUEMA %in% c("5-Não realizado") ~ "Não Tratada",
        TPESQUEMA %in% c("9-Ignorado") | is.na(TPESQUEMA) ~ "Tratamento Ignorado",
        TPEVIDENCI %in% c("9-Ignorado") | is.na(TPEVIDENCI) ~ "Adequação Indeterminada",
        TPEVIDENCI %in% c("1-Primária", "2-Secundária", "4-Latente") & TPESQUEMA == "1-Pen G benz 2.400.000UI" ~ "Adequado",
        TPEVIDENCI %in% c("1-Primária", "2-Secundária", "4-Latente") & TPESQUEMA != "1-Pen G benz 2.400.000UI" ~ "Inadequado",
        TPEVIDENCI == "3-Terciária" & TPESQUEMA == "3-Pen G benz 7.200.000UI" ~ "Adequado",
        TPEVIDENCI == "3-Terciária" & TPESQUEMA != "3-Pen G benz 7.200.000UI" ~ "Inadequado",
        TPESQUEMA == "4-Outro esquema" ~ "Inadequado (Outro Esquema)",
        TRUE ~ "Avaliação Inconclusiva" ),
      TRAT_ADEQUADO_BIN = case_when(
        TRAT_GESTANTE_ADEQUADO == "Adequado" ~ "Adequado",
        TRAT_GESTANTE_ADEQUADO %in% c("Não Tratada", "Inadequado", "Inadequado (Outro Esquema)") ~ "Inadequado/Não Tratado",
        TRUE ~ NA_character_ )
    )
  df_analise$TRAT_GESTANTE_ADEQUADO <- factor(df_analise$TRAT_GESTANTE_ADEQUADO, levels = c("Adequado", "Inadequado", "Inadequado (Outro Esquema)", "Não Tratada", "Tratamento Ignorado", "Adequação Indeterminada", "Avaliação Inconclusiva"))
  if (any(!is.na(df_analise$TRAT_ADEQUADO_BIN)) && n_distinct(df_analise$TRAT_ADEQUADO_BIN, na.rm = TRUE) > 1 && "Adequado" %in% unique(na.omit(df_analise$TRAT_ADEQUADO_BIN))) {
    df_analise$TRAT_ADEQUADO_BIN <- factor(df_analise$TRAT_ADEQUADO_BIN) 
    df_analise$TRAT_ADEQUADO_BIN <- relevel(df_analise$TRAT_ADEQUADO_BIN, ref = "Adequado") 
  } else if (any(!is.na(df_analise$TRAT_ADEQUADO_BIN))) {
    df_analise$TRAT_ADEQUADO_BIN <- factor(df_analise$TRAT_ADEQUADO_BIN) 
  }
  cat("Variáveis TRAT_GESTANTE_ADEQUADO e TRAT_ADEQUADO_BIN criadas.\n")
}, error = function(e){ cat("!! ERRO ao definir Variável de Adequação do Tratamento !!\n", e$message, "\n") })

# Gráfico: Adequação do Tratamento
cat("\nGerando gráfico: Adequação do Tratamento da Gestante...\n")
tryCatch({
  dados_plot_trat_gestante <- df_analise %>%
    filter(!is.na(TRAT_GESTANTE_ADEQUADO)) %>% 
    group_by(TRAT_GESTANTE_ADEQUADO) %>% summarise(N = n(), .groups = 'drop') %>%
    mutate(Porcentagem = N / sum(N),
           TRAT_GESTANTE_ADEQUADO = fct_reorder(TRAT_GESTANTE_ADEQUADO, N, .desc=FALSE))
  
  plot_trat_gest <- ggplot(dados_plot_trat_gestante, aes(x = N, y = TRAT_GESTANTE_ADEQUADO, fill = TRAT_GESTANTE_ADEQUADO)) +
    geom_col(show.legend = FALSE, alpha=0.9) +
    geom_text(aes(label = paste0(N, "\n(", format_percent_pt(Porcentagem), ")")), 
              hjust = -0.1, size = 3.0, lineheight=0.9, fontface="bold", color="black") + 
    scale_x_continuous(expand = expansion(mult = c(0, 0.30))) + 
    scale_fill_viridis_d(guide = "none") +
    labs(title = "Adequação do Tratamento da Gestante",
         subtitle = "Baseado na classificação clínica e esquema terapêutico (SINAN)",
         x = "Número de Casos", y = NULL) +
    theme(axis.text.y = element_text(size=rel(0.9)))
  print(plot_trat_gest)
  salvar_grafico_final(plot_trat_gest, "plot_adequacao_tratamento_gestante", width=8, height=6)
  salvar_tabela_xlsx(dados_plot_trat_gestante, "dados_adequacao_tratamento_gestante")
}, error = function(e){ cat("!! ERRO ao gerar Gráfico de Adequação do Tratamento !!\n", e$message, "\n") })

# --- 3.3. Tratamento do Parceiro ---
cat("\n--- 3.3. Tratamento do Parceiro ---\n")
cat("\nGerando gráfico: Tratamento do Parceiro...\n")
tryCatch({
  dados_plot_trat_parc <- df_analise %>%
    mutate(TRATPARC = fct_explicit_na(TRATPARC, na_level = "Ausente")) %>% 
    group_by(TRATPARC) %>% summarise(N = n(), .groups = 'drop') %>%
    mutate(Porcentagem = N / sum(N),
           LabelTxt = case_when( TRATPARC == "1-Sim" ~ "Sim", TRATPARC == "2-Não" ~ "Não",
                                 TRATPARC == "9-Ignorado" ~ "Ignorado", TRATPARC == "Ausente" ~ "Ausente", TRUE ~ "Outro"),
           Label = paste0(LabelTxt, "\n(", N, " / ", format_percent_pt(Porcentagem), ")"),
           TRATPARC = fct_reorder(TRATPARC, N, .desc=FALSE))
  
  plot_trat_parc <- ggplot(dados_plot_trat_parc, aes(x = N, y = TRATPARC, fill = TRATPARC)) +
    geom_col(show.legend = FALSE, alpha=0.9) +
    geom_text(aes(label = Label), hjust = -0.1, size = 3.0, lineheight=0.9, fontface="bold", color="black") + 
    scale_x_continuous(expand = expansion(mult = c(0, 0.30))) + 
    scale_fill_viridis_d(guide = "none") +
    scale_y_discrete(labels = function(x) gsub("^.-", "", x)) + 
    labs(title = "Tratamento Concomitante do Parceiro", x = "Número de Casos", y = "Parceiro Tratado?")
  print(plot_trat_parc)
  salvar_grafico_final(plot_trat_parc, "plot_tratamento_parceiro")
  salvar_tabela_xlsx(dados_plot_trat_parc %>% select(-Label), "dados_tratamento_parceiro")
}, error = function(e){ cat("!! ERRO ao gerar Gráfico de Tratamento do Parceiro !!\n", e$message, "\n") })


# --- 4. EPIDEMIOLOGIA ANALÍTICA ---
cat("\n\n--- INICIANDO EPIDEMIOLOGIA ANALÍTICA ---\n\n")
cat("--- 4.1. Fatores Associados ao Tratamento Inadequado/Não Tratamento ---\n")

# Preparar dados para o modelo
cat("Preparando dados para regressão...\n")
df_modelo <- NULL # Resetar df_modelo
tryCatch({
  if (!"TRAT_ADEQUADO_BIN" %in% names(df_analise)) {
    stop("Variável dependente 'TRAT_ADEQUADO_BIN' não foi criada corretamente.")
  }
  df_modelo <- df_analise %>%
    filter(!is.na(TRAT_ADEQUADO_BIN)) %>% 
    mutate(
      `Faixa Etária (anos)` = cut(AGE_YEARS, breaks = c(0, 19, 29, 39, Inf), labels = c("≤19", "20-29", "30-39", "≥40"), right = TRUE),
      `Escolaridade` = case_when( is.na(CS_ESCOL_N) | CS_ESCOL_N == "9-Ignorado" ~ "Ignorada/Ausente",
                                  CS_ESCOL_N %in% c("0-Analfabeto", "1-EF Incompleto(1-4)", "2-EF Completo(4)", "3-EF Incompleto(5-8)") ~ "Fundamental Incompleto ou menos",
                                  CS_ESCOL_N %in% c("4-EF Completo", "5-EM Incompleto") ~ "Fundamental Completo / Médio Incompleto",
                                  CS_ESCOL_N %in% c("6-EM Completo", "7-Superior Incompleto", "8-Superior Completo") ~ "Médio Completo ou mais", TRUE ~ "Outra"),
      `Escolaridade` = factor(`Escolaridade`, levels = c("Médio Completo ou mais", "Fundamental Completo / Médio Incompleto", "Fundamental Incompleto ou menos", "Ignorada/Ausente", "Outra")), 
      `Raça/Cor` = case_when( CS_RACA == "1-Branca" ~ "Branca", CS_RACA == "4-Parda" ~ "Parda", CS_RACA == "2-Preta" ~ "Preta",
                              CS_RACA %in% c("3-Amarela", "5-Indígena") ~ "Amarela/Indígena", CS_RACA == "9-Ignorado" | is.na(CS_RACA) ~ "Ignorada/Ausente", TRUE ~ "Outra"),
      `Raça/Cor` = factor(`Raça/Cor`, levels = c("Branca", "Parda", "Preta", "Amarela/Indígena", "Ignorada/Ausente", "Outra")), 
      `Idade Gestacional (Diagnóstico)` = case_when( CS_GESTANT == "1-1ºTrimestre" ~ "1º Trimestre", CS_GESTANT == "2-2ºTrimestre" ~ "2º Trimestre", CS_GESTANT == "3-3ºTrimestre" ~ "3º Trimestre", TRUE ~ "Ignorada/Outra"),
      `Idade Gestacional (Diagnóstico)` = factor(`Idade Gestacional (Diagnóstico)`, levels = c("1º Trimestre", "2º Trimestre", "3º Trimestre", "Ignorada/Outra")),
      `Classificação Clínica` = case_when( TPEVIDENCI %in% c("1-Primária", "2-Secundária") ~ "Primária/Secundária", TPEVIDENCI == "4-Latente" ~ "Latente", TPEVIDENCI == "3-Terciária" ~ "Terciária",
                                           is.na(TPEVIDENCI) | TPEVIDENCI == "9-Ignorado" ~ "Ignorada", TRUE ~ "Outra" ),
      `Classificação Clínica` = factor(`Classificação Clínica`, levels = c("Primária/Secundária", "Latente", "Terciária", "Ignorada", "Outra")),
      `Ano da Notificação` = factor(ANO_SGB)
    ) %>%
    filter(`Escolaridade` != "Outra", `Classificação Clínica` != "Outra", `Raça/Cor` != "Outra") %>% # Remover categorias 'Outra'
    # Filtrar NAs nas preditoras APÓS criação
    filter(!is.na(`Faixa Etária (anos)`), !is.na(`Raça/Cor`), !is.na(`Escolaridade`), 
           !is.na(`Classificação Clínica`), !is.na(`Idade Gestacional (Diagnóstico)`)) 
  
  dependent_var_model <- "TRAT_ADEQUADO_BIN" 
  if ("Adequado" %in% unique(na.omit(df_modelo[[dependent_var_model]]))) {
    df_modelo[[dependent_var_model]] <- factor(df_modelo[[dependent_var_model]]) 
    df_modelo[[dependent_var_model]] <- relevel(df_modelo[[dependent_var_model]], ref = "Adequado")
  } else { cat("AVISO: Nível de referência 'Adequado' não encontrado após filtros.\n") }
  
  explanatory_vars_model <- c("`Faixa Etária (anos)`", "`Raça/Cor`", "`Escolaridade`", 
                              "`Classificação Clínica`", "`Idade Gestacional (Diagnóstico)`", 
                              "`Ano da Notificação`")
  cat("Dados para regressão preparados. N =", nrow(df_modelo), "\n")
}, error = function(e){ cat("!! ERRO ao preparar dados para regressão !!\n", e$message, "\n"); df_modelo <- NULL })


# Ajustar e apresentar modelo apenas se df_modelo foi criado com sucesso
if (!is.null(df_modelo)) {
  min_obs_needed <- length(explanatory_vars_model) * 15 
  if (n_distinct(df_modelo[[dependent_var_model]], na.rm = TRUE) < 2 || nrow(df_modelo) < min_obs_needed ) {
    cat("Não há dados suficientes (N=", nrow(df_modelo), ", necessário ~", min_obs_needed, 
        ") ou variabilidade no desfecho para ajustar modelo.\n")
  } else {
    modelo_final_glm <- NULL
    cat("Ajustando modelo de regressão logística...\n")
    tryCatch({
      formula_modelo <- as.formula(paste(paste0("`", dependent_var_model, "`"), "~", paste(explanatory_vars_model, collapse = " + ")))
      modelo_final_glm <- glm(formula_modelo, family = binomial(link = "logit"), data = df_modelo)
      cat("Modelo ajustado.\n")
    }, error = function(e) { cat("!! ERRO ao ajustar modelo GLM !!\n", e$message, "\n") })
    
    if (!is.null(modelo_final_glm)) {
      # Tabela de Regressão
      cat("\n--- Tabela: Regressão Logística Multivariada ---\n")
      tbl_reg <- NULL 
      tryCatch({
        tbl_reg <- tbl_regression( modelo_final_glm, exponentiate = TRUE, label = labels_pt) %>%
          bold_labels() %>% bold_p(t = 0.05) %>%
          modify_fmt_fun(update = c(estimate, conf.low, conf.high) ~ function(x) style_number(x, digits = 2, decimal.mark=",")) %>%
          modify_table_styling(columns = label, footnote = "OR: Odds Ratio Ajustado; IC 95%: Intervalo de Confiança de 95%.") %>%
          modify_header(estimate ~ "**OR (IC 95%)**") %>% modify_column_hide(columns = c(ci))
        
        print(tbl_reg %>% as_tibble(col_labels = TRUE))
        cat("------------------------------------------------\n")
        salvar_tabela_xlsx(tbl_reg, "tabela_regressao_logistica_trat_inadequado")
      }, error = function(e){ cat("!! ERRO ao gerar Tabela de Regressão !!\n", e$message, "\n") })
      
      # Forest Plot
      cat("\nGerando Forest Plot...\n")
      plot_forest <- NULL 
      tryCatch({
        # Preparar labels para finalfit (precisa casar nomes exatos das variáveis no modelo)
        # finalfit usa os nomes das colunas do dataframe passado
        # Renomear df_modelo temporariamente para or_plot pode ser mais fácil
        df_plot_ff <- df_modelo %>% rename(!!!labels_pt) # Tenta renomear usando a lista de labels
        
        # Pegar os novos nomes das variáveis explicativas
        explanatory_vars_ff <- names(labels_pt)[names(labels_pt) %in% explanatory_vars_model] 
        # ^ Isso pode não funcionar bem com nomes com espaço/crase...
        # Melhor pegar os nomes DEPOIS do rename:
        explanatory_vars_ff <- setdiff(names(df_plot_ff), dependent_var_model)
        explanatory_vars_ff <- explanatory_vars_ff[explanatory_vars_ff %in% map_chr(labels_pt, ~as_label(as.formula(.)))] # Garantir que são os labels corretos
        
        # OBS: Renomear pode ser complexo. Usar tbl_regression e ggcoefstats pode ser alternativa
        
        # TENTATIVA COM or_plot e dados originais, confiando que ele pega labels do modelo
        plot_forest <- df_modelo %>% 
          or_plot(
            dependent = dependent_var_model,
            explanatory = explanatory_vars_model, # Passar nomes com crase
            remove_ref = TRUE, # Remover ref do plot
            table_text_size = 3, plot_text_size = 3,  
            breaks = c(0.1, 0.5, 1, 2, 5, 10)
          ) +
          labs(title = "Fatores Associados ao Tratamento Inadequado/Não Tratamento da Gestante") +
          theme(plot.title = element_text(hjust = 0.5)) 
        
      }, error = function(e) { cat("!! ERRO ao gerar Forest Plot (or_plot) !!\n", e$message, "\n") })
      
      if (!is.null(plot_forest)) {
        print(plot_forest) 
        salvar_grafico_final(plot_forest, "plot_forest_trat_inadequado", width = 10, height = 9) 
      } else { cat("-> Forest plot não gerado devido a erro.\n") }
    } else { cat("Modelo não ajustado, resultados não podem ser apresentados.\n") }
  } 
} else { cat("Dados insuficientes para modelo após preparação.\n") }


# --- 5. FINALIZAÇÃO ---
cat("\n\n--- ANÁLISE CONCLUÍDA ---\n")
cat("Resultados salvos em:", dir_relatorio, "\n")

cat("\n--- Informações da Sessão R ---\n")
print(sessionInfo())
cat("-------------------------------\n")

# Fim do script