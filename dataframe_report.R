# -------------------------------------------------------------------------
# --- DESCRIÇÃO ESTRUTURAL DETALHADA DE DATAFRAME EM R --------------------
# -------------------------------------------------------------------------
# Autor: Levi de Melo Amorim (FAMED/UFAL) & AI Assistant
# Data: 2025-05-09
#
# Objetivo:
#   Este script carrega um arquivo .RData contendo um objeto dataframe e
#   gera, ao final, um relatório completo no console com:
#     • Visão geral: dimensões, nomes e tipos de colunas,
#       estrutura inicial e primeiras linhas.
#     • Resumo estatístico via skimr.
#     • Análise de valores ausentes.
#     • Exploração de variáveis numéricas (summary + valores únicos até 15).
#     • Exploração de variáveis categóricas (top-15 frequências, excluindo IDs).
#     • Exploração de variáveis de data (intervalos e NAs).
#
#   TODO: Não gera arquivos externos; todo o output é logado no console
#         apenas ao final, em blocos ordenados.
#
# Configuração (ajuste antes de rodar):
#   DATA_FILE_PATH : caminho para o .RData com o dataframe.
#   DATAFRAME_NAME : nome exato do objeto dataframe dentro do .RData.
# -------------------------------------------------------------------------

# --- 0. CONFIGURAÇÃO DO USUÁRIO ---
DATA_FILE_PATH <- "sifg_alagoas_2010_2022_processed.RData"
DATAFRAME_NAME <- "sifg_translated_data"
# -------------------------------------------------------------------------

# --- 1. SETUP INICIAL ---
suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(skimr)
})

# --- 2. CARREGAMENTO DE DADOS ---
if (!file.exists(DATA_FILE_PATH)) {
  stop("ERRO: Arquivo não encontrado em ", DATA_FILE_PATH)
}
env_temp <- new.env()
load(DATA_FILE_PATH, envir = env_temp)
if (!exists(DATAFRAME_NAME, envir = env_temp)) {
  stop("ERRO: Objeto '", DATAFRAME_NAME, "' não existe dentro de ", DATA_FILE_PATH)
}
df <- env_temp[[DATAFRAME_NAME]]
rm(env_temp)
if (!is.data.frame(df) || nrow(df) == 0) {
  stop("ERRO: Objeto carregado não é um dataframe válido ou está vazio.")
}

# --- 3. PREPARAÇÃO DO RELATÓRIO EM MEMÓRIA ---
output_list <- list()

# 3.1 Visão Geral
output_list[["1_Visao_Geral"]] <- capture.output({
  cat("======================================================================\n")
  cat("                         1) VISÃO GERAL\n")
  cat("======================================================================\n")
  cat("Dimensões  : ", nrow(df), " linhas x ", ncol(df), " colunas\n", sep = "")
  cat("\nColunas e Tipos:\n")
  col_types <- map_chr(df, ~ paste(class(.x), collapse = ", "))
  print(tibble(Coluna = names(col_types), Tipo = col_types))
  cat("\nEstrutura (primeiras 10 colunas):\n")
  str(df[, 1:min(10, ncol(df))], list.len = 10)
  cat("\nPrimeiras 3 linhas:\n")
  print(head(df, 3))
})

# 3.2 Resumo Skimr
output_list[["2_Skimr"]] <- capture.output({
  cat("======================================================================\n")
  cat("                      2) RESUMO ESTATÍSTICO (skimr)\n")
  cat("======================================================================\n")
  print(skim(df))
})

# 3.3 Valores Ausentes
output_list[["3_Missing"]] <- capture.output({
  cat("======================================================================\n")
  cat("                 3) VALORES AUSENTES\n")
  cat("======================================================================\n")
  missing_tbl <- df %>%
    summarise(across(everything(), ~ sum(is.na(.)))) %>%
    pivot_longer(everything(), names_to = "Variavel", values_to = "N_Ausentes") %>%
    mutate(Pct = N_Ausentes / nrow(df)) %>%
    filter(N_Ausentes > 0) %>%
    arrange(desc(Pct))
  if (nrow(missing_tbl) == 0) {
    cat("Nenhuma coluna possui valores ausentes.\n")
  } else {
    missing_tbl %>%
      mutate(Pct = scales::percent(Pct, accuracy = 0.1, decimal.mark = ",")) %>%
      print(n = Inf)
  }
})

# 3.4 Variáveis Numéricas
output_list[["4_Numeric"]] <- capture.output({
  cat("======================================================================\n")
  cat("                4) VARIÁVEIS NUMÉRICAS\n")
  cat("======================================================================\n")
  df_num <- select(df, where(is.numeric))
  if (ncol(df_num) == 0) {
    cat("Não há variáveis numéricas no dataframe.\n")
  } else {
    cat("Resumo estatístico (summary):\n")
    print(summary(df_num))
    cat("\nValores únicos (até 15 distintos):\n")
    walk(names(df_num), function(v) {
      vals  <- unique(na.omit(df_num[[v]]))
      nvals <- length(vals)
      if (nvals <= 15) {
        cat(" - ", v, " (", nvals, " distintos): ", paste(sort(vals), collapse = ", "), "\n", sep = "")
      } else {
        cat(" - ", v, " (", nvals, " distintos): Muitos valores\n", sep = "")
      }
    })
  }
})

# 3.5 Variáveis Categóricas
output_list[["5_Categorical"]] <- capture.output({
  cat("======================================================================\n")
  cat("           5) VARIÁVEIS CATEGÓRICAS (chr/factor)\n")
  cat("======================================================================\n")
  cats <- names(df)[map_lgl(df, ~ is.character(.x) || is.factor(.x))]
  # excluir prováveis IDs com >1000 níveis
  cats <- keep(cats, ~ n_distinct(df[[.x]], na.rm = TRUE) <= 1000)
  if (length(cats) == 0) {
    cat("Não há variáveis categóricas relevantes.\n")
  } else {
    walk(cats, function(v) {
      cat("\n--- Variável:", v, "---\n")
      total_na <- sum(is.na(df[[v]]))
      pct_na   <- scales::percent(total_na / nrow(df), accuracy = 0.1, decimal.mark = ",")
      cat("Níveis únicos:", n_distinct(df[[v]], na.rm = TRUE),
          "| NAs:", total_na, "(", pct_na, ")\n")
      freq_tbl <- df %>%
        count(Valor = .data[[v]], name = "Contagem") %>%
        filter(!is.na(Valor)) %>%
        arrange(desc(Contagem)) %>%
        mutate(Pct = scales::percent(Contagem / sum(Contagem), accuracy = 0.1, decimal.mark = ","))
      cat("Top 15 frequências:\n")
      print(head(freq_tbl, 15), n = 15)
      if (nrow(freq_tbl) > 15) {
        cat("...mais", nrow(freq_tbl) - 15, "níveis não mostrados\n")
      }
    })
  }
})

# 3.6 Variáveis de Data
output_list[["6_Dates"]] <- capture.output({
  cat("======================================================================\n")
  cat("               6) VARIÁVEIS DE DATA\n")
  cat("======================================================================\n")
  df_dates <- select(df, where(is.Date))
  if (ncol(df_dates) == 0) {
    cat("Não há variáveis do tipo Date.\n")
  } else {
    walk(names(df_dates), function(v) {
      col    <- df_dates[[v]]
      dt_min <- min(col, na.rm = TRUE)
      dt_max <- max(col, na.rm = TRUE)
      na_cnt <- sum(is.na(col))
      pct_na <- scales::percent(na_cnt / nrow(df), accuracy = 0.1, decimal.mark = ",")
      cat(" - ", v, ": ", as.character(dt_min), " a ", as.character(dt_max),
          " (", na_cnt, " NAs, ", pct_na, ")\n", sep = "")
    })
  }
})

# --- 4. IMPRESSÃO FINAL NO CONSOLE ---
for (section in sort(names(output_list))) {
  cat(output_list[[section]], sep = "\n")
  cat("\n")  # separador entre seções
}
