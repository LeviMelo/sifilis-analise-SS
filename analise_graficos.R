# --- ANÁLISE EPIDEMIOLÓGICA DA SÍFILIS GESTACIONAL EM ALAGOAS (2010-2022) ---
#
# AUTOR: Gerado por IA (revisão de script original)
# DATA DE EXECUÇÃO: Sys.Date()
#
# DESCRIÇÃO GERAL:
# Este script realiza uma análise epidemiológica detalhada dos casos de sífilis 
# gestacional notificados no estado de Alagoas, Brasil, entre 2010 e 2022.
# O script utiliza dados processados (presumivelmente do SINAN) e foca em:
#   1. Configuração do Ambiente: Carregamento de bibliotecas, definição de
#      locales, criação de diretórios de saída e temas gráficos.
#   2. Carregamento e Pré-processamento de Dados: Carrega os dados, realiza
#      transformações de variáveis (fatores, numéricos), e define
#      parâmetros básicos da análise (período, UF, total de casos).
#   3. Análise Descritiva:
#      - Perfil de dados ausentes.
#      - Características sociodemográficas (idade, raça/cor, escolaridade) e
#        gestacionais (idade gestacional no diagnóstico, classificação clínica).
#      - Distribuição da idade materna (geral e por períodos).
#      - Distribuição por raça/cor e escolaridade.
#      - Tendência temporal dos casos notificados.
#      - Distribuição espacial dos casos por município de residência.
#      - Análise dos títulos do teste não treponêmico (DSTITULO1).
#   4. Avaliação do Diagnóstico e Tratamento:
#      - Resultados dos testes diagnósticos (não treponêmico e confirmatório).
#      - Criação de variáveis para adequação do tratamento da gestante
#        (detalhada e binária).
#      - Análise da adequação do tratamento da gestante.
#      - Análise do tratamento concomitante do parceiro.
#   5. Epidemiologia Analítica (Regressão Logística):
#      - Preparação dos dados para modelagem, com categorização de preditores
#        (faixa etária, escolaridade, raça/cor, classificação clínica,
#        idade gestacional, período de notificação).
#      - Ajuste de um modelo de regressão logística multivariada para 
#        identificar fatores associados ao tratamento inadequado/não tratamento.
#      - Apresentação dos resultados do modelo em tabela (gtsummary) e
#        graficamente (forest plot).
#   6. Saída:
#      - Gráficos são salvos em formato PNG.
#      - Tabelas (data.frames e objetos gtsummary) são salvas em formato XLSX.
#      - Resultados e mensagens de progresso são impressos no console.
#
# DEPENDÊNCIAS (Pacotes R):
#   - tidyverse, lubridate, scales, gtsummary, broom, broom.helpers,
#   - openxlsx, here, sf, geobr, patchwork, viridisLite, ggrepel, labelled,
#   - forcats
#
# ARQUIVO DE DADOS DE ENTRADA ESPERADO:
#   - "sifg_alagoas_2010_2022_processed.RData" (contendo o objeto 
#     'sifg_translated_data')
#
# ESTRUTURA DO DIRETÓRIO DE SAÍDA:
#   - ./relatorio/ (criado se não existir)
#
# NOTAS PARA DESENVOLVEDORES FUTUROS:
#   - Certifique-se de que o arquivo de dados de entrada está no diretório
#     correto ou ajuste o caminho em 'data_file'.
#   - A lógica de adequação do tratamento ('TRAT_GESTANTE_ADEQUADO') é baseada
#     em diretrizes comuns, mas pode precisar de revisão se os critérios locais
#     ou as definições das variáveis de entrada mudarem.
#   - A categorização das variáveis para o modelo de regressão (ex: Faixa_Etaria,
#     Escolaridade_Mod) pode ser ajustada conforme necessidade analítica.
#   - A interpretação dos resultados da regressão deve considerar as limitações
#     dos dados, como missing data e possíveis vieses de notificação.
#   - O download de shapefiles por 'geobr' requer conexão com a internet.
#
# ---------------------------------------------------------------------------

# --- 1. CONFIGURAÇÃO INICIAL ---
cat("1. CONFIGURAÇÃO INICIAL\n")
cat("1.1 Carregando bibliotecas...\n")
suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(scales)
  library(gtsummary)
  library(broom)
  library(broom.helpers) # Essencial para tidy_plus_plus
  library(openxlsx)
  library(here)
  library(sf)
  library(geobr)
  library(patchwork)
  library(viridisLite) # Usado para cores
  library(ggrepel)     # Para evitar sobreposição de texto em gráficos (se necessário)
  library(labelled)    # Para trabalhar com labels de variáveis
  library(forcats)     # Para manipulação de fatores
})
cat("-> Bibliotecas carregadas.\n\n")

# --- 1.2. Configurações de locale ---
cat("1.2 Configurações de locale...\n")
tryCatch({
  Sys.setlocale("LC_TIME", "pt_BR.UTF-8")
  Sys.setlocale("LC_ALL",  "pt_BR.UTF-8")
  cat("-> Locale definido para pt_BR.UTF-8.\n\n")
}, error = function(e) {
  message("!! Aviso: Locale pt_BR.UTF-8 não disponível. Usando locale padrão do sistema.\n", e$message, "\n\n")
})

# --- 1.3. Diretório de saída ---
cat("1.3 Definindo diretório de saída...\n")
dir_relatorio <- here::here("relatorio_sifilis_gestacional_AL")
if (!dir.exists(dir_relatorio)) {
  dir.create(dir_relatorio, recursive = TRUE)
  cat("-> Criado diretório de saída:", dir_relatorio, "\n\n")
} else {
  cat("-> Diretório de saída existente:", dir_relatorio, "\n\n")
}

# --- 1.4. Tema padrão para ggplot2 ---
cat("1.4 Definindo tema padrão para ggplot2...\n")
theme_set(
  theme_minimal(base_size = 12) + # Aumentei base_size para melhor legibilidade
    theme(
      plot.title            = element_text(hjust = 0.5, face = "bold", size = rel(1.2), margin = margin(b = 5)),
      plot.subtitle         = element_text(hjust = 0.5, size = rel(1.0), margin = margin(b = 10)),
      axis.title            = element_text(face = "bold", size = rel(1.0)),
      axis.text             = element_text(size = rel(0.9)),
      legend.title          = element_text(face = "bold", size = rel(0.9)),
      legend.text           = element_text(size = rel(0.85)),
      legend.position       = "bottom",
      strip.text            = element_text(face = "bold", size = rel(1.0), color = "white"), # Melhor contraste
      strip.background      = element_rect(fill = "grey40", color = "grey40"), # Melhor contraste
      panel.grid.minor      = element_blank(),
      panel.grid.major      = element_line(color = "grey90", linetype = "dashed"),
      plot.caption          = element_text(hjust = 0, face = "italic", size = rel(0.8)),
      plot.caption.position = "plot" # Alinha caption à esquerda do plot
    )
)
options(
  ggplot2.discrete.fill   = viridisLite::viridis, # Esquema de cores padrão para preenchimento discreto
  ggplot2.discrete.colour = viridisLite::viridis  # Esquema de cores padrão para cor discreta
)
cores_principais <- viridisLite::plasma(6) # Usando plasma para variação
cat("-> Tema ggplot2 definido.\n\n")

# --- 1.5. Funções auxiliares ---
cat("1.5 Definindo funções auxiliares...\n")
salvar_grafico_final <- function(plot_obj, nome_arquivo_base,
                                 width = 8, height = 6, dpi = 300) { # DPI aumentado
  if (is.null(plot_obj)) {
    cat("!! Aviso: Objeto de gráfico para '", nome_arquivo_base, "' é NULL. Não salvo.\n")
    return(invisible(NULL))
  }
  dest_png <- file.path(dir_relatorio, paste0(nome_arquivo_base, ".png"))
  tryCatch({
    ggsave(
      filename = dest_png,
      plot     = plot_obj,
      width    = width,
      height   = height,
      dpi      = dpi,
      bg       = "white"
    )
    cat("-> Gráfico salvo em:", dest_png, "\n")
  }, error = function(e) {
    cat("!! Erro ao salvar gráfico", nome_arquivo_base, ":", e$message, "\n")
  })
}

salvar_tabela_xlsx <- function(tabela_obj, nome_arquivo_base,
                               nome_planilha = "Dados") {
  if (is.null(tabela_obj)) {
    cat("!! Aviso: Objeto de tabela para '", nome_arquivo_base, "' é NULL. Não salvo.\n")
    return(invisible(NULL))
  }
  wb <- createWorkbook()
  addWorksheet(wb, nome_planilha)
  
  df_para_salvar <- NULL
  if (inherits(tabela_obj, "tbl_summary") || inherits(tabela_obj, "tbl_regression") || inherits(tabela_obj, "tbl_uvregression")) {
    # Para gtsummary objects, usar as_flex_table() ou as_gt() e depois exportar, ou as_tibble()
    # A conversão para tibble pode perder formatação, mas é mais simples para XLSX.
    # Removendo markdown para melhor visualização no Excel
    df_para_salvar <- gtsummary::as_tibble(tabela_obj, col_labels = TRUE) %>%
      mutate(across(everything(), ~str_replace_all(., "\\*\\*", ""))) # remove negrito markdown
  } else if (is.data.frame(tabela_obj)) {
    df_para_salvar <- tabela_obj
  } else {
    tryCatch({
      df_para_salvar <- as.data.frame(tabela_obj)
    }, error = function(e) {
      cat("!! Erro ao converter objeto para data.frame para salvar tabela", nome_arquivo_base, ":", e$message, "\n")
      return(invisible(NULL))
    })
  }
  
  if(is.null(df_para_salvar)) return(invisible(NULL))

  dest_xlsx <- file.path(dir_relatorio, paste0(nome_arquivo_base, ".xlsx"))
  tryCatch({
    writeData(wb, sheet = nome_planilha, x = df_para_salvar, 
              headerStyle = createStyle(textDecoration = "bold")) # Estilo para cabeçalho
    # Auto-ajuste da largura das colunas
    setColWidths(wb, nome_planilha, cols = 1:ncol(df_para_salvar), widths = "auto")
    saveWorkbook(wb, file = dest_xlsx, overwrite = TRUE)
    cat("-> Tabela salva em:", dest_xlsx, "\n")
  }, error = function(e) {
    cat("!! Erro ao salvar tabela", nome_arquivo_base, ":", e$message, "\n")
  })
}
cat("-> Funções auxiliares definidas.\n\n")

# --- 1.6. Carregamento e preparação dos dados ---
cat("2. CARREGAMENTO E PRÉ-PROCESSAMENTO DOS DADOS\n")
data_file <- "sifg_alagoas_2010_2022_processed.RData" # Certifique-se que este arquivo está no diretório de trabalho
if (!file.exists(data_file)) {
  stop("ERRO CRÍTICO: Arquivo de dados não encontrado: ", data_file,
       "\nExecute o script de processamento de dados antes ou verifique o caminho.")
}
load(data_file) # Deve carregar 'sifg_translated_data'
if (!exists("sifg_translated_data") || !is.data.frame(sifg_translated_data) ||
    nrow(sifg_translated_data) == 0) {
  stop("ERRO CRÍTICO: Objeto 'sifg_translated_data' não encontrado, não é um data.frame ou está vazio em ", data_file)
}
cat("-> Dados carregados com sucesso.\n")

cat("2.1 Pré-processamento e criação de variáveis derivadas...\n")
df_analise <- sifg_translated_data %>%
  mutate(
    ANO_SGB_fct       = factor(ANO_SGB), # Ano como fator
    # Corrigindo conversão de DSTITULO1 para numérico
    DSTITULO1_numeric = suppressWarnings(as.numeric(as.character(DSTITULO1))),
    DSTITULO1_numeric = ifelse(is.finite(DSTITULO1_numeric), DSTITULO1_numeric, NA_real_),
    # Log2 do título para análise (adicionar 1 para evitar log(0) se houver '0')
    DSTITULO1_log2    = ifelse(DSTITULO1_numeric > 0, log2(DSTITULO1_numeric), NA_real_),

    # Reordenando e garantindo que níveis de fatores estejam corretos
    CS_RACA           = factor(CS_RACA,
                               levels = c("1-Branca","2-Preta","3-Amarela",
                                          "4-Parda","5-Indígena","9-Ignorado")),
    CS_ESCOL_N        = factor(CS_ESCOL_N,
                               levels = c("0-Analfabeto","1-EF Incompleto(1-4)",
                                          "2-EF Completo(4)","3-EF Incompleto(5-8)",
                                          "4-EF Completo","5-EM Incompleto",
                                          "6-EM Completo","7-Superior Incompleto",
                                          "8-Superior Completo","9-Ignorado",
                                          "10-Não se aplica")), # Manter "Não se aplica" se presente
    CS_GESTANT        = factor(CS_GESTANT,
                               levels = c("1-1ºTrimestre","2-2ºTrimestre",
                                          "3-3ºTrimestre","4-Idade gestacional ignorada","9-Ignorado")), # Adicionar "9-Ignorado" se presente
    TPEVIDENCI        = factor(TPEVIDENCI,
                               levels = c("1-Primária","2-Secundária",
                                          "3-Terciária","4-Latente","9-Ignorado")),
    TPESQUEMA         = factor(TPESQUEMA,
                               levels = c("1-Pen G benz 2.400.000UI",
                                          "2-Pen G benz 4.800.000UI",
                                          "3-Pen G benz 7.200.000UI",
                                          "4-Outro esquema","5-Não realizado","9-Ignorado")),
    TRATPARC          = factor(TRATPARC,
                               levels = c("1-Sim","2-Não","9-Ignorado")),
    
    # Simplificação dos resultados de testes para tabelas e plots
    TPTESTE1_Simple   = factor(case_when(
      TPTESTE1 == "1-Reagente"     ~ "Reagente",
      TPTESTE1 == "2-Não reagente" ~ "Não Reagente",
      TPTESTE1 == "3-Não realizado"~ "Não Realizado", # Adicionado
      is.na(TPTESTE1)              ~ "NA/Ausente",    # Adicionado
      TRUE                         ~ "Ignorado"       # Captura "9-Ignorado"
    ), levels = c("Reagente", "Não Reagente", "Não Realizado", "Ignorado", "NA/Ausente")),
    
    TPCONFIRMA_Simple = factor(case_when(
      TPCONFIRMA == "1-Reagente"     ~ "Reagente",
      TPCONFIRMA == "2-Não reagente" ~ "Não Reagente",
      TPCONFIRMA == "3-Não realizado"~ "Não Realizado", # Adicionado
      is.na(TPCONFIRMA)              ~ "NA/Ausente",    # Adicionado
      TRUE                           ~ "Ignorado"       # Captura "9-Ignorado"
    ), levels = c("Reagente", "Não Reagente", "Não Realizado", "Ignorado", "NA/Ausente")),
    
    # Variável de Faixa Etária para facilitar análises
    Faixa_Etaria_Cat = cut(AGE_YEARS,
                           breaks = c(0, 14, 19, 24, 29, 34, 39, Inf), # Maior granularidade
                           labels = c("<=14", "15-19", "20-24", "25-29", "30-34", "35-39", ">=40"),
                           right = TRUE, include.lowest = TRUE)
  )

# Definindo labels para variáveis (exemplo, adicione mais conforme necessário)
df_analise <- df_analise %>%
  set_variable_labels(
    AGE_YEARS = "Idade Materna (anos)",
    Faixa_Etaria_Cat = "Faixa Etária Materna",
    CS_RACA = "Raça/Cor Autodeclarada",
    CS_ESCOL_N = "Nível de Escolaridade",
    CS_GESTANT = "Idade Gestacional no Diagnóstico",
    TPEVIDENCI = "Classificação Clínica da Sífilis",
    DSTITULO1_numeric = "Título do Teste Não Treponêmico",
    DSTITULO1_log2 = "Log2 do Título do Teste Não Treponêmico",
    TPTESTE1_Simple = "Resultado Teste Não Treponêmico (Simplificado)",
    TPCONFIRMA_Simple = "Resultado Teste Confirmatório (Simplificado)",
    TPESQUEMA = "Esquema Terapêutico da Gestante",
    TRATPARC = "Parceiro Tratado Concomitantemente"
  )

ANO_INICIO  <- min(df_analise$ANO_SGB, na.rm = TRUE)
ANO_FIM     <- max(df_analise$ANO_SGB, na.rm = TRUE)
UF_ESTUDO   <- df_analise$NOME_UF_NOT[1] # Assume que é constante
UF_CODIGO   <- df_analise$SG_UF_NOT[1]   # Assume que é constante
TOTAL_CASOS <- nrow(df_analise)

cat("-> Pré‐processamento e criação de variáveis concluídos.\n")
cat("Período da análise:", ANO_INICIO, "-", ANO_FIM, "\n")
cat("UF em estudo:", UF_ESTUDO, "(Código IBGE:", UF_CODIGO, ")\n")
cat("Total de Casos no período:", 
    scales::label_number(big.mark = ".", decimal.mark = ",")(TOTAL_CASOS), 
    "\n\n")

# --- 2. ANÁLISE DESCRITIVA ---
cat("3. ANÁLISE DESCRITIVA\n")

## 2.1. Perfil de dados ausentes
cat("3.1 Análise de Dados Ausentes...\n")
tabela_missing_full <- df_analise %>%
  summarise(across(everything(), ~sum(is.na(.)))) %>%
  pivot_longer(everything(),
               names_to  = "Variável",
               values_to = "N_Ausentes") %>%
  mutate(Pct_Ausente = N_Ausentes / TOTAL_CASOS) %>%
  filter(N_Ausentes > 0) %>%
  arrange(desc(Pct_Ausente)) %>%
  mutate(Pct_Ausente_Format = scales::percent(Pct_Ausente, accuracy = 0.1, decimal.mark = ","))

cat("Principais variáveis com dados ausentes:\n")
print(
  tabela_missing_full %>%
    select(Variável, N_Ausentes, Pct_Ausente_Format) %>%
    slice_head(n = 15), # Mostra as 15 primeiras
  n = 15
)
salvar_tabela_xlsx(tabela_missing_full,
                   "tabela_dados_ausentes_completa",
                   "Dados Ausentes")
cat("\n")

## 2.2. Características sociodemográficas e gestacionais (Tabela gtsummary)
cat("3.2 Tabela de Características Sociodemográficas e Gestacionais...\n")
tbl_caracteristicas <- df_analise %>%
  select(AGE_YEARS, Faixa_Etaria_Cat, CS_RACA, CS_ESCOL_N,
         CS_GESTANT, TPEVIDENCI, DSTITULO1_numeric) %>%
  # Garante que DSTITULO1_numeric é puro numeric
  mutate(DSTITULO1_numeric = as.numeric(DSTITULO1_numeric)) %>%
  tbl_summary(
    # by = NULL,      # não precisamos desse argumento explícito
    type       = list(
      all_continuous()  ~ "continuous2",
      all_categorical() ~ "categorical"
    ),
    statistic  = list(
      all_continuous()  ~ c("{mean} (DP {sd})", 
                            "{median} (Q1 {p25}, Q3 {p75})", 
                            "{min}–{max}"),
      all_categorical() ~ "{n} ({p}%)"
    ),
    digits        = list(
      all_continuous()  ~ 1,
      all_categorical() ~ c(0, 1)
    ),
    missing_text  = "(Ignorado/Ausente)",
    sort          = list(everything() ~ "frequency")
  ) %>%
  modify_header(
    label  ~ "**Característica**",
    stat_0 ~ "**N (%)**"
  ) %>%
  modify_table_body(~ .x %>%
                      mutate(
                        label = ifelse(
                          row_type == "level",
                          str_replace(label, "^[0-9]+[\\.-]\\s*", ""),
                          label
                        ) %>% str_trim()
                      )
  ) %>%
  bold_labels() %>%
  modify_footnote(everything() ~ NA) %>%
  modify_caption(
    paste0(
      "**Tabela 1: Características Sociodemográficas e Gestacionais ",
      "(N = ", format(TOTAL_CASOS, big.mark = "."), ")**"
    )
  )

print(tbl_caracteristicas %>% as_tibble(col_labels = TRUE))
salvar_tabela_xlsx(
  tbl_caracteristicas,
  "tabela_caracteristicas_socio_gest",
  "Caracteristicas"
)

cat("\n")

## 2.3. Gráfico: Distribuição da Idade Materna (Histograma e Densidade)
cat("3.3 Gráfico — Distribuição da Idade Materna...\n")
# 2a) Preparar o factor “limpo” de raça
df_analise <- df_analise %>%
  mutate(
    Raca_Label = str_remove(as.character(CS_RACA), "^[0-9]+[\\.-]\\s*"),
    Raca_Label = fct_explicit_na(Raca_Label, na_level = "Ignorado")
  )

# 2b) Cálculo de média, mediana e máximo de contagens por idade
mean_age   <- mean(df_analise$AGE_YEARS, na.rm = TRUE)
median_age <- median(df_analise$AGE_YEARS, na.rm = TRUE)
# Como binwidth=1, o count por idade é igual ao histograma:
age_counts <- df_analise %>% count(AGE_YEARS)
max_count   <- max(age_counts$n)

plot_idade_raca2 <- ggplot(df_analise, 
                           aes(x = AGE_YEARS, fill = Raca_Label)) +
  geom_histogram(aes(y = after_stat(count)), 
                 binwidth = 1,
                 color    = "white",
                 alpha    = 0.8,
                 position = "stack") +
  # Anotações de média e mediana no topo do gráfico
  geom_vline(xintercept = mean_age,   color = "firebrick", linetype = "dashed", size = 0.8) +
  geom_vline(xintercept = median_age, color = "darkblue",  linetype = "dotted", size = 0.8) +
  annotate("text",
           x    = mean_age * 1.15,
           y    = max_count * 1.05,
           label= paste0("Média = ", sprintf("%.1f", mean_age)),
           color= "firebrick",
           vjust= -0.5) +
  annotate("text",
           x    = median_age * 0.85,
           y    = max_count * 1.05,
           label= paste0("Mediana = ", sprintf("%.1f", median_age)),
           color= "darkblue",
           vjust= -0.5) +
  scale_x_continuous(breaks = seq(0, 50, 5)) +
  scale_y_continuous(
    name      = "Número de Casos",
    labels    = scales::label_number(big.mark = ".", decimal.mark = ","),
    expand    = expansion(mult = c(0, 0.12)) # espaço extra para as anotações
  ) +
  scale_fill_viridis_d(name = "Raça/Cor") +
  labs(
    title    = "Distribuição da Idade Materna por Raça/Cor",
    subtitle = paste0(UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM),
    x        = "Idade Materna (anos)"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position  = "bottom",
    legend.title     = element_text(face = "bold"),
    plot.title       = element_text(hjust = 0.5, face = "bold"),
    plot.subtitle    = element_text(hjust = 0.5),
    axis.text.x      = element_text(angle = 45, hjust = 1)
  )

print(plot_idade_raca2)

salvar_grafico_final(plot_idade_raca2,
                     "plot_distribuicao_idade_raca_contagem",
                     width = 10, height = 6)
# Exporta a tabela de contagens por idade que alimentou o histograma
salvar_tabela_xlsx(
  age_counts %>%
    rename(
      Idade_Materna = AGE_YEARS,
      Contagem      = n
    ),
  "dados_distribuicao_idade_materna",
  "Idade Materna"
)


cat("\n")

## 2.4. Gráfico: Idade Materna por Período (Violino e Boxplot)
cat("3.4 Gráfico — Idade Materna por Período Definido...\n")
df_periodo <- df_analise %>%
  mutate(
    Periodo = case_when(
      ANO_SGB %in% 2010:2014 ~ "2010-2014",
      ANO_SGB %in% 2015:2018 ~ "2015-2018",
      ANO_SGB %in% 2019:2022 ~ "2019-2022",
      TRUE                   ~ NA_character_
    ),
    Periodo = factor(Periodo, levels = c("2010-2014","2015-2018","2019-2022"))
  ) %>%
  filter(!is.na(Periodo), !is.na(AGE_YEARS))

if (nrow(df_periodo) > 0) {
  stats_periodo <- df_periodo %>%
    group_by(Periodo) %>%
    summarise(
      N       = n(),
      Media   = round(mean(AGE_YEARS, na.rm = TRUE), 1),
      DP      = round(sd(AGE_YEARS, na.rm = TRUE), 1),
      Mediana = round(median(AGE_YEARS, na.rm = TRUE), 1),
      Q1      = round(quantile(AGE_YEARS, 0.25, na.rm = TRUE), 1),
      Q3      = round(quantile(AGE_YEARS, 0.75, na.rm = TRUE), 1),
      Min     = round(min(AGE_YEARS, na.rm = TRUE), 1),
      Max     = round(max(AGE_YEARS, na.rm = TRUE), 1),
      .groups = "drop"
    )
  cat("Estatísticas da Idade Materna por Período:\n")
  print(stats_periodo)
  salvar_tabela_xlsx(stats_periodo, "tabela_idade_stats_por_periodo", "Idade por Período")
  
  plot_idade_periodo_scaled <- ggplot(df_periodo,
                                      aes(x = Periodo,
                                          y = AGE_YEARS,
                                          fill = Periodo)) +
    geom_violin(alpha = 0.6,
                trim = FALSE,
                draw_quantiles = c(0.25, 0.5, 0.75),
                scale = "count") +
    scale_fill_viridis_d(option = "plasma", guide = "none") +
    scale_y_continuous(breaks = seq(0, 55, 5),
                       limits = c(min(df_periodo$AGE_YEARS, na.rm=TRUE)-2,
                                  max(df_periodo$AGE_YEARS, na.rm=TRUE)+2)) +
    labs(
      title    = "Distribuição da Idade Materna por Período",
      subtitle = paste0(UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM),
      x        = "Período",
      y        = "Idade Materna (anos)"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title      = element_text(hjust = 0.5, face = "bold"),
      axis.text.x     = element_text(angle = 0, hjust = 0.5),
      legend.position = "none"
    )
  
  print(plot_idade_periodo_scaled)
  salvar_grafico_final(plot_idade_periodo_scaled,
                       "plot_distribuicao_idade_por_periodo_scaled",
                       width = 9, height = 6.5)} else {
  cat("-> Sem dados suficientes para Gráfico de Idade por Período.\n")
}
cat("\n")

## 2.5. Gráficos: Raça/Cor e Escolaridade (Barras Horizontais)
cat("3.5 Gráficos — Raça/Cor e Escolaridade...\n")
# Raça/Cor
plot_raca_data <- df_analise %>%
  mutate(CS_RACA = fct_explicit_na(CS_RACA, na_level = "9-Ignorado")) %>% # Garante que NA vire Ignorado
  count(CS_RACA, sort = TRUE) %>% # sort=TRUE para ordenar por frequência
  mutate(
    Porcentagem   = n / sum(n),
    LabelNoPrefix = str_replace(as.character(CS_RACA), "^[0-9]+[\\.-]\\s*", ""),
    LabelNoPrefix = factor(LabelNoPrefix) %>% fct_reorder(n) # Reordenar para plot
  )

cat("Distribuição por Raça/Cor:\n")
print(plot_raca_data %>% select(Raça_Cor = CS_RACA, Label = LabelNoPrefix, Contagem = n, Porcentagem))
salvar_tabela_xlsx(plot_raca_data, "dados_distribuicao_raca", "Raça-Cor")

plot_raca <- ggplot(plot_raca_data, aes(x = n, y = LabelNoPrefix, fill = LabelNoPrefix)) +
  geom_col(show.legend = FALSE, alpha = 0.9) +
  geom_text(
    aes(label = paste0(format(n, big.mark=".", decimal.mark = ","), " (",
                       scales::percent(Porcentagem, accuracy = 0.1,
                                       decimal.mark = ","), ")")),
    hjust = -0.08, size = 3.2, fontface = "bold"
  ) +
  scale_fill_viridis_d(guide = "none") +
  scale_x_continuous(expand = expansion(mult = c(0.01, 0.22))) + # Aumentar expansão para texto
  labs(
    title = "Distribuição por Raça/Cor Autodeclarada",
    subtitle = paste0(UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM),
    x     = "Número de Casos",
    y     = NULL
  ) +
  theme(panel.grid.major.y = element_blank())
print(plot_raca)
salvar_grafico_final(plot_raca, "plot_raca_barras", width = 9, height = 5.5)
cat("\n")

# Escolaridade
niveis_esc_ordem <- c( "0-Analfabeto","1-EF Incompleto(1-4)","2-EF Completo(4)",
                       "3-EF Incompleto(5-8)","4-EF Completo","5-EM Incompleto",
                       "6-EM Completo","7-Superior Incompleto","8-Superior Completo",
                       "9-Ignorado", "10-Não se aplica")

plot_escol_data <- df_analise %>%
  mutate(CS_ESCOL_N = fct_explicit_na(CS_ESCOL_N, na_level = "9-Ignorado")) %>%
  # filter(CS_ESCOL_N != "10-Não se aplica") # Decidir se mantém ou remove "Não se aplica"
  count(CS_ESCOL_N) %>%
  mutate(
    Porcentagem       = n / sum(n),
    LabelNoPrefix     = str_replace(as.character(CS_ESCOL_N), "^[0-9]+[\\.-]\\s*", ""),
    # Assegurar a ordem correta dos níveis de escolaridade
    CS_ESCOL_N_ORD    = factor(CS_ESCOL_N, levels = intersect(niveis_esc_ordem, levels(CS_ESCOL_N)), ordered = TRUE),
    LabelNoPrefix_ORD = factor(LabelNoPrefix, 
                               levels = str_replace(intersect(niveis_esc_ordem, levels(CS_ESCOL_N)), "^[0-9]+[\\.-]\\s*", ""),
                               ordered = TRUE)
  ) %>% 
  arrange(CS_ESCOL_N_ORD) # Ordenar pelos níveis definidos

cat("Distribuição por Escolaridade:\n")
print(plot_escol_data %>% select(Escolaridade = CS_ESCOL_N, Label = LabelNoPrefix, Contagem = n, Porcentagem))
salvar_tabela_xlsx(plot_escol_data, "dados_distribuicao_escolaridade", "Escolaridade")

plot_escolaridade <- ggplot(plot_escol_data, aes(x = n, y = fct_rev(LabelNoPrefix_ORD), fill = Porcentagem)) + # fct_rev para ordem desejada
  geom_col(alpha = 0.9) +
  geom_text(
    aes(label = paste0(format(n, big.mark="."), " (", scales::percent(Porcentagem, accuracy = 0.1, decimal.mark = ","), ")")),
    hjust = -0.08, size = 3.2, fontface = "bold"
  ) +
  scale_fill_viridis_c(option = "cividis", name = "% Casos", labels = label_percent(decimal.mark = ",")) +
  scale_x_continuous(expand = expansion(mult = c(0.01, 0.22))) +
  labs(
    title = "Distribuição por Nível de Escolaridade",
    subtitle = paste0(UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM),
    x     = "Número de Casos",
    y     = NULL
  ) +
  theme(
    legend.position = "right", legend.key.height = unit(0.8, "cm"),
    panel.grid.major.y = element_blank()
  )
print(plot_escolaridade)
salvar_grafico_final(plot_escolaridade, "plot_escolaridade_barras", width = 10, height = 7)
cat("\n")

## 2.6. Distribuição temporal dos casos (Gráfico de Linha)
cat("3.6 Gráfico — Tendência Temporal dos Casos Notificados...\n")
casos_por_ano <- df_analise %>%
  group_by(ANO_SGB) %>%
  summarise(N_Casos = n(), .groups = "drop") %>%
  complete(ANO_SGB = ANO_INICIO:ANO_FIM, fill = list(N_Casos = 0)) # Garante todos os anos no eixo

plot_tendencia <- ggplot(casos_por_ano, aes(x = ANO_SGB, y = N_Casos)) +
  geom_line(color = cores_principais[3], linewidth = 1.2, alpha = 0.8) +
  geom_point(color = cores_principais[3], size = 3.5, shape = 21, fill = "white", stroke = 1.3) +
  geom_text_repel(aes(label = N_Casos), 
                  size = 3.5, fontface = "bold", 
                  nudge_y = max(casos_por_ano$N_Casos)*0.05, # Ajuste para não sobrepor
                  min.segment.length = 0) + 
  scale_x_continuous(breaks = ANO_INICIO:ANO_FIM) +
  scale_y_continuous(
    limits = c(0, max(casos_por_ano$N_Casos, na.rm=T) * 1.20), # Aumentar limite superior
    labels = label_number(big.mark = ".")
  ) +
  labs(
    title    = "Tendência Temporal dos Casos de Sífilis Gestacional Notificados",
    subtitle = paste0(UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM),
    x        = "Ano da Notificação",
    y        = "Número de Casos Notificados"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1))
print(plot_tendencia)
salvar_grafico_final(plot_tendencia, "plot_tendencia_temporal_casos")
salvar_tabela_xlsx(casos_por_ano, "dados_tendencia_temporal_casos", "Casos por Ano")
cat("\n")

## 2.7. Distribuição espacial dos casos (Mapa por Município de Residência)
cat("3.7 Mapa — Distribuição dos Casos por Município de Residência...\n")
shape_al <- NULL
tryCatch({
  cat("-> Baixando shapefile para Alagoas (UF_CODIGO:", UF_CODIGO, ")...\n")
  shape_al <- read_municipality(code_muni = as.numeric(UF_CODIGO), year = 2020) # geobr espera code_muni numérico para UF
  cat("-> Shapefile carregado.\n")
}, error = function(e) {
  message("!! Aviso: Falha ao baixar/carregar shapefile para Alagoas: ", e$message)
  message("-> Certifique-se que o pacote 'geobr' está instalado e há conexão com a internet.")
})

if (!is.null(shape_al) && nrow(shape_al) > 0) {
  casos_por_muni <- df_analise %>%
    filter(!is.na(ID_MN_RESI) & str_length(ID_MN_RESI) == 7) %>% # ID_MN_RESI é o código do município
    group_by(code_muni = as.character(ID_MN_RESI)) %>% # geobr usa 'code_muni'
    summarise(N_Casos = n(), .groups = "drop")
  
  # Certificar que code_muni em shape_al é character para o join
  mapa_dados <- shape_al %>%
    mutate(code_muni = as.character(code_muni)) %>%
    left_join(casos_por_muni, by = "code_muni") %>%
    mutate(N_Casos = replace_na(N_Casos, 0))
  
  # Adicionar centróides para rótulos dos municípios com mais casos (opcional)
  # centroids <- st_centroid(mapa_dados) %>%
  #   filter(N_Casos > quantile(mapa_dados$N_Casos, 0.9, na.rm=TRUE)) # Ex: Top 10%

  # 1a) Recalcule as estatísticas que entram na legenda
  library(scales)
  
  # recalcula estatísticas só para título
  avg_cases    <- mean(mapa_dados$N_Casos, na.rm = TRUE)
  median_cases <- median(mapa_dados$N_Casos, na.rm = TRUE)
  max_cases    <- max(mapa_dados$N_Casos, na.rm = TRUE)
  
  plot_mapa_log <- ggplot(mapa_dados) +
    geom_sf(aes(fill = N_Casos), color = "gray50", size = 0.15) +
    scale_fill_viridis_c(
      option = "plasma",
      trans  = "log10",
      # quebrar em potências de 10
      breaks    = c(1, 10, 100, 500, 2000),
      # rótulos math format 10^n
      labels    = label_number(big.mark = ".", decimal.mark = ","),
      na.value  = "grey80",
      guide  = guide_colorbar(
        direction     = "horizontal",
        barwidth      = unit(8, "cm"),
        barheight     = unit(0.4, "cm"),
        title.position= "top",
        label.position= "bottom",
        ticks         = FALSE
      ),
      name = "Casos (log10)"
    ) +
    labs(
      title    = "Distribuição Espacial Acumulada dos Casos por Município",
      subtitle = sprintf(
        "%s, %d–%d  •  Média: %d  •  Mediana: %d  •  Máx: %d",
        UF_ESTUDO, ANO_INICIO, ANO_FIM,
        round(avg_cases), round(median_cases), round(max_cases)
      ),
      caption  = "Fonte: SINAN / geobr"
    ) +
    theme_void(base_size = 12) +
    theme(
      plot.title      = element_text(hjust = 0.5, face = "bold"),
      plot.subtitle   = element_text(hjust = 0.5),
      legend.position = "bottom",
      legend.title    = element_text(face = "bold"),
      legend.text     = element_text(size = rel(0.8))
    )
  
  print(plot_mapa_log)
  salvar_grafico_final(plot_mapa_log, "mapa_casos_log10_legend", width = 8, height = 6)
  
  
  
  tabela_mapa_dados <- mapa_dados %>%
    sf::st_drop_geometry() %>% # Remover geometria para salvar como tabela
    select(Nome_Municipio = name_muni, Codigo_IBGE = code_muni, N_Casos) %>%
    arrange(desc(N_Casos))
  salvar_tabela_xlsx(tabela_mapa_dados, "dados_mapa_casos_municipio_residencia", "Casos por Município")
} else {
  cat("-> Mapa não gerado: shapefile de Alagoas indisponível ou vazio.\n")
}
cat("\n")

## 2.8. Análise do Título do Teste Não Treponêmico (DSTITULO1)
cat("3.8 Análise do Título do Teste Não Treponêmico (DSTITULO1)...\n")
if ("DSTITULO1_numeric" %in% names(df_analise) && sum(!is.na(df_analise$DSTITULO1_numeric)) > 0) {
  
  summary_dstitulo1 <- df_analise %>%
    filter(!is.na(DSTITULO1_numeric)) %>%
    summarise(
      N_validos = n(),
      Mediana = median(DSTITULO1_numeric, na.rm = TRUE),
      Media   = mean(DSTITULO1_numeric, na.rm = TRUE),
      Min     = min(DSTITULO1_numeric, na.rm = TRUE),
      Max     = max(DSTITULO1_numeric, na.rm = TRUE),
      Pct_Alto_Titulo_gte16 = mean(DSTITULO1_numeric >= 16, na.rm = TRUE)
    )
  cat("Resumo dos títulos (DSTITULO1):\n")
  print(summary_dstitulo1)
  salvar_tabela_xlsx(summary_dstitulo1, "resumo_dstitulo1", "Resumo DSTITULO1")

  # Boxplot de DSTITULO1 (log2) por Classificação Clínica
  cutoff <- 64
  
  plot_dstitulo1_classif <- df_analise %>%
    filter(!is.na(DSTITULO1_log2),
           !is.na(TPEVIDENCI),
           TPEVIDENCI != "9-Ignorado",
           DSTITULO1_numeric <= cutoff) %>%    # <— drop outliers
    mutate(TPEVIDENCI_Label = str_remove(as.character(TPEVIDENCI),
                                         "^[0-9]+[\\.-]\\s*")) %>%
    ggplot(aes(x = TPEVIDENCI_Label, y = DSTITULO1_log2, fill = TPEVIDENCI_Label)) +
    geom_boxplot(outlier.alpha = 0.5, show.legend = FALSE) +
    scale_y_continuous(breaks = seq(0, log2(cutoff), 1),
                       labels = function(x) paste0("1:", 2^x)) +
    scale_fill_viridis_d(option = "mako", guide = "none") +
    labs(
      title    = "Título Não Treponêmico (Log2) por Classificação Clínica",
      subtitle = paste0(UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM),
      x        = "Classificação Clínica",
      y        = "Título (VDRL/RPR)"
    ) +
    theme(axis.text.x = element_text(angle = 25, hjust = 1))
  print(plot_dstitulo1_classif)
  salvar_grafico_final(plot_dstitulo1_classif, "plot_dstitulo1_por_classificacao", width = 8, height = 6.5)
} else {
  cat("-> Sem dados válidos de DSTITULO1_numeric para análise.\n")
}
cat("\n")


# --- 3. AVALIAÇÃO DO DIAGNÓSTICO E TRATAMENTO ---
cat("4. AVALIAÇÃO DO DIAGNÓSTICO E TRATAMENTO\n")

## 3.1 Diagnóstico laboratorial (Tabela gtsummary)
cat("4.1 Tabela — Resultados dos Testes Diagnósticos (Simplificados)...\n")
if (all(c("TPTESTE1_Simple", "TPCONFIRMA_Simple") %in% names(df_analise))) {
  tbl_testes <- df_analise %>%
    select(TPTESTE1_Simple, TPCONFIRMA_Simple) %>%
    tbl_summary(missing_text="(NA/Ausente)") %>%
    modify_header(label ~ "**Tipo de Teste**") %>%
    bold_labels() %>%
    modify_footnote(update = everything() ~ NA) %>%
    modify_caption(
      paste0(
        "**Tabela 2: Resultados dos Testes Diagnósticos (N = ",
        format(TOTAL_CASOS, big.mark=".", decimal.mark=","),  
        ")**"
      )
    )
  
  print(as_tibble(tbl_testes, col_labels = TRUE))
  salvar_tabela_xlsx(tbl_testes, "tabela_testes_diagnosticos", "Testes Diagnósticos")
} else {
  cat("-> Colunas de testes simplificadas (TPTESTE1_Simple, TPCONFIRMA_Simple) não encontradas. Tabela não gerada.\n")
}
cat("\n")

## 3.2 Adequação do tratamento da gestante
cat("4.2 Criando Variáveis de Adequação do Tratamento da Gestante...\n")
df_analise <- df_analise %>%
  mutate(
    TRAT_GESTANTE_ADEQUADO = case_when(
      # Casos onde o tratamento não pode ser avaliado ou é claramente inadequado/ignorado
      is.na(TPESQUEMA) | TPESQUEMA == "9-Ignorado" ~ "Trat. Esquema Ignorado",
      TPESQUEMA == "5-Não realizado" ~ "Não Tratada",
      is.na(TPEVIDENCI) | TPEVIDENCI == "9-Ignorado" ~ "Class. Clínica Ignorada", # Se a classificacao é ignorada, não podemos julgar adequacao do esquema
      TPESQUEMA == "4-Outro esquema" ~ "Inadequado (Outro Esquema)",

      # Sífilis Primária, Secundária, Latente (Recente ou Indeterminada com esquema para recente)
      TPEVIDENCI %in% c("1-Primária", "2-Secundária", "4-Latente") & TPESQUEMA == "1-Pen G benz 2.400.000UI" ~ "Adequado",
      TPEVIDENCI %in% c("1-Primária", "2-Secundária", "4-Latente") & TPESQUEMA != "1-Pen G benz 2.400.000UI" ~ "Inadequado (Esquema Divergente)",
      
      # Sífilis Terciária (ou Latente Tardia com esquema para terciária)
      TPEVIDENCI == "3-Terciária" & TPESQUEMA == "3-Pen G benz 7.200.000UI" ~ "Adequado",
      TPEVIDENCI == "3-Terciária" & TPESQUEMA != "3-Pen G benz 7.200.000UI" ~ "Inadequado (Esquema Divergente)",
      
      TRUE ~ "Inconclusivo/Verificar" # Casos não cobertos, ex: Latente com esquema 4.800.000
    ),
    # Variável binária para regressão
    TRAT_ADEQUADO_BIN = case_when(
      TRAT_GESTANTE_ADEQUADO == "Adequado" ~ "Adequado",
      TRAT_GESTANTE_ADEQUADO %in% c("Não Tratada", "Inadequado (Outro Esquema)", 
                                    "Inadequado (Esquema Divergente)") ~ "Inadequado/Não Tratado",
      TRUE ~ NA_character_ # Casos ignorados, inconclusivos, ou com info faltando para a variável binária
    )
  ) %>%
  mutate(
    TRAT_GESTANTE_ADEQUADO = factor(TRAT_GESTANTE_ADEQUADO,
                                    levels = c("Adequado", "Inadequado (Esquema Divergente)", 
                                               "Inadequado (Outro Esquema)", "Não Tratada", 
                                               "Trat. Esquema Ignorado", "Class. Clínica Ignorada", 
                                               "Inconclusivo/Verificar")),
    TRAT_ADEQUADO_BIN = factor(TRAT_ADEQUADO_BIN, levels = c("Adequado", "Inadequado/Não Tratado"))
  )
df_analise <- df_analise %>% 
  set_variable_labels(TRAT_GESTANTE_ADEQUADO = "Adequação do Tratamento da Gestante (Detalhado)",
                      TRAT_ADEQUADO_BIN = "Adequação do Tratamento da Gestante (Binário)")
cat("-> Variáveis de adequação do tratamento da gestante criadas.\n\n")

cat("4.3 Gráfico — Adequação do Tratamento da Gestante...\n")
dados_trat_gestante <- df_analise %>%
  filter(!is.na(TRAT_GESTANTE_ADEQUADO)) %>%
  count(TRAT_GESTANTE_ADEQUADO, sort = TRUE) %>%
  mutate(
    Porcentagem = n / sum(n),
    TRAT_GESTANTE_ADEQUADO_ORD = fct_reorder(TRAT_GESTANTE_ADEQUADO, n) # Ordena por frequência
  )

plot_trat_gestante <- ggplot(dados_trat_gestante,
                             aes(x = n, y = TRAT_GESTANTE_ADEQUADO_ORD, fill = TRAT_GESTANTE_ADEQUADO_ORD)) +
  geom_col(show.legend = FALSE, alpha = 0.9) +
  geom_text(aes(label = paste0(format(n, big.mark="."), "\n(",
                               scales::percent(Porcentagem, accuracy = 0.1, decimal.mark = ","), ")")),
            hjust = -0.08, size = 3.0, lineheight = 0.9, fontface = "bold") +
  scale_x_continuous(expand = expansion(mult = c(0.01, 0.25))) +
  scale_fill_viridis_d(option = "rocket", guide = "none") +
  labs(
    title    = "Adequação do Tratamento da Gestante",
    subtitle = paste0("Baseado na classificação clínica vs. esquema terapêutico. ", UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM),
    x        = "Número de Casos",
    y        = NULL
  ) +
  theme(axis.text.y = element_text(size = rel(0.85)), panel.grid.major.y = element_blank())
print(plot_trat_gestante)
salvar_grafico_final(plot_trat_gestante, "plot_adequacao_tratamento_gestante", width = 10, height = 7)
salvar_tabela_xlsx(dados_trat_gestante, "dados_adequacao_tratamento_gestante", "Adequação Trat. Gestante")
cat("\n")

## 3.3 Tratamento do Parceiro
cat("4.4 Gráfico — Tratamento Concomitante do Parceiro...\n")
dados_parceiro <- df_analise %>%
  mutate(TRATPARC = fct_explicit_na(TRATPARC, na_level = "9-Ignorado")) %>% # NAs viram "9-Ignorado"
  count(TRATPARC, sort = TRUE) %>%
  mutate(
    Porcentagem = n / sum(n),
    LabelTxt    = str_replace(as.character(TRATPARC), "^[0-9]+[\\.-]\\s*", ""),
    Label       = paste0(LabelTxt, "\n(", format(n, big.mark="."), " / ",
                         scales::percent(Porcentagem, accuracy = 0.1, decimal.mark = ","), ")"),
    TRATPARC_ORD = fct_reorder(TRATPARC, n) # Ordena por frequência
  )

plot_parc <- ggplot(dados_parceiro, aes(x = n, y = TRATPARC_ORD, fill = TRATPARC_ORD)) +
  geom_col(show.legend = FALSE, alpha = 0.9) +
  geom_text(aes(label = Label),
            hjust = -0.08, size = 3.0, lineheight = 0.9, fontface = "bold") +
  scale_x_continuous(expand = expansion(mult = c(0.01, 0.30))) +
  scale_fill_viridis_d(option = "cubehelix", guide = "none") +
  scale_y_discrete(labels = ~str_replace(., "^[0-9]+[\\.-]\\s*", "")) + # Limpa labels do eixo Y
  labs(
    title = "Tratamento Concomitante do Parceiro Sexual",
    subtitle = paste0(UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM),
    x     = "Número de Casos",
    y     = "Parceiro Tratado?"
  ) +
  theme(panel.grid.major.y = element_blank())
print(plot_parc)
salvar_grafico_final(plot_parc, "plot_tratamento_parceiro", width=9, height=5)
salvar_tabela_xlsx(dados_parceiro %>% select(-Label, -LabelTxt), "dados_tratamento_parceiro", "Tratamento Parceiro")
cat("\n")





# --- 5. EPIDEMIOLOGIA ANALÍTICA: REGRESSÃO LOGÍSTICA MULTIVARIADA ---
cat("5. EPIDEMIOLOGIA ANALÍTICA: REGRESSÃO LOGÍSTICA MULTIVARIADA\n")

## 5.1. Preparação dos dados para regressão (Fatores associados ao Tratamento Inadequado)
cat("5.1 Preparando dados para regressão...\n")
df_modelo <- tryCatch({
  if (!"TRAT_ADEQUADO_BIN" %in% names(df_analise)) {
    stop("Variável dependente 'TRAT_ADEQUADO_BIN' não foi criada corretamente.")
  }
  df_analise %>%
    filter(!is.na(TRAT_ADEQUADO_BIN)) %>%
    mutate(
      Faixa_Etaria_Reg   = factor(Faixa_Etaria_Cat),
      Escolaridade_Reg   = case_when(
        is.na(CS_ESCOL_N) | CS_ESCOL_N %in% c("9-Ignorado","10-Não se aplica") ~ "Ignorada/Não Aplic.",
        CS_ESCOL_N %in% c("0-Analfabeto","1-EF Incompleto(1-4)","2-EF Completo(4)","3-EF Incompleto(5-8)") ~ "Ens. Fund. Incompleto ou menos",
        CS_ESCOL_N %in% c("4-EF Completo","5-EM Incompleto") ~ "Ens. Fund. Completo / Médio Incompleto",
        CS_ESCOL_N %in% c("6-EM Completo","7-Superior Incompleto","8-Superior Completo") ~ "Ens. Médio Completo ou mais",
        TRUE ~ "Outra/Verificar"
      ) %>% factor(levels = c(
        "Ens. Médio Completo ou mais",
        "Ens. Fund. Completo / Médio Incompleto",
        "Ens. Fund. Incompleto ou menos",
        "Ignorada/Não Aplic."
      )),
      Raca_Cor_Reg       = case_when(
        CS_RACA=="1-Branca"      ~ "Branca",
        CS_RACA=="4-Parda"       ~ "Parda",
        CS_RACA=="2-Preta"       ~ "Preta",
        CS_RACA %in% c("3-Amarela","5-Indígena") ~ "Amarela/Indígena",
        is.na(CS_RACA) | CS_RACA=="9-Ignorado"   ~ "Ignorada",
        TRUE ~ "Outra/Verificar"
      ) %>% factor(levels = c("Branca","Parda","Preta","Amarela/Indígena","Ignorada")),
      Class_Clinica_Reg  = case_when(
        TPEVIDENCI %in% c("1-Primária","2-Secundária") ~ "Primária/Secundária",
        TPEVIDENCI=="4-Latente"                        ~ "Latente",
        TPEVIDENCI=="3-Terciária"                      ~ "Terciária",
        TRUE ~ "Ignorada"
      ) %>% factor(levels = c("Primária/Secundária","Latente","Terciária","Ignorada")),
      Idade_Gest_Reg     = case_when(
        CS_GESTANT=="1-1ºTrimestre" ~ "1º Trimestre",
        CS_GESTANT=="2-2ºTrimestre" ~ "2º Trimestre",
        CS_GESTANT=="3-3ºTrimestre" ~ "3º Trimestre",
        TRUE                         ~ "Ignorada/Outra"
      ) %>% factor(levels = c("1º Trimestre","2º Trimestre","3º Trimestre","Ignorada/Outra")),
      Periodo_Not_Reg    = case_when(
        ANO_SGB %in% 2010:2014 ~ "2010-2014",
        ANO_SGB %in% 2015:2018 ~ "2015-2018",
        ANO_SGB %in% 2019:2022 ~ "2019-2022",
        TRUE                   ~ NA_character_
      ) %>% factor(levels = c("2010-2014","2015-2018","2019-2022"))
    ) %>%
    select(TRAT_ADEQUADO_BIN, Faixa_Etaria_Reg, Raca_Cor_Reg,
           Escolaridade_Reg, Class_Clinica_Reg,
           Idade_Gest_Reg, Periodo_Not_Reg) %>%
    filter(
      !if_any(everything(), ~ .=="Outra/Verificar"),
      Escolaridade_Reg!="Ignorada/Não Aplic.",
      Raca_Cor_Reg!="Ignorada",
      Class_Clinica_Reg!="Ignorada",
      Idade_Gest_Reg!="Ignorada/Outra"
    ) %>%
    drop_na() %>%
    { 
      if ("Adequado" %in% levels(.$TRAT_ADEQUADO_BIN)) 
        mutate(., TRAT_ADEQUADO_BIN = relevel(TRAT_ADEQUADO_BIN, ref="Adequado")) 
      else . 
    }
}, error = function(e) {
  cat("!! ERRO CRÍTICO ao preparar dados para regressão !!\n", e$message, "\n")
  NULL
})

if (is.null(df_modelo) || nrow(df_modelo)==0) {
  cat("!! Dados insuficientes para regressão. Abortando seção.\n")
} else {
  
  explanatory_vars_model <- c(
    "Faixa_Etaria_Reg","Raca_Cor_Reg","Escolaridade_Reg",
    "Class_Clinica_Reg","Idade_Gest_Reg","Periodo_Not_Reg"
  )
  # EPV check
  min_evt <- min(table(df_modelo$TRAT_ADEQUADO_BIN), na.rm=TRUE)
  epv     <- min_evt/length(explanatory_vars_model)
  cat("-> Número mínimo de eventos:", min_evt, " | EPV:", sprintf("%.1f",epv), "\n")
  
  ## 5.2. Ajuste do modelo de regressão logística
  cat("5.2 Ajustando modelo de regressão logística para 'TRAT_ADEQUADO_BIN'...\n")
  formula_str <- paste("TRAT_ADEQUADO_BIN ~", paste(explanatory_vars_model, collapse=" + "))
  modelo_final_glm <- glm(as.formula(formula_str), data=df_modelo, family=binomial(link="logit"))
  cat("-> Modelo ajustado com sucesso.\n\n")
  
  ## 5.3. Tabela — Regressão Logística Multivariada
  cat("5.3 Tabela — Regressão Logística Multivariada (OR Ajustados)\n")
  model_labels_list <- list(
    Faixa_Etaria_Reg   ~ "Faixa Etária Materna",
    Raca_Cor_Reg       ~ "Raça/Cor Autodeclarada",
    Escolaridade_Reg   ~ "Nível de Escolaridade",
    Class_Clinica_Reg  ~ "Classificação Clínica da Sífilis",
    Idade_Gest_Reg     ~ "Idade Gestacional no Diagnóstico",
    Periodo_Not_Reg    ~ "Período da Notificação"
  )
  tbl_reg <- tbl_regression(
    modelo_final_glm,
    exponentiate = TRUE,
    label        = model_labels_list
  ) %>%
    bold_labels() %>%
    bold_p(t = 0.05) %>%
    modify_fmt_fun(
      update = c(estimate, conf.low, conf.high) ~ function(x) style_number(x, digits=2, decimal.mark=",")
    ) %>%
    modify_column_merge(
      pattern = "{estimate} ({conf.low} – {conf.high})",
      rows    = !is.na(estimate)
    ) %>%
    modify_header(
      label    ~ "**Variável**",
      estimate ~ "**OR (IC 95%)**"
    ) %>%
    modify_footnote(update = everything() ~ NA) %>%
    modify_caption(paste0(
      "**Tabela 3: Fatores Associados ao Tratamento Inadequado/Não Tratado (vs. Adequado)**\n",
      UF_ESTUDO, ", ", ANO_INICIO, "–", ANO_FIM,
      ". OR: Odds Ratio Ajustado; IC 95%: Intervalo de Confiança de 95%."
    )) %>%
    add_global_p(keep = TRUE)
  
  print(tbl_reg %>% as_tibble(col_labels = TRUE))
  salvar_tabela_xlsx(tbl_reg,
                     "tabela_regressao_logistica_trat_inadequado",
                     "Regressão Logística")
  cat("\n")
  
  ## 5.4. Gráfico — Forest Plot dos Resultados da Regressão
  cat("5.4 Gráfico — Forest Plot dos Resultados da Regressão...\n")
  plot_forest <- NULL
  tryCatch({
    # broom.helpers approach
    var_labels_vector <- c(
      Faixa_Etaria_Reg  = "Faixa Etária Materna",
      Raca_Cor_Reg      = "Raça/Cor Autodeclarada",
      Escolaridade_Reg  = "Nível de Escolaridade",
      Class_Clinica_Reg = "Classificação Clínica da Sífilis",
      Idade_Gest_Reg    = "Idade Gestacional no Diagnóstico",
      Periodo_Not_Reg   = "Período da Notificação"
    )
    
    tidy_data <- broom.helpers::tidy_plus_plus(
      model              = modelo_final_glm,
      exponentiate       = TRUE,
      conf.int           = TRUE,
      intercept          = FALSE,
      add_reference_rows = TRUE,
      variable_labels    = var_labels_vector
    )
    
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
    
    # dynamic x‐axis breaks
    min_ci <- min(c(0.05, plot_df$conf.low), na.rm = TRUE)
    max_ci <- max(c(8,    plot_df$conf.high), na.rm = TRUE)
    if (min_ci <= 0)      min_ci <- 0.05
    if (max_ci <= min_ci) max_ci <- max(8, min_ci * 2)
    breaks   <- c(0.1,0.25,0.5,1,2,4,8,16)
    x_breaks <- breaks[breaks >= min_ci*0.8 & breaks <= max_ci*1.2]
    if (length(x_breaks) < 3) x_breaks <- c(0.25,0.5,1,2,4)
    
    # plot
    plot_forest <- ggplot(plot_df, aes(x = estimate, y = fct_rev(y_axis_level))) +
      geom_vline(xintercept = 1, linetype="dashed", color="grey70", linewidth=0.6) +
      geom_errorbarh(aes(xmin = conf.low, xmax = conf.high, color = significance_group),
                     height=0.2, linewidth=0.7, na.rm=TRUE) +
      geom_point(aes(fill = significance_group, shape = reference_row),
                 size=2.8, color="black", stroke=0.4) +
      
      scale_x_log10(
        breaks = x_breaks,
        limits = c(min_ci*0.8, max_ci*1.2),
        labels = label_number(accuracy=0.1, decimal.mark=",")
      ) +
      scale_y_discrete(
        position = "right",
        labels = function(id) sapply(strsplit(id, "_"), `[`, 2)
      ) +
      
      # split legends
      scale_color_manual(
        name         = "Significância",
        values       = c("p < 0,05" = cores_principais[1],
                         "p ≥ 0,05" = "grey30"),
        na.translate = FALSE
      ) +
      scale_fill_manual(
        values = c("p < 0,05" = cores_principais[1],
                   "p ≥ 0,05" = "white"),
        guide  = "none"
      ) +
      scale_shape_manual(
        name         = "Tipo",
        values       = c(`FALSE` = 21, `TRUE` = 22),
        labels       = c("Comparação","Referência"),
        na.translate = FALSE
      ) +
      guides(
        shape = guide_legend(order = 1),
        color = guide_legend(order = 2)
      ) +
      
      facet_grid(var_label ~ ., scales = "free_y", space = "free_y", switch = "y") +
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
        legend.position    = "top",
        legend.box         = "horizontal",
        legend.title       = element_text(face="bold"),
        legend.text        = element_text(size=rel(0.85)),
        plot.title         = element_text(hjust=0.5, face="bold"),
        plot.subtitle      = element_text(hjust=0.5, margin=margin(b=6)),
        plot.caption       = element_text(hjust=0.5, face="italic", size=rel(0.8), margin=margin(t=6))
      )
    
    print(plot_forest)
    salvar_grafico_final(
      plot_forest,
      "plot_forest_trat_inadequado_broomhelpers",
      width = 10, height = 12
    )
    
  }, error = function(e) {
    cat("!! ERRO ao gerar Forest Plot !!\n", e$message, "\n")
    if (exists("tidy_data")) str(tidy_data)
    if (exists("plot_df"))    str(plot_df)
  })
  
}  # fim do if(df_modelo)
cat("\n")


# --- 5. FINALIZAÇÃO ---
cat("--- ANÁLISE CONCLUÍDA ---\n")
cat("Todos os resultados, gráficos e tabelas foram salvos em:", dir_relatorio, "\n")
cat("Data e Hora da Conclusão:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")

# Fim do Script