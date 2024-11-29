source("_setup.R")


# SIMAT -------------------------------------------------------------------

files <- list.files('Deserción Escolar/data/Matriculas validadas por año',
                    full.names = T) %>% str_remove("\\~\\$")

SIMAT_sheets <- list()
for (file in files) {
  SIMAT_sheets[[basename(file)]] <- excel_sheets(file)
}
