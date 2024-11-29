source("_setup.R")


# SIMAT -------------------------------------------------------------------

files <- list.files('data/Matriculas validadas por año')

SIMAT_sheets <- list()
for (file in files) {
  SIMAT_sheets[[file]] <- excel_sheets(file)
}
