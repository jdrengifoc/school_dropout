source("_setup.R")

create_folder <- function(folders, recursive = T) {
  for (folder in folders) {
    if (!file.exists(folder)) {
      dir.create(folder, recursive = recursive)
      print(paste("created", folder))
    }
  }
}

# SIMAT -------------------------------------------------------------------

files <- list.files('Deserción Escolar/data/SIMAT/Matriculas validadas por año',
                    full.names = T) %>% str_remove("\\~\\$")

SIMAT_sheets <- list()
for (file in files) {
  SIMAT_sheets[[basename(file)]] <- excel_sheets(file)
}

# Convert xlsx to parquet
new_folder <- file.path(ROOT_FOLDER, 'data/raw/SIMAT')
create_folder(new_folder)
for (file in files) {
  for (file_sheet in excel_sheets(file)) {
    new_file <- sprintf("%s_%s.parquet", 
                        str_remove(basename(file), "\\..*"), file_sheet)
    
    read_excel(file, sheet = file_sheet) %>% 
      write_parquet(file.path(new_folder, new_file))
  }
}