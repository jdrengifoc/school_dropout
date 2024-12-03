source("_setup.R")

create_folder <- function(folders, recursive = T) {
  for (folder in folders) {
    if (!file.exists(folder)) {
      dir.create(folder, recursive = recursive)
      print(paste("created", folder))
    }
  }
}

# SIMAT 2004 - 2022 -------------------------------------------------------------------

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
    message(file)
    new_file <- sprintf("%s_%s.parquet", 
                        str_remove(basename(file), "\\..*"), file_sheet)
    
    read_excel(file, sheet = file_sheet) %>% 
      write_parquet(file.path(new_folder, new_file))
  }
}

files <- list.files(new_folder)

raw_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT.xlsx')
create_partial_dictionary(folder = new_folder, files = files, 
                          dict_path = raw_dict_path, verbose = T, overwrite = F)
sort_partial_dictionary(raw_dict_path, overwrite = T)

# SIMAT 2017-2023------------------

parquet_folder_2017_2023 <- "/Users/mariajoseguerrero/Documents/Secretaria de Educación/Deserción Escolar/data/SIMAT/SIMAT 2017-2023"
output_path <- file.path(DICTS_FOLDER, "Raw_SIMAT_2017_2023.xlsx")

files <- list.files(parquet_folder_2017_2023)

raw_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_2017_2023.xlsx')


create_partial_dictionary(folder = parquet_folder_2017_2023, files = files, 
                          dict_path = raw_dict_path, verbose = T, overwrite = T)
sort_partial_dictionary(raw_dict_path, overwrite = T)













