source("_setup_school_dropout.R")

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


### TAREA. INCORPORAR CODIGO DE CONVERSION A PARQUET


files <- list.files(FOLDER_SIMAT_MINISTRY)

raw_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_2017_2023.xlsx')

create_partial_dictionary(folder = FOLDER_SIMAT_MINISTRY, files = files, 
                          dict_path = raw_dict_path, verbose = T, overwrite = T)
sort_partial_dictionary(raw_dict_path, overwrite = T)



# Edit dictionaries -------------------------------------------------------

raw_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT.xlsx')
raw_clean_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_clean.xlsx')


# TAREA. LEER TABLA
# trash_files <- read_excel(...)
# complementary_files <- read_excel(...)
# base_files <- read_excel(...)

# TAREA. Completar edicion de diccionarios
get_dicts(raw_dict_path) %>% 
  unify_columns("NOMBRE1", "AL_PRIM_NOMB") %>% 
  unify_columns("NOMBRE1", "NAME1") %>% 
  delete_columns(c("AL_PRIM_NOMB", "NAME1")) %>% 
  # modify_uniname("OLD_UNINAME", 'NEW_UNINAME') %>%
  # "numeric", "integer" "character", "date", "logical"
  # La menos restrictiva
  # modify_uniclass("NOMBRE1", "character") %>% 
  # modify_uniclass(c("NOMBRE1", "NOMBRE2"), 'character') %>% 
  # delete_files(c(trash_files, complementary_files)) %>% 
  save_dicts(raw_clean_dict_path)





# Unify sensitive information ---------------------------------------------


folder <- file.path(ROOT_FOLDER, 'data/raw/SIMAT')
files <- base_files
dict <- read_excel(raw_clean_dict_path, sheet = 'colname')
SELECTED_COLUMNS <- dict$uniname
new_folder <- file.path(ROOT_FOLDER, 'data/processed/SIMAT/Medellin')

create_folders(new_folder)
for (file in files) {
  print("Begin file")
  new_file <- sprintf("SIMAT_matricula_validada_%s.parquet",
                      str_extract(file, '\\d{4}'))
  arrow::open_dataset(file.path(folder, file)) %>%
    unify_colnames(dict, file, SELECTED_COLUMNS) %>% 
    unify_classes(dict, file, SELECTED_COLUMNS) %>% 
    write_parquet(file.path(new_folder, new_file))
  print("End file")
}

# Crear ids
# Eliminar informacion sensible.

# TAREA. TABLA CON LOS UNINAMES ASOCIADOS A LAS VARIABLES DE INFORMACION SENSIBLE.
# banned_columns <- read_excel()
# arrow::open_dataset(file.path(folder, file)) %>% 
#   select(all_of(banned_columns))

