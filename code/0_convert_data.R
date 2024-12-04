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

files <- list.files('Deserción Escolar/data/SIMAT/Matricula validada 2017-2023',
                    full.names = T) %>% str_remove("\\~\\$")

SIMAT_sheets <- list()
for (file in files) {
  SIMAT_sheets[[basename(file)]] <- excel_sheets(file)
}

new_folder <- file.path(ROOT_FOLDER, 'data/raw/SIMAT_2017_2023')
create_folder(new_folder)


for (file in files) {
  if (grepl("\\.xlsx$", file)) { 
    for (file_sheet in excel_sheets(file)) {
      message(sprintf("Procesando archivo Excel: %s, hoja: %s", basename(file), file_sheet))
       new_file <- sprintf("%s_%s.parquet", 
       str_remove(basename(file), "\\..*"), file_sheet)
      read_excel(file, sheet = file_sheet) %>% 
        write_parquet(file.path(new_folder, new_file))
    }
  } else if (grepl("\\.csv$", file)) {  # Si es un archivo CSV
    message(sprintf("Procesando archivo CSV: %s", basename(file)))
    new_file <- sprintf("%s.parquet", str_remove(basename(file), "\\..*"))
    read_csv(file) %>% 
      write_parquet(file.path(new_folder, new_file))
  }
}


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

df <- read_excel(file.path(DICTS_FOLDER, 'raw_SIMAT.xlsx'), sheet = "Clasificacion")

base_files <- df_clean %>% filter(Clasificacion == "Base") %>% pull(File)
complementary_files <- df_clean %>% filter(Clasificacion == "Complementario") %>% pull(File)
trash_files <- df_clean %>% filter(Clasificacion == "Basura") %>% pull(File)

files_directory <- file.path(FOLDER_SIMAT)
list.files(files_directory)





# TAREA. Completar edicion de diccionarios

get_dicts("Deserción Escolar/school_dropout/data/metadata/dicts/raw_SIMAT.xlsx")

get_dicts(raw_dict_path) %>% 
  unify_columns("NOMBRE1", "AL_PRIM_NOMB") %>% 
  unify_columns("NOMBRE1", "NAME1") %>% 
  unify_columns("NOMBRE2", "AL_SEGU_NOMB") %>% 
  unify_columns("APELLIDO1", "AL_PRIM_APEL") %>% 
  unify_columns("APELLIDO2", "AL_SEGU_APEL") %>% 
  unify_columns("TIPO_DOCUMENTO", "TDOC") %>% 
  unify_columns("TIPO_DOCUMENTO", "TI_CODI_ID") %>% 
  unify_columns("TIPO_DOCUMENTO", "Tdoc") %>% 
  unify_columns("TIPO_DOCUMENTO", "tipo_dcto") %>% 
  unify_columns("TIPO_DOCUMENTO", "td") %>% 
  unify_columns("NRO_DOCUMENTO", "Ndoc") %>% 
  unify_columns("NRO_DOCUMENTO", "AL_NUME_ID") %>% 
  unify_columns("NRO_DOCUMENTO", "nro_docum") %>% 
  unify_columns("NRO_DOCUMENTO", "nro_dcto") %>% 
  unify_columns("DIRECCION_RESIDENCIA", "Dir_res") %>% 
  unify_columns("DIRECCION_RESIDENCIA", "direc_residencia") %>% 
  unify_columns("DIRECCION_RESIDENCIA", "DIRECCION_RESIDENCIA  Homologada") %>% 
  unify_columns("DIRECCION_RESIDENCIA", "DirAcud") %>% 
  unify_columns("TEL", "Tel_res") %>% 
  unify_columns("TEL", "AL_TELE_RESI") %>% 
  unify_columns("TEL", "telefono") %>% 
  unify_columns("TEL", "tel_ubicac") %>% 
  unify_columns("TEL_ACUDIENTE", "TelAcud") %>% 
  unify_columns("TEL_ACUDIENTE", "AL_TELE_RESI_ACU") %>% 
  unify_columns("TEL_PADRE", "AL_PADR_TEL") %>% 
  unify_columns("TEL_MADRE", "AL_MADR_TEL") %>% 
  unify_columns("TIPO_DOCUMENTO_ACUDIENTE", "NdocAcud") %>% 
  unify_columns("NRO_DOCUMENTO_ACUDIENTE", "AL_CEDU_ACUD") %>% 
  unify_columns("NOMBRE1_ACUDIENTE", "Nomb1 Acud") %>% 
  unify_columns("NOMBRE1_ACUDIENTE", "AL_NOMB_ACUD") %>% 
  unify_columns("NOMBRE2_ACUDIENTE", "Nomb2 Acud") %>% 
  unify_columns("NOMBRE_MADRE", "AL_MADR_NOMB") %>% 
  unify_columns("NRO_DOCUMENTO_MADRE", "AL_MADR_ID") %>% 
  unify_columns("NOMBRE_PADRE", "AL_PADR_NOMB") %>% 
  unify_columns("NRO_DOCUMENTO_PADRE", "AL_PADR_ID") %>% 
  delete_columns(c("AL_PRIM_NOMB", "NAME1", "AL_SEGU_NOMB", "AL_PRIM_APEL", "AL_SEGU_APEL",
                   "TDOC", "TI_CODI_ID", "Tdoc", "tipo_dcto", "td", "Ndoc","AL_NUME_ID", 
                   "nro_docum", "Dir_res", "direc_residencia", "DIRECCION_RESIDENCIA  Homologada", 
                   "DirAcud", "Tel_res", "AL_TELE_RESI", "telefono", "tel_ubicac", "TelAcud",
                   "AL_TELE_RESI_ACU","AL_PADR_TEL", "AL_MADR_TEL", "NdocAcud","AL_CEDU_ACUD", 
                   "Nomb1 Acud", "AL_NOMB_ACUD", "Nomb2 Acud", "AL_MADR_NOMB", "AL_MADR_ID", 
                   "AL_PADR_NOMB", "AL_PADR_ID")) %>% 
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

