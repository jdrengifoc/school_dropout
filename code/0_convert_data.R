source("_setup_school_dropout.R")

# SIMAT 2004 - 2022 -------------------------------------------------------------------
files <- list.files('Deserción Escolar/data/SIMAT/Matriculas validadas por año',
                    full.names = T) %>% str_remove("\\~\\$")

SIMAT_sheets <- list()
for (file in files) {
  SIMAT_sheets[[basename(file)]] <- excel_sheets(file)
}

# Convert xlsx to parquet
new_folder <- FOLDER_SIMAT_2004
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


# SIMAT 2017-2023 ---------------------------------------------------------

## TAREA. CORRER y ver que todo salga bien (avisar)
## TAREA. documentar
files <- list.files('Deserción Escolar/data/SIMAT/Matricula validada 2017-2023',
                    full.names = T) %>% str_remove("\\~\\$")

new_folder <- FOLDER_SIMAT_2017
create_folders(new_folder)

for (file in files) {
  message("Procesando archivo: ", basename(file))
  new_file <- sprintf("%s.parquet", str_remove(basename(file), "\\..*"))
  if (grepl("\\.xlsx$", file)) { 
    read_excel(file) %>% 
      write_parquet(file.path(new_folder, new_file))
  } else { 
    read_delim_arrow(file, delim = ";") %>% 
      write_parquet(file.path(new_folder, new_file))
  }
}


files <- list.files(new_folder)
raw_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_2017_2023.xlsx')
create_partial_dictionary(folder = new_folder, files = files, 
                          dict_path = raw_dict_path, verbose = T, overwrite = T)
sort_partial_dictionary(raw_dict_path, overwrite = T)

# Edit dictionaries -------------------------------------------------------

raw_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT.xlsx')
raw_clean_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_clean.xlsx')

get_dicts(raw_dict_path) %>% 
  unify_uninames("NOMBRE1", "AL_PRIM_NOMB") %>% 
  unify_uninames("NOMBRE1", "NAME1") %>% 
  unify_uninames("NOMBRE2", "AL_SEGU_NOMB") %>% 
  unify_uninames("APELLIDO1", "AL_PRIM_APEL") %>% 
  unify_uninames("APELLIDO2", "AL_SEGU_APEL") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TDOC") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TI_CODI_ID") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TIPO_DCTO...6") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TD") %>% 
  unify_uninames("NRO_DOCUMENTO", "NDOC") %>% 
  unify_uninames("NRO_DOCUMENTO", "AL_NUME_ID") %>% 
  unify_uninames("NRO_DOCUMENTO", "NRO_DOCUM") %>% 
  unify_uninames("NRO_DOCUMENTO", "NRO_DCTO") %>% 
  unify_uninames("DIRECCION_RESIDENCIA", "DIR_RES") %>% 
  unify_uninames("DIRECCION_RESIDENCIA", "DIREC_RESIDENCIA") %>% 
  unify_uninames("DIRECCION_RESIDENCIA", "DIRACUD") %>% 
  unify_uninames("TEL", "TEL_RES") %>% 
  unify_uninames("TEL", "AL_TELE_RESI") %>% 
  unify_uninames("TEL", "TELEFONO") %>% 
  unify_uninames("TEL", "TEL_UBICAC") %>% 
  modify_uniname("TEL_ACUDIENTE", "TELACUD") %>% 
  unify_uninames("TEL_ACUDIENTE", "AL_TELE_RESI_ACU") %>% 
  modify_uniname("TEL_PADRE", "AL_PADR_TEL") %>% 
  modify_uniname("TEL_MADRE", "AL_MADR_TEL") %>% 
  modify_uniname("TIPO_DOCUMENTO_ACUDIENTE", "NDOCACUD") %>% 
  modify_uniname("NRO_DOCUMENTO_ACUDIENTE", "AL_CEDU_ACUD") %>% 
  modify_uniname("NOMBRE1_ACUDIENTE", "NOMB1 ACUD") %>% 
  unify_uninames("NOMBRE1_ACUDIENTE", "AL_NOMB_ACUD") %>% 
  modify_uniname("NOMBRE2_ACUDIENTE", "NOMB2 ACUD") %>% 
  modify_uniname("NOMBRE_MADRE", "AL_MADR_NOMB") %>% 
  modify_uniname("NRO_DOCUMENTO_MADRE", "AL_MADR_ID") %>% 
  modify_uniname("NOMBRE_PADRE", "AL_PADR_NOMB") %>% 
  modify_uniname("NRO_DOCUMENTO_PADRE", "AL_PADR_ID") %>% 
  # TAREA. Desacparecer warnings y revisar que elimine todo.
  delete_uninames(
    c("AL_PRIM_NOMB", "NAME1", "AL_SEGU_NOMB", "AL_PRIM_APEL", "AL_SEGU_APEL",
      "TDOC", "TI_CODI_ID", "TDOC", "TIPO_DCTO...6", "TIPO DCTO...8", "TD",
      "NDOC", "AL_NUME_ID", "NRO_DOCUM", "DIR_RES", "DIREC_RESIDENCIA",
      "DIRECCION_RESIDENCIA HOMOLOGADA", 
      "DIRACUD", "TEL_RES", "AL_TELE_RESI", "TELEFONO", "TEL_UBICAC", "TELACUD",
      "AL_TELE_RESI_ACU", "AL_PADR_TEL", "AL_MADR_TEL", "NDOCACUD", "AL_CEDU_ACUD", 
      "NOMB1 ACUD", "AL_NOMB_ACUD", "NOMB2 ACUD", "AL_MADR_NOMB", "AL_MADR_ID", 
      "AL_PADR_NOMB", "AL_PADR_ID", "NRO_DCTO")
  ) %>% 
  # TAREA. modificar uniclass para los uninames que tienen missing de las variables de arriba.
  # view_colname() %>% 
  # filter(uniname %in% c("NOMBRE1", "NOMBRE2")) %>% View
  # La menos restrictiva
    modify_uniclass(c("NRO_DOCUMENTO", "TIPO_DOCUMENTO"), 'character') %>% 
    
  #uninames_to_modify = "NRO_DOCUMENTO","TIPO_DOCUMENTO",  
  #new_uniclass = "character" # "numeric", "integer" "character", "date", "logical"
  # ) %>% 
  # modify_uniclass(c("NOMBRE1", "NOMBRE2"), 'character') %>% 
  save_dicts(raw_clean_dict_path)

# Unify sensitive information ---------------------------------------------

folder <- FOLDER_SIMAT_2004
files <- read_excel(
  file.path(DICTS_FOLDER, 'raw_SIMAT.xlsx'), sheet = "Clasificacion") %>% 
  filter(Clasificacion == "Base") %>% pull(File)
dict <- read_excel(raw_clean_dict_path, sheet = 'colname')
SELECTED_COLUMNS <- dict$uniname
new_folder <- file.path(ROOT_FOLDER, 'data/processed/SIMAT/Medellin/2004-2023')

create_folders(new_folder)
for (file in files) {
  message("Begin", file)
  new_file <- sprintf("SIMAT_matricula_validada_%s.parquet",
                      str_extract(file, '\\d{4}'))
  arrow::open_dataset(file.path(folder, file)) %>%
    unify_colnames(dict, file, SELECTED_COLUMNS) %>% 
    unify_classes(dict, file, SELECTED_COLUMNS) %>% 
    write_parquet(file.path(new_folder, new_file))
  message("End", file)
}

# Crear ids
# Eliminar informacion sensible.

# TAREA. TABLA CON LOS UNINAMES ASOCIADOS A LAS VARIABLES DE INFORMACION SENSIBLE.
# banned_columns <- read_excel()
# arrow::open_dataset(file.path(folder, file)) %>% 
#   select(all_of(banned_columns))

