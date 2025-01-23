source("_setup_school_dropout.R")

# SIMAT 2004 - 2022 -------------------------------------------------------------------
files <- list.files('Deserción Escolar/data/SIMAT/Matriculas validadas por año',
                    full.names = T) %>% str_remove("\\~\\$")

# Convert xlsx to parquet
new_folder <- FOLDER_RAW_SIMAT_2004
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
dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_2004-2022.xlsx')
create_partial_dictionary(folder = new_folder, files = files, 
                          dict_path = dict_path, verbose = T, overwrite = F)
sort_partial_dictionary(dict_path, overwrite = T)


# SIMAT 2017-2023 ---------------------------------------------------------

files <- list.files('Deserción Escolar/data/SIMAT/Matricula validada 2017-2023',
                    full.names = T) %>% str_remove("\\~\\$")

new_folder <- FOLDER_RAW_SIMAT_2017
create_folder(new_folder)
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
dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_2017-2023.xlsx')
create_partial_dictionary(folder = new_folder, files = files, 
                          dict_path = dict_path, verbose = T, overwrite = T)
sort_partial_dictionary(dict_path, overwrite = T)



# Edit dictionaries ---------------------------------------------------
raw_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_2004-2022.xlsx')
clean_dict_path <- file.path(DICTS_FOLDER, 'raw_SIMAT_2004-2022_clean.xlsx')

mapping_uninames <- list(
  "NOMBRE1" = c("AL_PRIM_NOMB", "NAME1"),
  "NOMBRE2" = "AL_SEGU_NOMB",
  "APELLIDO1" = "AL_PRIM_APEL",
  "APELLIDO2" = "AL_SEGU_APEL",
  "TIPO_DOCUMENTO" = c(
    "TDOC", "TI_CODI_ID", "TIPO_DCTO...6", "TIPO_DOCUM", "TD"),
  "NRO_DOCUMENTO" = c("NDOC", "AL_NUME_ID", "NRO_DOCUM", "NRO_DCTO"),
  "DIRECCION_RESIDENCIA" = c("DIR_RES", "DIREC_RESID", "DIREC_RESIDENCIA"),
  "TEL" = c("TEL_RES", "AL_TELE_RESI", "TELEFONO", "TEL_UBICAC"),
  "TEL_ACUDIENTE" = c("TELACUD", "AL_TELE_RESI_ACU"),
  "TEL_PADRE" = "AL_PADR_TEL",
  "TEL_MADRE" = "AL_MADR_TEL",
  "TIPO_DOCUMENTO_ACUDIENTE" = "TDOCACUD",
  "NRO_DOCUMENTO_ACUDIENTE" = c("NDOCACUD", "AL_CEDU_ACUD"),
  "NOMBRE1_ACUDIENTE" = c("NOMB1 ACUD", "AL_NOMB_ACUD"),
  "NOMBRE2_ACUDIENTE" = "NOMB2 ACUD",
  "NOMBRE_MADRE" = "AL_MADR_NOMB",
  "NRO_DOCUMENTO_MADRE" = "AL_MADR_ID",
  "NOMBRE_PADRE" = "AL_PADR_NOMB",
  "NRO_DOCUMENTO_PADRE" = "AL_PADR_ID"
)

mapping_uniclasses <- list(
  "character" = c("NRO_DOCUMENTO", "TIPO_DOCUMENTO")
)

get_dicts(raw_dict_path) %>% 
  modify_uninames(mapping_uninames) %>% 
  modify_uniclasses(mapping_uniclasses) %>% 
  save_dicts(clean_dict_path)

# Unify base files --------------------------------------------------------


folder <- FOLDER_RAW_SIMAT_2004
files <- read_excel(
  file.path(DICTS_FOLDER, 'tables.xlsx'), sheet = "Clasificacion") %>% 
  filter(Clasificacion == "Base") %>% pull(File)

dict <- get_dicts(clean_dict_path)[['colname']]
SELECTED_COLUMNS <- dict$uniname

new_folder <- FOLDER_PROCESSED_SIMAT_2004
create_folder(new_folder)
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

files <- list.files(new_folder)
dict_path <- file.path(DICTS_FOLDER, 'processed_SIMAT_2004-2022.xlsx')
create_partial_dictionary(folder = new_folder, files = files, 
                          dict_path = dict_path, verbose = T, overwrite = T)
sort_partial_dictionary(dict_path, overwrite = T)



# Create fake ids SIMAT 2004 ----------------------------------------------

folder <- FOLDER_PROCESSED_SIMAT_2004
ids_path <- file.path(FOLDER_INDIVIDUALS, 'ids_SIMAT_2004-2022.parquet')

df <- NULL
for (file in files) {
  col_names <- open_dataset(file.path(folder, file))%>% names()
  # Student.
  df0 <- open_dataset(file.path(folder, file)) %>% 
    distinct(TIPO_DOCUMENTO, NRO_DOCUMENTO) %>% 
    mutate(label = 'estudiante') %>% collect
  # Guardian. Only present two years, one with `TIPO` and another without it.
  if ('NRO_DOCUMENTO_ACUDIENTE' %in% col_names) {
    if ('TIPO_DOCUMENTO_ACUDIENTE' %in% col_names) {
      df1 <- open_dataset(file.path(folder, file)) %>% 
        select(TIPO_DOCUMENTO = TIPO_DOCUMENTO_ACUDIENTE,
               NRO_DOCUMENTO = NRO_DOCUMENTO_ACUDIENTE) %>%
        distinct(TIPO_DOCUMENTO, NRO_DOCUMENTO) %>%
        mutate(label = 'acudiente') %>% collect
    } else {
      df1 <- open_dataset(file.path(folder, file)) %>% 
        select(NRO_DOCUMENTO = NRO_DOCUMENTO_ACUDIENTE) %>% 
        distinct(NRO_DOCUMENTO) %>% 
        
        # We assume are CC by logic and variable name, but in 2004 are about 98%.
        mutate(TIPO_DOCUMENTO = "1", label = 'acudiente') %>% collect
    }
    df0 <- bind_rows(df0, df1)
  }
  # Father.
  if ('NRO_DOCUMENTO_PADRE' %in% col_names) { 
    df0 <- bind_rows(
      df0, 
      # Assume are CC as they are parents (usually adults)
      open_dataset(file.path(folder, file)) %>% 
        select(NRO_DOCUMENTO = NRO_DOCUMENTO_PADRE) %>% 
        distinct(NRO_DOCUMENTO) %>% 
        mutate(TIPO_DOCUMENTO = "1", label = 'padre') %>% collect
    )
  }
  # Mother.
  if ('NRO_DOCUMENTO_MADRE' %in% col_names) {
    df0 <- bind_rows(
      df0, 
      # Assume are CC as they are parents (usually adults)
      open_dataset(file.path(folder, file)) %>% 
        select(NRO_DOCUMENTO = NRO_DOCUMENTO_MADRE) %>% 
        distinct(NRO_DOCUMENTO) %>% 
        mutate(TIPO_DOCUMENTO = "1", label = 'madre') %>% collect
    )
  }
  df <- bind_rows(df, df0)
}
df %>% distinct %>% 
  left_join(
    read_excel(file.path(DICTS_FOLDER, 'tables.xlsx'),
               sheet = "TIPO_DOCUMENTO_ids_map_2004"),
    by = "TIPO_DOCUMENTO"
  ) %>% 
  select(-TIPO_DOCUMENTO) %>% rename(TIPO_DOCUMENTO = TIPO_DOCUMENTO_id) %>% 
  distinct() %>% 
  write_parquet(ids_path)


open_dataset(ids_path) %>% 
  distinct(TIPO_DOCUMENTO, NRO_DOCUMENTO) %>% 
  collect %>% mutate(fake_id = row_number()) %>% 
  write_parquet('temp.parquet')

open_dataset(ids_path) %>% 
  left_join(open_dataset('temp.parquet'), 
            by = c('TIPO_DOCUMENTO', 'NRO_DOCUMENTO')) %>% 
  write_parquet(ids_path)


# Remove sensitive information SIMAT 2004 ---------------------------------

sensitive_vars <- c(
  "NOMBRE1", "NOMBRE2", "APELLIDO1", "APELLIDO2", 
  "TIPO_DOCUMENTO", "NRO_DOCUMENTO", "DIRECCION_RESIDENCIA", 
  "TEL", "TEL_ACUDIENTE", "TEL_PADRE", "TEL_MADRE", 
  "TIPO_DOCUMENTO_ACUDIENTE", "NRO_DOCUMENTO_ACUDIENTE", 
  "NOMBRE1_ACUDIENTE", "NOMBRE2_ACUDIENTE", "NOMBRE_MADRE", 
  "NRO_DOCUMENTO_MADRE", "NOMBRE_PADRE", "NRO_DOCUMENTO_PADRE",
  "APELLIDO1_FON", "APELLIDO2_FON", "NOMBRE", "NOMBRE1_FON", "NOMBRE2_FON",
  "APEL1 ACUD", "APEL2 ACUD", "DIRECCION_RESIDENCIA  HOMOLOGADA",
  "DIRECCION_RESIDENCIA...24", "DIRECCION_RESIDENCIA...25", 
  "TIPO E IDENTIFICACIÓN", "ID", "TIPO_DCTO...8")

folder <- FOLDER_PROCESSED_SIMAT_2004
files <- list.files(folder)
new_folder <- FOLDER_UNSENSITIVE_SIMAT_2004
create_folder(new_folder)

for (file in files) {
  df <- open_dataset(file.path(folder, file)) %>% 
    left_join(open_dataset('temp.parquet'), 
              by = c('TIPO_DOCUMENTO', 'NRO_DOCUMENTO')) 
  col_names <- names(df)
  if ('NRO_DOCUMENTO_ACUDIENTE' %in% col_names) { 
    #Asumimos que todos son cedulas 
    if (!'TIPO_DOCUMENTO_ACUDIENTE' %in% col_names) {
      df <- df %>% mutate(TIPO_DOCUMENTO_ACUDIENTE = "1")
    }
    df <- df %>% left_join(
      open_dataset('temp.parquet') %>% 
        rename(fake_id_acudiente = fake_id) , 
      by = join_by(TIPO_DOCUMENTO_ACUDIENTE == TIPO_DOCUMENTO,
                   NRO_DOCUMENTO_ACUDIENTE == NRO_DOCUMENTO)
    )
  }
  # Mother.
  if ('NRO_DOCUMENTO_MADRE' %in% col_names) {
    df <- df %>% 
      left_join(
        open_dataset('temp.parquet') %>% 
          select(fake_id_madre = fake_id, NRO_DOCUMENTO),
        by = join_by(NRO_DOCUMENTO_MADRE == NRO_DOCUMENTO)
      )
  }
  # Father.
  if ('NRO_DOCUMENTO_PADRE' %in% col_names) {
    df <- df %>% 
      left_join(
        open_dataset('temp.parquet') %>% 
          select(fake_id_padre = fake_id, NRO_DOCUMENTO) ,
        by = join_by(NRO_DOCUMENTO_PADRE == NRO_DOCUMENTO)
      )
  }
  df %>% 
    select(-all_of(sensitive_vars[sensitive_vars %in% names(df)])) %>% 
    write_parquet(file.path(new_folder, file))
}

unlink('temp.parquet')


#' LAB
#' numeric DOC
#' MADRE y PADRE incluidos en acudientes


files <- list.files(new_folder)
dict_path <- file.path(DICTS_FOLDER, 'unsensitive_SIMAT_2004-2022.xlsx')
create_partial_dictionary(folder = new_folder, files = files, 
                          dict_path = dict_path, verbose = T, overwrite = T)
sort_partial_dictionary(dict_path, overwrite = T)




# Create fake ids SIMAT 2017 ----------------------------------------------
folder <- FOLDER_RAW_SIMAT_2017
ids_path <- file.path(FOLDER_INDIVIDUALS, 'ids_SIMAT_2017-2023.parquet')
files <- list.files(folder)
  
df <- NULL
for (file in files) {
  df0 <- open_dataset(file.path(folder, file)) %>% 
    distinct(TIPO_DOCUMENTO, NRO_DOCUMENTO) %>% collect
  df <- bind_rows(df, df0)
}

# df %>% distinct %>% count(TIPO_DOCUMENTO) %>% collect %>% View 
df %>% distinct %>% 
  collect %>% mutate(fake_id = row_number()) %>% 
  write_parquet(ids_path)


# Remove sensitive information SIMAT 2017 ---------------------------------

sensitive_vars <- c(
  "NOMBRE1", "NOMBRE2", "APELLIDO1", "APELLIDO2", "TIPO_DOCUMENTO", 
  "NRO_DOCUMENTO", "DIRECCION_RESIDENCIA", "TEL"
)

folder <- FOLDER_RAW_SIMAT_2017
files <- list.files(folder)
new_folder <- FOLDER_UNSENSITIVE_SIMAT_2017
create_folder(new_folder)

for (file in files) {
  df <- open_dataset(file.path(folder, file))
  df %>% 
    mutate(TIPO_DOCUMENTO = as.numeric(TIPO_DOCUMENTO)) %>% 
    left_join(open_dataset(ids_path), 
              by = c('TIPO_DOCUMENTO', 'NRO_DOCUMENTO')) %>% 
    select(-all_of(sensitive_vars[sensitive_vars %in% names(df)])) %>% 
    write_parquet(file.path(new_folder, file))
}

files <- list.files(new_folder)
dict_path <- file.path(DICTS_FOLDER, 'unsensitive_SIMAT_2017-2023.xlsx')
create_partial_dictionary(folder = new_folder, files = files, 
                          dict_path = dict_path, verbose = T, overwrite = T)
sort_partial_dictionary(dict_path, overwrite = T)
