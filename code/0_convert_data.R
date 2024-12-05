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

# Edit dictionaries -------------------------------------------------------

get_dicts(file.path(DICTS_FOLDER, 'raw_SIMAT_2004-2022.xlsx')) %>% 
  unify_uninames("NOMBRE1", "AL_PRIM_NOMB") %>% 
  unify_uninames("NOMBRE1", "NAME1") %>% 
  unify_uninames("NOMBRE2", "AL_SEGU_NOMB") %>% 
  unify_uninames("APELLIDO1", "AL_PRIM_APEL") %>% 
  unify_uninames("APELLIDO2", "AL_SEGU_APEL") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TDOC") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TI_CODI_ID") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TIPO_DCTO...6") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TIPO_DOCUM") %>% 
  unify_uninames("TIPO_DOCUMENTO", "TD") %>% 
  unify_uninames("NRO_DOCUMENTO", "NDOC") %>% 
  unify_uninames("NRO_DOCUMENTO", "AL_NUME_ID") %>% 
  unify_uninames("NRO_DOCUMENTO", "NRO_DOCUM") %>% 
  unify_uninames("NRO_DOCUMENTO", "NRO_DCTO") %>% 
  unify_uninames("DIRECCION_RESIDENCIA", "DIR_RES") %>% 
  unify_uninames("DIRECCION_RESIDENCIA", "DIREC_RESID") %>% 
  unify_uninames("DIRECCION_RESIDENCIA", "DIREC_RESIDENCIA") %>% 
  unify_uninames("TEL", "TEL_RES") %>% 
  unify_uninames("TEL", "AL_TELE_RESI") %>% 
  unify_uninames("TEL", "TELEFONO") %>% 
  unify_uninames("TEL", "TEL_UBICAC") %>% 
  modify_uniname("TEL_ACUDIENTE", "TELACUD") %>% 
  unify_uninames("TEL_ACUDIENTE", "AL_TELE_RESI_ACU") %>% 
  modify_uniname("TEL_PADRE", "AL_PADR_TEL") %>% 
  modify_uniname("TEL_MADRE", "AL_MADR_TEL") %>% 
  modify_uniname("TIPO_DOCUMENTO_ACUDIENTE", "TDOCACUD") %>% 
  modify_uniname("NRO_DOCUMENTO_ACUDIENTE", "NDOCACUD") %>% 
  unify_uninames("NRO_DOCUMENTO_ACUDIENTE", "AL_CEDU_ACUD") %>% 
  modify_uniname("NOMBRE1_ACUDIENTE", "NOMB1 ACUD") %>% 
  unify_uninames("NOMBRE1_ACUDIENTE", "AL_NOMB_ACUD") %>% 
  modify_uniname("NOMBRE2_ACUDIENTE", "NOMB2 ACUD") %>% 
  modify_uniname("NOMBRE_MADRE", "AL_MADR_NOMB") %>% 
  modify_uniname("NRO_DOCUMENTO_MADRE", "AL_MADR_ID") %>% 
  modify_uniname("NOMBRE_PADRE", "AL_PADR_NOMB") %>% 
  modify_uniname("NRO_DOCUMENTO_PADRE", "AL_PADR_ID") %>% 
  modify_uniclass(c("NRO_DOCUMENTO", "TIPO_DOCUMENTO"), 'character') %>% 
  save_dicts(file.path(DICTS_FOLDER, 'raw_SIMAT_2004-2022_clean.xlsx'))

# Unify base files --------------------------------------------------------


folder <- FOLDER_RAW_SIMAT_2004
files <- read_excel(
  file.path(DICTS_FOLDER, 'tables.xlsx'), sheet = "Clasificacion") %>% 
  filter(Clasificacion == "Base") %>% pull(File)

dict <- read_excel(file.path(DICTS_FOLDER, 'raw_SIMAT_2004-2022_clean.xlsx'), 
                   sheet = 'colname')
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


# Lab ---------------------------------------------------------------------

# folder <- FOLDER_PROCESSED_SIMAT_2004
# file <- "SIMAT_matricula_validada_2005.parquet"
# 
# ids_vars <- c("TIPO_DOCUMENTO", "NRO_DOCUMENTO", "NRO_DOCUMENTO_MADRE",
#               "NRO_DOCUMENTO_PADRE", "NRO_DOCUMENTO_ACUDIENTE")
# 
# # Ver manualmente
# open_dataset(file.path(folder, file)) %>% 
#   select(all_of(ids_vars)) %>% 
#   distinct() %>% collect %>% View
# # No hay padre y madre juntos
# open_dataset(file.path(folder, file)) %>% 
#   select(all_of(ids_vars)) %>% 
#   filter(
#     !(NRO_DOCUMENTO_MADRE == '' | is.na(NRO_DOCUMENTO_MADRE)),
#     !(NRO_DOCUMENTO_PADRE == '' | is.na(NRO_DOCUMENTO_PADRE))
#   ) %>% 
#   distinct() %>% collect %>% View
# # No hay padre o madre
# open_dataset(file.path(folder, file)) %>% 
#   select(all_of(ids_vars)) %>% 
#   filter(
#     NRO_DOCUMENTO_MADRE == '' | is.na(NRO_DOCUMENTO_MADRE),
#     NRO_DOCUMENTO_PADRE == '' | is.na(NRO_DOCUMENTO_PADRE)
#   ) %>% 
#   distinct() %>% collect %>% View
# # Todos los padres son acudientes
# open_dataset(file.path(folder, file)) %>% 
#   select(all_of(ids_vars)) %>% 
#   filter(NRO_DOCUMENTO_PADRE != NRO_DOCUMENTO_ACUDIENTE) %>% 
#   distinct() %>% collect %>% View
# # Todas las madres son acudientes
# open_dataset(file.path(folder, file)) %>% 
#   select(all_of(ids_vars)) %>% 
#   filter(NRO_DOCUMENTO_MADRE != NRO_DOCUMENTO_ACUDIENTE) %>% 
#   distinct() %>% collect %>% View


# Create fake ids ---------------------------------------------------------

folder <- FOLDER_PROCESSED_SIMAT_2004
ids_path <- file.path(FOLDER_INDIVIDUALS, 'ids_SIMAT_2004-2022.parquet')

df <- NULL
for (file in files) {
  col_names <- open_dataset(file.path(folder, file))
  # Student.
  df0 <- open_dataset(file.path(folder, file)) %>% 
    distinct(TIPO_DOCUMENTO, NRO_DOCUMENTO) %>% 
    mutate(label = 'estudiante') %>% collect
  # Guardian. Only present two years, one with `TIPO` and another without it.
  if ('NRO_DOCUMENTO_ACUDIENTE' %in% col_names) {
    if ('TIPO_DOCUMENTO_ACUDIENTE' %in% col_names) {
      df1 <- open_dataset(file.path(folder, file)) %>% 
        distinct(TIPO_DOCUMENTO = TIPO_DOCUMENTO_ACUDIENTE,
                 NRO_DOCUMENTO = NRO_DOCUMENTO_ACUDIENTE) %>% 
        mutate(label = 'acudiente') %>% collect
    } else {
      df1 <- open_dataset(file.path(folder, file)) %>% 
        distinct(NRO_DOCUMENTO = NRO_DOCUMENTO_ACUDIENTE) %>% 
        # We assume are CC by logic and variable name, but in 2004 are about 98%.
        mutate(TIPO_DOCUMENTO = 1, label = 'acudiente') %>% collect
    }
    df0 <- bind_rows(df0, df1)
  }
  # Mother.
  if ('NRO_DOCUMENTO_MADRE' %in% col_names) {
    df0 <- bind_rows(
      df0, 
      # Assume are CC as they are parents (ussually adults)
      open_dataset(file.path(folder, file)) %>% 
        distinct(NRO_DOCUMENTO = NRO_DOCUMENTO_PADRE) %>% 
        mutate(TIPO_DOCUMENTO = 1, label = 'padre') %>% collect
    )
  }
  # Father.
  if ('NRO_DOCUMENTO_PADRE' %in% col_names) {
    df0 <- bind_rows(
      df0, 
      # Assume are CC as they are parents (ussually adults)
      open_dataset(file.path(folder, file)) %>% 
        distinct(NRO_DOCUMENTO = NRO_DOCUMENTO_MADRE) %>% 
        mutate(TIPO_DOCUMENTO = 1, label = 'madre') %>% collect
    )
  }
  df <- bind_rows(df, df0)
}
df %>% distinct %>% 
  left_join(
    read_excel(file.path(DICTS_FOLDER, 'tables.xlsx'), sheet = "ids_map"),
    by = "TIPO_DOCUMENTO"
    ) %>% 
  select(-TIPO_DOCUMENTO) %>% rename(TIPO_DOCUMENTO = TIPO_DOCUMENTO_id) %>% 
  write_parquet(ids_path)


open_dataset(ids_path) %>% 
  distinct(TIPO_DOCUMENTO, NRO_DOCUMENTO) %>% 
  collect %>% mutate(fake_id = row_number()) %>% 
  write_parquet('temp.parquet')

open_dataset(ids_path) %>% 
  left_join(open_dataset('temp.parquet'), 
            by = c('TIPO_DOCUMENTO', 'NRO_DOCUMENTO')) %>% 
  write_parquet(ids_path)

open_dataset(ids_path) %>% 
  count(TIPO_DOCUMENTO)%>% collect()%>% View()

# Remove sensitive information --------------------------------------------

sensitive_vars <- c(
  "NOMBRE1", "NOMBRE2", "APELLIDO1", "APELLIDO2", 
  "TIPO_DOCUMENTO", "NRO_DOCUMENTO", "DIRECCION_RESIDENCIA", 
  "TEL", "TEL_ACUDIENTE", "TEL_PADRE", "TEL_MADRE", 
  "TIPO_DOCUMENTO_ACUDIENTE", "NRO_DOCUMENTO_ACUDIENTE", 
  "NOMBRE1_ACUDIENTE", "NOMBRE2_ACUDIENTE", "NOMBRE_MADRE", 
  "NRO_DOCUMENTO_MADRE", "NOMBRE_PADRE", "NRO_DOCUMENTO_PADRE"
  )

folder <- FOLDER_PROCESSED_SIMAT_2004
files <- list.files(folder)
new_folder <- FOLDER_UNSENSITIVE_SIMAT_2004
create_folders(new_folder)

for (file in files) {
  df <- open_dataset(file.path(folder, file)) %>% 
    left_join(open_dataset('temp.parquet') %>% 
                filter(label == 'estudiante'), 
              by = c('TIPO_DOCUMENTO', 'NRO_DOCUMENTO')) 
  col_names <- names(df)
  if ('NRO_DOCUMENTO_ACUDIENTE' %in% col_names) {
    if (!'TIPO_DOCUMENTO_ACUDIENTE' %in% col_names) {
      df <- df %>% mutate(TIPO_DOCUMENTO_ACUDIENTE == 1)
    }
    df <- df %>% left_join(
      open_dataset('temp.parquet') %>% 
        rename(fake_id_acudiente = fake_id) %>% 
        filter(label == 'acudiente'), 
      by = join_by(TIPO_DOCUMENTO_ACUDIENTE == TIPO_DOCUMENTO,
                   NRO_DOCUMENTO_ACUDIENTE == NRO_DOCUMENTO)
    )
  }
  # Mother.
  if ('NRO_DOCUMENTO_MADRE' %in% col_names) {
    df <- df %>% 
      left_join(
        open_dataset('temp.parquet') %>% 
          select(fake_id_madre = fake_id, NRO_DOCUMENTO) %>% 
          filter(label == 'madre'),
        by = join_by(NRO_DOCUMENTO_MADRE == NRO_DOCUMENTO)
      )
  }
  # Father.
  if ('NRO_DOCUMENTO_PADRE' %in% col_names) {
    df <- df %>% 
      left_join(
        open_dataset('temp.parquet') %>% 
          select(fake_id_padre = fake_id, NRO_DOCUMENTO) %>% 
          filter(label == 'padre'),
        by = join_by(NRO_DOCUMENTO_PADRE == NRO_DOCUMENTO)
      )
  }
  df %>% 
    select(-all_of(sensitive_vars)) %>% 
    write_parquet(file.path(new_folder, file))
}

unlink('temp.parquet')

#' LAB
#' numeric DOC
#' MADRE y PADRE incluidos en acudientes