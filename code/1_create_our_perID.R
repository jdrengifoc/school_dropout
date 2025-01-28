source("_setup_school_dropout.R")

FOLDER_UNSENSITIVE_SIMAT_2017 %>% list.files()

#' Less `PER_ID`s than `fake_id`s.
#' Perhaps they use other variables and make a better identification.
#' 
#' More both than `fake_id`s.
#' There are different documents associated to the same person. Which is to be
#' expected as a student can change its type of document. Or could be an error
tibble(
  both = open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
    distinct(fake_id, PER_ID) %>% collect %>% nrow,
  our = open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
    distinct(fake_id) %>% collect %>% nrow,
  original = open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
    distinct(PER_ID) %>% collect %>% nrow
)

# In average a `PER_ID` is seen 3.67 times over a seven years framework
open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
  count(PER_ID) %>% summarise(max(n), mean(n), median(n), min(n)) %>% collect

# In average a `PER_ID` is seen 3.67 times over a seven years framework
open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
  count(PER_ID) %>% summarise(mean(n)) %>% collect

# 6 observations with repeated per ID per year.  
open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
  count(ANNO_INF, PER_ID) %>% filter(n > 1) %>% 
  left_join(
    open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
      select(
        PER_ID, YEAR = ANNO_INF, fake_id, GENERO, FECHA_NACIMIENTO,
        NAC_DEPTO, NAC_MUN,
        ETNIA, TIPO_DISCAPACIDAD, PROVIENE_OTRO_MUN, PROVIENE_SECTOR_PRIV,
        ),
    by = c('PER_ID')
    ) %>% 
  collect %>% View

# 6 observations with repeated per ID per year.  
open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
  count(PER_ID, GENERO, FECHA_NACIMIENTO, NAC_DEPTO, NAC_MUN) %>% 
  select(PER_ID, n) %>% filter(n > 1) %>% arrange(PER_ID) %>% 
  left_join(
    open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% 
      select(
        PER_ID, YEAR = ANNO_INF, fake_id, 
        GENERO, FECHA_NACIMIENTO, NAC_DEPTO, NAC_MUN, ETNIA, 
        TIPO_DISCAPACIDAD, PROVIENE_OTRO_MUN, PROVIENE_SECTOR_PRIV,
      ) %>% arrange(PER_ID, desc(YEAR)),
    by = c('PER_ID')
  ) %>% 
  collect %>% View

# `Divipola_MUNICIPIO` All in MED, however there are institutions.
open_dataset(
  FOLDER_UNSENSITIVE_SIMAT_2017, 
  schema = schema(field("Divipola_MUNICIPIO", string()))) %>% 
  distinct(Divipola_MUNICIPIO) %>% collect %>% View

open_dataset(FOLDER_UNSENSITIVE_SIMAT_2017) %>% glimpse
  count(ANNO_INF) %>% collect %>% View

# Unify 2017 ---------------------------------------------------
raw_dict_path <- file.path(DICTS_FOLDER, 'unsensitive_SIMAT_2017-2023.xlsx')
clean_dict_path <- file.path(DICTS_FOLDER, 'unsensitive_SIMAT_2017-2023_lab.xlsx')

selected_uninames <- c(
  # structural_vars
  'ANNO_INF', 'PER_ID',
  'FECHA_NACIMIENTO', 'NAC_DEPTO', 'NAC_MUN', 'EDAD',
  'GENERO', 'ETNIA', 'FAKE_ID'
)
mapping_uninames <- list(
  "YEAR" = "ANNO_INF",
  "DPTO_NACIMIENTO" = "NAC_DEPTO",
  "MPIO_NACIMIENTO" = "NAC_MUN"
)

mapping_uniclasses <- list(
  "integer" = c(
    "YEAR", "PER_ID", "DPTO_NACIMIENTO", "MPIO_NACIMIENTO", "EDAD", "ETNIA"
    ),
  "character" = c("NRO_DOCUMENTO", "TIPO_DOCUMENTO", "GENERO")#,
  #"date" = c("FECHA_NACIMIENTO")
)

get_dicts(raw_dict_path) %>% 
  select_uninames(selected_uninames) %>%
  modify_uninames(mapping_uninames) %>% 
  modify_uniclasses(mapping_uniclasses) %>% 
  save_dicts(clean_dict_path)

# Unify
folder <- FOLDER_UNSENSITIVE_SIMAT_2017
files <- list.files(FOLDER_UNSENSITIVE_SIMAT_2017)

dict <- get_dicts(clean_dict_path)[['colname']]
SELECTED_COLUMNS <- dict$uniname

new_folder <- 'school_dropout/data/_lab'
create_folder(new_folder)
for (file in files) {
  message("Begin ", file)
  new_file <- file
  arrow::open_dataset(file.path(folder, file)) %>%
    unify_colnames(dict, file, SELECTED_COLUMNS) %>% 
    mutate(
      FECHA_NACIMIENTO = mdy(FECHA_NACIMIENTO)
    ) %>% collect %>% 
    unify_classes(dict, file, SELECTED_COLUMNS) %>% 
    write_parquet(file.path(new_folder, new_file))
  message("\tEnd ", file)
}

folder <- 'school_dropout/data/_lab'
blocking_variables <- c(
  "FECHA_NACIMIENTO", "DPTO_NACIMIENTO", "MPIO_NACIMIENTO", "GENERO", "ETNIA"
)
open_dataset(folder) %>% 
  select(all_of(blocking_variables)) %>% 
  distinct %>% collect %>% 
  mutate(block_id = row_number()) %>% 
  write_parquet('temp.parquet')

#' Caraterísticas id
#' 1. Único por periodo
#' 2. Único por persona.
open_dataset('temp.parquet') %>% 
  left_join(
    open_dataset(folder),
    by = blocking_variables
    ) %>% 
  relocate(block_id, PER_ID, FAKE_ID) %>%
  collect

open_dataset(folder) %>% 
  group_by(PER_ID, YEAR) %>%
  arrange(YEAR) %>% collect %>% 
  mutate(
    diff_year = YEAR - lag(YEAR, default = first(YEAR))
    
    ) %>% 
  summarise(
    discontinuity_count = sum(diff_year > 1), 
    year_range = max(YEAR) - min(YEAR),
    .groups = 'drop'
    ) %>% 
  write_parquet('temp.parquet')


open_dataset(folder) %>% 
  # Auxiliary variables.
  select(PER_ID, YEAR) %>% 
  group_by(PER_ID) %>% 
  summarise(
    year_range = max(YEAR) - min(YEAR) + 1, year_max = max(YEAR),
    .groups = 'drop') %>% 
  left_join(
    open_dataset(folder) %>% select(PER_ID, YEAR) %>% count(PER_ID)
  ) %>% 
  # Individual metrics.
  mutate(
    discontinuity_count = year_range - n, 
    discontinuity_prop = discontinuity_count / year_range,
    has_discontinuity = discontinuity_count > 0,
    one_value = (n == 1) & (year_max == 2023L)
    ) %>% #collect
  # Aggregate metrics.
  summarise(
    across(discontinuity_count:one_value, mean)
  ) %>%
  collect
  
  arrange(YEAR) %>% collect %>% 
  mutate(
    diff_year = YEAR - lag(YEAR, default = first(YEAR))
    
  ) %>% 
  summarise(
    discontinuity_count = sum(diff_year > 1), 
    year_range = max(YEAR) - min(YEAR),
    .groups = 'drop'
  ) %>% 
  write_parquet('temp.parquet')

open_dataset('temp.parquet') %>% 
  mutate(
    is_discontinous = discontinuity_count > 0,
    discontinuity_prop = discontinuity_count / year_range
    ) %>% 
  filter(is.na(discontinuity_prop)) %>% collect
  summarise(mean(is_discontinous), mean(discontinuity_prop)) %>% 
  collect

# Structural variables ----------------------------------------------------

var_names <- list(
  structural_vars = c(
    'ANNO_INF', 'PER_ID',
    'FECHA_NACIMIENTO', 'NAC_DEPTO', 'NAC_MUN', 'EDAD',
    'GENERO', 'ETNIA'
    ),
  mobility_vars = c(
    # Institution codes
    'CODIGO_DANE','CODIGO_DANE_SEDE', 
    'CONS_SEDE', 'CODIGO_SED', 'SEDE_ID', 'NOMBRE_SEDE',
    'EST_ID', 'NOMBRE_ESTABLECIMIENTO',
    # Institution characteristics.
    'CARACTER', 'ESPECIALIDAD', 'TIPO_JORNADA', 'METODOLOGIA', 'INTERNADO',
    # Studies
    'GRADO', 'GRUPO', 'REPITENTE', 'NUEVO',
    # Residence
    'RES_DEPTO', 'RES_MUN', 'ESTRATO', 'ZON_ALU',
    # Changes
    'PROVIENE_SECTOR_PRIV', 'PROVIENE_OTRO_MUN', 'SIT_ACAD_ANO_ANT', 
    'CON_ALUM_ANO_ANT', 'ESTADO', 'MOTIVO', 'FECHA_NOVEDAD',
    'CODIGO_PAIS_ORIGEN', 'NOMBRE_PAIS_ORIGEN',
    # dynamic demographics
    'TIPO_DISCAPACIDAD', 'SUBSIDIADO', 'SISBEN', 'CAB_FAMILIA', 
    'BEN_MAD_FLIA', 'BEN_VET_FP', 'BEN_HER_NAC', 
    'TRASTORNOS_ESPECIFICOS', 'DISCAPACIDAD_HOMOLGADA_2021'
    ),
  delete_vars = c(
    'MUN_CODIGO', 'DPTO_CARGA',
    'Divipola_MUNICIPIO', # 2022 has institution names
    'GRADO1', 'ETNIA1', 'DISCAPACIDAD1', 'REPITENCIA_FINAL'
    ),
  extra_vars = c(
    'CAP_EXC', 'RES', 'INS_FAMILIAR', 'FUE_RECU'
    ),
  unknown_vars = c(
    'CTE_ID_SECTOR', 'CTE_ID_CALENDARIO', 'CTE_ID_ZONA', 'TOTAL_MATRICULA',
    'MATRICULA_DEFINITIVA', 'EFICIENCIA', 'CTE_ID_APOYO_ACAD_ESP', 'CTE_ID_SRPA',
    'SECTOR_MATR', 'NIVEL5'
    )
  )





 
files <- list.files(FOLDER_UNSENSITIVE_SIMAT_2017, full.names = T)
files <- files[-c(6L)]
open_dataset(files[2]) %>% glimpse
  count(Matricula_Definitiva) %>% arrange(desc(n)) %>% collect 
  select(CODIGO_DANE, CODIGO_DANE_SEDE) %>% 
  mutate(pp = CODIGO_DANE_SEDE == CODIGO_DANE) %>% 
  filter(!pp) %>% head %>% collect
  count(pp) %>% collect


