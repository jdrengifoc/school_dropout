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



