
# Folders -----------------------------------------------------------------

create_folders <- function(folders, recursive = T) {
  for (folder in folders) {
    if (!file.exists(folder)) {
      dir.create(folder, recursive = recursive)
      message(paste("created", folder))
    }
  }
}

# Edit dictionaries -------------------------------------------------------


get_dicts <- function(dict_path) {
  list(
    colname = read_excel(dict_path, sheet = 'colname'),
    colclass = read_excel(dict_path, sheet = 'colclass')
  )
}

unify_uninames <- function(dicts, primary_uniname, secondary_uniname) {
  
  if (!'uniname' %in% names(dicts$colname)) {
    stop('The column uniname must be in the dictionaries.')
  }
  if (!primary_uniname %in% dicts$colname$uniname) {
    stop('`primary_uniname` ', primary_uniname,  ' is not a valid uniname.')
  }
  if (!secondary_uniname %in% dicts$colname$uniname) {
    stop('`secondary_uniname` ', secondary_uniname, ' is not a valid uniname.')
  }
  # Compute new `class_mode`.
  new_class_mode <- dicts$colclass %>% 
    pivot_longer(cols = matches("\\."), names_to = "file", values_to = "colclass") %>% 
    filter(uniname %in% c(primary_uniname, secondary_uniname), !is.na(colclass)) %>% 
    count(colclass) %>% filter(n == max(n)) %>% pull(colclass)
  
  
  dicts$colclass <- dicts$colclass %>% 
    # Merge columns classes from `secondary_uniname` to `primary_uniname`.
    pivot_longer(cols = matches("\\."), names_to = "file", values_to = "colclass") %>% 
    group_by(file) %>%
    mutate(
      colclass = if_else(
        uniname == primary_uniname & is.na(colclass),
        colclass[uniname == secondary_uniname],
        colclass
      )
    ) %>%
    pivot_wider(names_from = file, values_from = colclass)
  
  
  dicts$colname <- dicts$colname %>%
    # Merge columns names.
    pivot_longer(cols = matches("\\."), names_to = "file", values_to = "value") %>%
    group_by(file) %>%
    mutate(
      value = if_else(
        uniname == primary_uniname & is.na(value),
        value[uniname == secondary_uniname],
        value
      )
    ) %>%
    pivot_wider(names_from = file, values_from = value) %>% 
    mutate(
      # Keep the not NA `uniclass` giving priority to the `primary_uniname`.
      uniclass = if_else(
        uniname == primary_uniname,
        if_else(is.na(uniclass), 
                uniclass[uniname == secondary_uniname],
                uniclass),
        uniclass
      ),
      # New `class_mode`.
      class_mode = if_else(uniname == primary_uniname,
                           new_class_mode, class_mode),
      # Union of `unique_classes`
      unique_classes = 
        if_else(
          uniname == primary_uniname,
          union(
            str_split_1(unique_classes[uniname == primary_uniname], '; '),
            str_split_1(unique_classes[uniname == secondary_uniname], '; ')
          ) %>% paste(collapse = '; '),
          unique_classes
        ),
      # Add the `coverage`
      coverage = if_else(
        uniname == primary_uniname,
        coverage + coverage[uniname == secondary_uniname],
        coverage
      ),
      uniclass = if_else(
        uniname == primary_uniname,
        if_else(str_detect(unique_classes, '; '), NA, uniclass),
        uniclass
      )
    ) %>% 
    filter(uniname != secondary_uniname) %>% 
    arrange(desc(coverage), uniname)
  
  return(dicts)
}

delete_uninames <- function(dicts, uninames_to_delete) {
  if (!all(uninames_to_delete %in% dicts$colname$uniname)) {
    warning('All `uninames_to_delete` elements must be valid uninames\n',
            'The following are not valid uninames\n',
      paste(uninames_to_delete[! uninames_to_delete %in% dicts$colname$uniname],
            collapse = ' ; ; '))
  }
  
  dicts$colname <- dicts$colname %>% 
    # Merge columns classes from `secondary_uniname` to `primary_uniname`.
    pivot_longer(cols = matches("\\."), names_to = "file", values_to = "value") %>% 
    group_by(file) %>%
    filter(!uniname %in% uninames_to_delete) %>%
    pivot_wider(names_from = file, values_from = value)
  
  dicts$colclass <- dicts$colclass %>% 
    pivot_longer(cols = matches("\\."), names_to = "file", values_to = "colclass") %>% 
    group_by(file) %>%
    filter(!uniname %in% uninames_to_delete) %>%
    pivot_wider(names_from = file, values_from = colclass)
  
  return(dicts)
}

modify_uniclass <- function(dicts, uninames_to_modify, new_uniclass) {
  dicts$colname <- dicts$colname %>% 
    mutate(uniclass = if_else(uniname %in% uninames_to_modify, 
                              new_uniclass, uniclass))
  
  return(dicts)
}

modify_uniname <- function(dicts, new_uniname, old_uniname) {
  if (!old_uniname %in% dicts$colname$uniname) {
    stop('`old_uniname` ', old_uniname,  ' is not a valid uniname.')
  }
  if (new_uniname %in% dicts$colname$uniname) {
    stop('`new_uniname` ', new_uniname,  ' must not be a existing uniname.')
  }
  
  dicts$colname <- dicts$colname %>% 
    mutate(uniname = if_else(uniname == old_uniname, new_uniname, uniname))
  dicts$colclass <- dicts$colclass %>% 
    mutate(uniname = if_else(uniname == old_uniname, new_uniname, uniname))
  
  return(dicts)
}

save_dicts <- function(dicts, new_dict_path) {
  write_xlsx(dicts, new_dict_path)
}

view_colname <- function(dicts) {
  dicts$colname
}
