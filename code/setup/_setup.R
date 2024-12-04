ROOT_FOLDER <- c(
  mariajoseguerrero = "Deserción Escolar/school_dropout", 
  juanrengifo101 = "school_dropout"
)[Sys.getenv("USER")]

source(file.path(ROOT_FOLDER, 'code/setup/_globals.R'))
source(file.path(ROOT_FOLDER, 'code/setup/_requirements.R'))
source(file.path(ROOT_FOLDER, 'code/functions/auxiliary.R'))
