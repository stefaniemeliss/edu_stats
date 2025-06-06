# Schools, pupils and their characteristics #

# This release contains the latest statistics on school and pupil numbers and their characteristics, 
# including age, gender, free school meals (FSM) eligibility, English as an additional language, ethnicity, school characteristics, class sizes.
# The publication combines information from the school census, school level annual school census, 
# general hospital school census and alternative provision census.

# The most recently published data is from the school census which took place on 18th January 2024 (Spring census)

options(scipen = 999)
# empty work space
rm(list = ls())
gc()

# load libraries
library(kableExtra)
library(dplyr)

devtools::source_url("https://github.com/stefaniemeliss/edu_stats/blob/main/functions.R?raw=TRUE")

# define directories
dir <- getwd()
dir_data <- file.path(dir, "data")
dir_misc <- file.path(dir, "misc")
dir_in <- file.path(dir_data, "school-pupils-and-their-characteristics")

# determine year list (akin to other data sources)
years_list <- paste0(20, 10:23, 11:24)
lookup <- data.frame(time_period = as.numeric(years_list),
                     academic_year = as.numeric(substr(years_list, 1, 4)))

# rename folders that currently only have one year included (data collected in Jan)
rename_folders <- F
if (rename_folders) {
  # save df with old and new folder names
  start <- 2010
  finish <- 2019
  # data collection takes place in Jan of the year
  # e.g., data from 2014 is from 2013/14
  tmp <- data.frame(old = c(start:finish)) 
  tmp$new <- paste0(paste0(tmp$old-1,"-", gsub(20, "",tmp$old)))
  # add dirs
  tmp$from <- file.path(dir_in, tmp$old)
  tmp$to <- file.path(dir_in, tmp$new)
  # rename
  file.rename(from = c(tmp$from), to = c(tmp$to))
}


# determine years of interest
start <- 2010
finish <- 2023

id_cols <- c("time_period", "urn")


# pupil on roll #

files_pupils <- list.files(path = dir_in,
                           pattern = "school_level|School_level_school|Schools_Pupils_UD|pupil_characteristics_UD",
                           recursive = T,
                           full.names = T)
files_pupils <- files_pupils[!grepl("Meta|meta|ncyear|class|census|TEMPLATE", files_pupils)]
files_pupils

# class size #

files_classes <- list.files(path = dir_in,
                            pattern = "class|Class",
                            recursive = T,
                            full.names = T)
files_classes <- files_classes[!grepl(".pdf|.xls|/data/class|/data/spc", files_classes)]
files_classes

#### combine data ####

# loop over years #
for (i in seq_along(start:finish)) {
  
  year = c(start:finish)[i]
  
  # determine academic year
  academic_year <- paste0(year,"-", year+1)
  
  # pupils on roll #
  
  # read in pupil on roll data
  tmp_p <- read.csv(file = files_pupils[i])

  # change col names
  names(tmp_p) <- tolower(gsub("X...", "", names(tmp_p), fixed = T))
  names(tmp_p) <- gsub("x..", "perc.", names(tmp_p), fixed = T)
  names(tmp_p) <- gsub(".", "_", names(tmp_p), fixed = T)

  # subset to exclude any non-school level data
  tmp_p <- tmp_p %>% filter(urn != "" & urn != 0)
  
  # remove school that was registered twice
  tmp_p <- tmp_p[!(tmp_p$urn == 143840 & tmp_p$estab == 6008), ]
  
  # format opendate if it exists
  if ("opendate" %in% names(tmp_p)) {
    tmp_p$opendate <- as.Date(tmp_p$opendate, format =  "%d/%m/%Y")
    tmp_p <- rename(tmp_p, open_date = opendate)
  } else if ("open_date" %in% names(tmp_p)) {
    tmp_p$open_date <- ifelse(tmp_p$open_date == "00:00.0" | tmp_p$open_date == "", NA, tmp_p$open_date)
    tmp_p$open_date <- as.Date(tmp_p$open_date, format =  "%d/%m/%Y")
  }
  
  # filter to remove columns
  tmp_p <- tmp_p[, !grepl("_time_|key_stage|early_year|nursery|reception|subsi", names(tmp_p))]
  
  # clean data
  tmp_p <- tmp_p %>% 
    mutate(
      # Convert Character Vector between Encodings
      across(where(is.character), ~iconv(., to = "UTF-8")), 
      # replace spaces and %
      across(where(is.character), ~gsub("^\\s+|\\s+$|%", "", .)), 
      # ‘:’, ‘x’ = Not applicable
      across(where(is.character), ~na_if(., ":")),
      # ‘z’ Unknown
      across(where(is.character), ~na_if(., "z")),
      # ‘NULL’ Unknown
      across(where(is.character), ~na_if(., "NULL")),
      # information unknown
      across(where(is.character), ~na_if(., "Unknown")),
      # suppression: percentage based on 1 or 2 pupils have been suppressed and published as “x"
      across(where(is.character), ~na_if(., "x")),
      # some data not collected from some schools
      across(where(is.character), ~na_if(., "..")),
      # replace empties with NAs
      across(where(is.character), ~na_if(., "")),
      # Upper suppression has been applied where a percentage is greater than 99.0 as ">"
      across(matches("perc_of_pupil"), ~gsub(">", "99.0", .)), 
      # replace remaining ">" in counts with NAs
      across(where(is.character), ~na_if(., ">")),
      # make numeric
      across(matches("headcount|pupil|boarder|infant"), ~as.numeric(.))
    ) %>% 
    as.data.frame()
  
  # remove columns where all rows contain NA values
  tmp_p <- tmp_p[,colSums(is.na(tmp_p)) < nrow(tmp_p)]
  
  # add year
  tmp_p$time_period <- as.numeric(gsub("-20", "", academic_year))
  
  # class size #
  
  # read in class size data
  tmp_c <- read.csv(file = files_classes[i])
  
  # change col names
  names(tmp_c) <- tolower(gsub("X...", "", names(tmp_c), fixed = T))
  names(tmp_c) <- gsub(".", "_", names(tmp_c), fixed = T)
  
  # subset to exclude any non-school level data
  tmp_c <- tmp_c %>% filter(urn != "", urn != 0)
  
  # remove school that was registered twice
  tmp_c <- tmp_c[!(tmp_c$urn == 143840 & tmp_c$estab == 6008), ]
  
  # filter to remove columns
  tmp_c <- tmp_c[, !grepl("key_stage|classes_of_size|exc|lawful|large", names(tmp_c))]
  
  # format open_date if it exists
  if ("opendate" %in% names(tmp_c)) {
    tmp_c$opendate <- ifelse(tmp_c$opendate == "00:00.0" | tmp_c$opendate == "", NA, tmp_c$opendate)
    tmp_c$opendate <- as.Date(tmp_c$opendate, format =  "%d/%m/%Y")
    tmp_c <- rename(tmp_c, open_date = opendate)
  } else if ("open_date" %in% names(tmp_c)) {
    tmp_c$open_date <- ifelse(tmp_c$open_date == "00:00.0" | tmp_c$open_date == "", NA, tmp_c$open_date)
    tmp_c$open_date <- as.Date(tmp_c$open_date, format =  "%d/%m/%Y")
  }
  
  if ("la" %in% names(tmp_c)) {
    tmp_c <- rename(tmp_c, old_la_code = la)
  }
  
  # remove other cols included in pupil level data
  overlap <- intersect(names(tmp_p), names(tmp_c))
  overlap <- setdiff(overlap, "urn") # exclude urn
  
  # clean data
  tmp_c <- tmp_c %>% 
    # drop overlapping columns
    select(-one_of({overlap})) %>% 
    mutate(
      # Convert Character Vector between Encodings
      across(where(is.character), ~iconv(., to = "UTF-8")),
      # replace spaces and %
      across(where(is.character), ~gsub("^\\s+|\\s+$|%", "", .)),
      # ‘:’, ‘x’ = Not applicable
      across(where(is.character), ~na_if(., ":")),
      # ‘z’ Unknown
      across(where(is.character), ~na_if(., "z")),
      # ‘NULL’ Unknown
      across(where(is.character), ~na_if(., "NULL")),
      # information unknown
      across(where(is.character), ~na_if(., "Unknown")),
      # suppression: percentage based on 1 or 2 pupils have been suppressed and published as “x"
      across(where(is.character), ~na_if(., "x")),
      # some data not collected from some schools
      across(where(is.character), ~na_if(., "..")),
      # replace empties with NAs
      across(where(is.character), ~na_if(., "")),
      # replace remaining ">" in counts with NAs
      across(where(is.character), ~na_if(., ">")),
      # make numeric
      across(where(is.character), ~as.numeric(.))
    ) %>%
    as.data.frame()
  
  # remove columns where all rows contain NA values
  tmp_c <- tmp_c[,colSums(is.na(tmp_c)) < nrow(tmp_c)]
  
  # add year
  tmp_c$time_period <- as.numeric(gsub("-20", "", academic_year))
  
  # combine with data on pupils on roll
  tmp <- full_join(tmp_p, tmp_c, by = id_cols) %>%
    #rename(., la_code = "la") %>% 
    rename_with(., ~gsub("_name|_number|_no_|__name_", "", .)) %>% 
    as.data.frame()
  
  if ("typeofestablishment" %in% names(tmp)) {
    tmp <- rename(tmp, type_of_establishment = typeofestablishment)
  }
  
  # combine across years
  if (year == start) {
    spc <- tmp
  } else {
    spc <- rbind.all.columns(spc, tmp)
  }
  
  rm(tmp, tmp_p, tmp_c)
  
}

#### extract relevant data ####

# Clean information on school identifiers #

# read in establishment data
est <- read.csv(file = file.path(dir_data, "data_establishments_search.csv"), na.strings = "")
est <- as.data.frame(fread(file.path(dir_data, "data_establishments_download.csv"), encoding = "UTF-8"))
est <- est[!is.na(est$laestab), ]

# select relevant laestab and urn only and relevant columns
est <- est[!duplicated(est), c("laestab", "urn", "establishmentname")]
names(est) <- c("laestab", "urn_est", "school")

# extract all id pairings
ids <- spc %>% 
  # replace NA in LAESTAB where possible
  mutate(
    laestab = ifelse(is.na(laestab) & !is.na(old_la_code) & !is.na(estab),
                     as.numeric(paste0(old_la_code, estab)),
                     laestab)
  ) %>%
  # select columns
  select(urn, laestab) %>%
  # remove duplicated rows
  filter(!duplicated(.)) %>%
  # identify problematic parings
  mutate(
    urn_match = urn %in% est$urn_est
    ) %>%
  # sort data
  arrange(urn) %>%
  # rename
  rename(urn_spc = urn,
         laestab_spc = laestab) %>%
  as.data.frame()

# create id lookup table for each urn #
id_lookup <- ids %>%
  # FIX URNs #
  left_join(., # add correct urn numbers for urns without a match
            # mapping between urn and laestab for all incorrect urns
            est[est$laestab %in% ids$laestab_spc[ids$urn_match == F], c("laestab", "urn_est")],
            join_by(laestab_spc == laestab)
  ) %>% 
  mutate(
    # combine both urn variables into one with the correct URN numbers
    urn = ifelse(urn_match, urn_spc, urn_est)
    ) %>%
  # FIX LAESTABS #
  left_join(., # get the correct laestab for each urn
            est, join_by(urn == urn_est)) %>%
  # select columns
  select(urn, urn_spc, laestab, school) %>%
  # remove duplicates
  filter(!duplicated(.)) %>%
  as.data.frame()


# fix id information in spc
spc <- spc %>% 
  # remove columns that are uninformative
  select(where(~length(unique(na.omit(.x))) > 1)) %>% 
  # rename original id columns
  rename(urn_spc = urn, laestab_spc = laestab, school_spc = school) %>%
  # add correct ids
  full_join(id_lookup, .) %>%
  # sort data
  arrange(laestab, time_period) %>%
  as.data.frame()


# select subset of columns
df <- spc %>% 
  # select columns
  select(time_period,
         urn,
         laestab, 
         school) %>%
  as.data.frame()
apply(df, 2, function(x){sum(is.na(x))})  
  

# add deprivation data #

add_deprivation_data = F

if (add_deprivation_data) {
  
  # read in file
  depr <- data.table::fread(file.path(dir_misc, "data_deprivation_2019_by_school_postcodes_2010_2023.csv"))
  
  # Income Deprivation Affecting Children Index (IDACI)
  # IDACI decile categorises areas into ten groups (deciles) based on the proportion of children living in income-deprived households. 
  # Each decile represents 10% of areas, with decile 1 being the most deprived and decile 10 being the least deprived.
  
  # change col names
  depr[, school_postcode := Postcode]
  depr[, idaci_decile := `IDACI Decile`]
  
  # subset data
  depr <- depr[`Postcode Status` != "**UNMATCHED**", .(school_postcode, idaci_decile)]
  
  # merge with df
  df <- df %>% 
    full_join(., as.data.frame(depr), by = "school_postcode") %>% as.data.frame()
}

# merge timelines across columns #

# headcount
# Headcount of pupils	= Full-time + part-time pupils
cols_to_merge <- c("headcount_of_pupils__unrounded_", "headcount_of_pupils")
new_col <- "npuptot__spc"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# fte pupils
# Part-time pupils divided by 2 + full-time pupils
cols_to_merge <- c("fte_pupils__unrounded_", "fte_pupils")
new_col <- "fte_pup__spc"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# number of pupils of compulsory school age and above
# Pupils aged 5 and above

cols_to_merge <- c("number_of_pupils_of_compulsory_school_age_and_above__rounded_", 
                   "number_of_pupils_of_compulsory_school_age_and_above")
new_col <- "npupcaa__spc"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# number of pupils female
cols_to_merge <- c("headcount_total_girls", "headcount_total_female", "headcount_total_girls__rounded_")

new_col <- "npupf"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# number of pupils male
cols_to_merge <- c("headcount_total_boys", "headcount_total_male", "headcount_total_boys__rounded_")

new_col <- "npupm"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# # pupil gender clean up
# 
# # some schools have NA for npuptot and NA for npupf [npupm] but 0 for npupm [npupf]
# # the NA was likely caused by replacing the x (statistical suppression) with NAs whereas 0 are correct
# # but 0 for npupm [npupf] are meaningless if npuptot and npupf [npupm] are unknown
# # hence, replace all zeros to allow to remove schools without data later
# 
# df$npupf <- ifelse(df$npupf == 0 & is.na(df$npuptot__spc) & is.na(df$npupm), NA, df$npupf)
# df$npupf_tag <- ifelse(df$npupf == 0 & is.na(df$npuptot__spc) & is.na(df$npupm), NA, df$npupf_tag)
# 
# df$npupm <- ifelse(df$npupm == 0 & is.na(df$npuptot__spc) & is.na(df$npupf), NA, df$npupm)
# df$npupm_tag <- ifelse(df$npupm == 0 & is.na(df$npuptot__spc) & is.na(df$npupf), NA, df$npupm_tag)


# number of pupils known to be eligible for free school meals
cols_to_merge <- c("number_of_pupils_known_to_be_eligible_for_free_school_meals", 
                   "number_of_pupils_known_to_be_eligible_for_and_claiming_free_school_meals")
new_col <- "npupfsm_e"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# number of pupils taking a free school meal on census day	
cols_to_merge <- c("number_of_pupils_taking_free_school_meals", 
                   "number_of_pupils_taking_a_free_school_meal_on_census_day",
                   "number_of_fsm_eligible_pupils_taking_a_free_school_meal_on_census_day")
new_col <- "npupfsm_t"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# Number of pupils (used for FSM calculation in Performance Tables)	
new_col <- "npup_calcspt"
spc[, new_col] <- spc$number_of_pupils__used_for_fsm_calculation_in_performance_tables_
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# number of pupils known to be eligible for free school meals (School Performance Tables)	
new_col <- "npupfsm_e_spt"
spc[, new_col] <- spc$number_of_pupils_known_to_be_eligible_for_free_school_meals__performance_tables_
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# first language #
# number of pupils whose first language is known or believed to be English	
new_col <- "npupenl" # English as a Native Language
spc[, new_col] <- spc$number_of_pupils_whose_first_language_is_known_or_believed_to_be_english
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# number of pupils whose first language is known or believed to be other than English	
new_col <- "npupeal" # English as additional language
spc[, new_col] <- spc$number_of_pupils_whose_first_language_is_known_or_believed_to_be_other_than_english
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# number of pupils whose first language is unclassified	
new_col <- "npupflu" # first language unclassified
spc[, new_col] <- spc$number_of_pupils_whose_first_language_is_unclassified
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# # pupil language clean up
# 
# # some schools have NA for npuptot and NA for npupenl [npupeal] but 0 for npupeal [npupenl]
# # the NA was likely caused by replacing the x (statistical suppression) with NAs whereas 0 are correct
# # but 0 for npupeal [npupenl] are meaningless if npuptot and npupenl [npupeal] are unknown
# # hence, replace all zeros to allow to remove schools without data later
# 
# df$npupenl <- ifelse(df$npupenl == 0 & is.na(df$npuptot__spc) & is.na(df$npupeal), NA, df$npupenl)
# 
# df$npupeal <- ifelse(df$npupeal == 0 & is.na(df$npuptot__spc) & is.na(df$npupenl), NA, df$npupeal)

# ethnic origin #
# Number of pupils by ethnic group	Includes pupils of compulsory school age and above only

# white British ethnic origin
new_col <- "npupeowb" 
spc[, new_col] <- spc$number_of_pupils_classified_as_white_british_ethnic_origin
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# Black ethnic origin

new_col <- "npupeobl" 
tmp <- spc[, grepl("urn|time_period|as_black|as_caribbean|as_african|other_black", names(spc))]
tmp[, new_col] <- rowSums(tmp[, grepl("num", names(tmp))], na.rm = T)
tmp$na_count <- rowSums(is.na(tmp[, grepl("num", names(tmp))]))
tmp[, new_col] <- ifelse(tmp$na_count == 5, NA, tmp[, new_col])
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)

# Asian ethnic origin
new_col <- "npupeoas" 
tmp <- spc[, grepl("urn|time_period|indian|paki|bangl|chin|other_asian", names(spc))]
tmp[, new_col] <- rowSums(tmp[, grepl("num", names(tmp))], na.rm = T)
tmp$na_count <- rowSums(is.na(tmp[, grepl("num", names(tmp))]))
tmp[, new_col] <- ifelse(tmp$na_count == 5, NA, tmp[, new_col])
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)

# ethnic origin unclassified
new_col <- "npupeou" 
spc[, new_col] <- spc$number_of_pupils_unclassified
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)


rm(tmp)
gc()


# total number of classes taught by one teacher
# one teacher classes as taught during a single selected period in each school on the day of the census
# data cleaning: replace all zeros with NAs as 0 suggests that no classes were taught by one teacher, unlikely!
spc <- spc %>% 
  mutate(across(matches("classes_taught"), ~ifelse(. == 0, NA, .))) %>% as.data.frame()

cols_to_merge <- c("total_of_classes_taught_by_one_teacher",
                   "total_of_primary_classes_taught_by_one_teacher",
                   "total_of_classes_in_primary_schools_taught_by_one_teacher",
                   "total_of_secondary_classes_taught_by_one_teacher",
                   "total_of_classes_in_secondary_schools_taught_by_one_teacher")
new_col <- "nclt1t"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# total number of pupils in classes taught by one teacher
# already replaced all zeros with NAs with call above
cols_to_merge <- c("total_of_pupils_in_classes_taught_by_one_teacher",
                   "total_of_pupils_in_primary_classes_taught_by_one_teacher",
                   "total_of_pupils_in_classes_in_primary_schools_taught_by_one_teacher",
                   "total_of_pupils_in_secondary_classes_taught_by_one_teacher",
                   "total_of_pupils_in_classes_in_secondary_schools_taught_by_one_teacher")
new_col <- "npupclt1t"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# average size of one teacher classes = Number of pupils divided by number of classes
# replace all zeros with NAs
spc <- spc %>% 
  mutate(across(matches("average"), ~ifelse(. == 0, NA, .))) %>% as.data.frame()

cols_to_merge <- c("average_size_of_one_teacher_classes", 
                   "average_size_of_one_teacher_primary_classes",
                   "average_size_of_one_teacher_classes_in_primary_schools",
                   "average_size_of_one_teacher_secondary_classes",
                   "average_size_of_one_teacher_classes_in_secondary_schools")
new_col <- "avgclsize"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)


#### save data ####


# remove duplicates
df <- df[!duplicated(df), ]


# remove all rows that do not have 
df <- df %>%
  ungroup() %>%
  # count NAs in each row
  mutate(na_count = rowSums(is.na(df))) %>%
  # remove empty rows (i.e., 29 NA per row)
  filter(na_count != 29) %>%
  # remove rows that have 0 in npup_calcspt and no other obs
  filter(!(npup_calcspt == 0 & na_count == 28)) %>%
  select(-na_count) %>%
  as.data.frame()

# GET URN NUMBERS #

get_urns <- F

if (get_urns) {
  
  # Step 1: Initial Input
  
  # define academic years
  academic_years <- lookup$academic_year
  
  # get school identifiers from df
  urn_list <- unique(df$urn)
  laestab_list <- unique(df$laestab)

  # read in establishment data
  est <- read.csv(file = file.path(dir_data, "data_establishments_search.csv"), na.strings = "")
  est <- est[!is.na(est$laestab), ]
  
  # select relevant laestab and urn only and relevant columns
  est <- est[est$laestab %in% laestab_list | est$urn %in% urn_list, c("laestab", "urn", "opendate", "closedate")]
  
  # Step 3a: Identify Schools with Single Entries
  
  laestab_s <- est %>%
    group_by(laestab) %>%
    summarise(n = n()) %>%
    filter(n == 1) %>%
    pull(laestab)
  
  # Step 3b: Identify Schools with Multiple Entries
  
  laestab_m <- est %>%
    group_by(laestab) %>%
    summarise(n = n()) %>%
    filter(n > 1) %>%
    pull(laestab)
  
  # Step 4: process schools with single entry
  
  scaffold <- merge(data.frame(time_period = academic_years),
                    data.frame(laestab = laestab_s))
  urn_s <- est %>%
    filter(laestab %in% laestab_s) %>%
    select(-c(opendate, closedate)) %>%
    full_join(scaffold, ., by = "laestab") %>% 
    as.data.frame()
  
  # Step 5: process schools with multiple entries
  urn_m <- create_urn_df(est[est$laestab %in% laestab_m, ], 2010, 2023)

  # combine schools with single and multiple entries
  urn <- rbind(urn_s, urn_m)
  
  # fix time_period
  urn$time_period <- plyr::mapvalues(urn$time_period, lookup$academic_year, lookup$time_period, warn_missing = TRUE)
  
  # save urns
  data.table::fwrite(urn, file = file.path(dir_misc, "data_spc_urn.csv"), row.names = F)
  
} else {
  
  # read in urn data
  urn <- read.csv(file = file.path(dir_misc, "data_spc_urn.csv"))
}


# COMBINE ALL DFs #
gc()


# save file
df <- df[with(df, order(laestab, time_period)),]
data.table::fwrite(df, file = file.path(dir_data, "data_spc.csv"), row.names = F)

#### create var dict ####
dict <- data.frame(variable = names(df)[!grepl("_tag", names(df))])
dict$explanation <- c("academic year",
                      "unique reference number",
                      "Establishment (ESTAB) number",
                      "name of school",

                      "headcount pupils",
                      "FTE pupils",
                      "number of pupils of compulsary age and above",
                      "number of pupils female",
                      "number of pupils male",
                      "number of pupils eligible for FSM",
                      "number of pupils taking FSM",
                      "number of pupils (SPT)",
                      "number of pupils eligible for FSM (SPT)",
                      "number of English as native language pupils",
                      "number of English as additional language pupils",
                      "number of pupils with unclassified first language",
                      "number of pupils classified as white British ethnic origin",
                      "number of pupils classified as Black ethnic origin",
                      "number of pupils classified as Asian ethnic origin",
                      "number of pupils unclassified ethnic origin",
                      "number of classes taught by one teacher",
                      "number of pupils in classes taught by one teacher",
                      "average class size")
# save file
write.csv(dict, file = file.path(dir_misc, "meta_spc.csv"), row.names = F)

#### extract postcodes and process deprivation data ####

extract_postcodes = F

if(extract_postcodes){
  # extract
  pcd <- as.data.frame(na.omit(unique(spc$school_postcode)))
  
  # Define a function to save data in chunks
  save_in_chunks <- function(data, chunk_size = 10000, file_prefix = "extracted_data", output_folder = ".") {
    
    # get data dimensions
    n <- nrow(data)
    num_chunks <- ceiling(n / chunk_size)
    
    # Ensure the output folder exists
    if (!dir.exists(output_folder)) {
      dir.create(output_folder, recursive = TRUE)
    }
    
    # save each chunk as csv
    for (i in seq_len(num_chunks)) {
      start_row <- (i - 1) * chunk_size + 1
      end_row <- min(i * chunk_size, n)
      chunk <- data[start_row:end_row, ]
      file_name <- file.path(output_folder, paste0(file_prefix, "_part", i, ".csv"))
      write.table(chunk, file_name, row.names = FALSE, col.names = FALSE, quote = FALSE, sep = ",")
    }
  }
  # save data in chunks
  save_in_chunks(pcd, chunk_size = 10000, file_prefix = "school_postcodes_2010_2023", output_folder = dir_misc)
  
}

# outside of script, upload files to https://imd-by-postcode.opendatacommunities.org/imd/2019 to get deprivation data

# process deprivation data
process_deprivation_data = F

if(process_deprivation_data){
  
  # list files
  file_list <- list.files(path = dir_misc, pattern = "2019-deprivation", full.names = T)
  
  # read and row bind csv files
  depr <- data.table::rbindlist(lapply(file_list, data.table::fread))
  
  # file file
  data.table::fwrite(depr, file.path(dir_misc, "data_deprivation_2019_by_school_postcodes_2010_2023.csv"))
  
}

