# Schools, pupils and their characteristics #

# This release contains the latest statistics on school and pupil numbers and their characteristics, 
# including age, gender, free school meals (FSM) eligibility, English as an additional language, ethnicity, school characteristics, class sizes.
# The publication combines information from the school census, school level annual school census, 
# general hospital school census and alternative provision census.

# The most recently published data is from the school census which took place on 18th January 2024 (Spring census)

options(scipen = 999)
# empty work space
rm(list = ls())

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
  tmp_p <- tmp_p %>% filter(urn != "", urn != 0)
  
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
  tmp_p <- tmp_p[, !grepl("_time_|unclassified|key_stage|early_year|nursery|reception|subsi", names(tmp_p))]
  
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

# create scaffold to safe data
urn_list <- unique(spc$urn)

scaffold <- merge(data.frame(time_period = as.numeric(years_list)),
                  data.frame(urn = urn_list))

# only select columns that have more than 1 unique observation
df <- spc %>% 
  # remove columns that are uninformative
  select(where(~length(unique(na.omit(.x))) > 1)) %>% 
  # make date a date again
  mutate(open_date = as.Date(open_date, origin = "1970-01-01")) %>%
  # select columns
  select(time_period,
         urn,  
         estab, laestab,
         school, 
         school_postcode,
         region, region_code,
         urban_rural,
         parl_con_code, parl_con, # Parliamentary Constituency
         new_la_code, old_la_code, la, # Local Authority
         district_administrative_code, district_administrative, # district
         ward_code, ward, # Ward is a subdivision of a local authority area or district, typically used for electoral purposes of local council
         cas_ward_code, cas_ward, # Census Ward
         open_date,
         phase_of_education, school_type, phase_type_grouping, type_of_establishment, academy_flag, trust,
         form_7_school_type, form_7_school_type_description, middle_school,
         school_size,
         sex_of_school_description, denomination, admissions_policy
  ) %>%
  # group by schools
  group_by(urn) %>%
  mutate(
    # fill missing values: observations to be carried backward
    across(c(estab, laestab,
             school,
             school_postcode,
             region, region_code,
             urban_rural,
             parl_con_code, parl_con,
             new_la_code, old_la_code, la,
             district_administrative_code, district_administrative,
             ward_code, ward,
             cas_ward_code, cas_ward,
             open_date,
             school_type, phase_type_grouping,
             type_of_establishment,
             sex_of_school_description, denomination, admissions_policy, middle_school),
           ~zoo::na.locf(., na.rm = FALSE, fromLast = TRUE)),
    # fill missing values: observations to be carried forward
    across(c(estab, laestab,
             school,
             school_postcode,
             region, region_code,
             urban_rural,
             parl_con_code, parl_con,
             new_la_code, old_la_code, la,
             district_administrative_code, district_administrative,
             ward_code, ward,
             cas_ward_code, cas_ward,
             open_date,
             phase_of_education, school_type,
             phase_type_grouping, type_of_establishment,
             sex_of_school_description, denomination, admissions_policy,
             form_7_school_type, form_7_school_type_description, middle_school),
           ~zoo::na.locf(., na.rm = FALSE, fromLast = FALSE)))  %>%
  ungroup() %>%
  # save the specific columns as CSV
  {data.table::fwrite(select(., time_period, urn, laestab, old_la_code, estab, school, school_postcode), 
                      file.path(dir_data,"data_identifiers.csv")); .} %>%
  # join with scaffold
  full_join(x = scaffold, y = ., by = id_cols) %>%
  # sort data
  arrange(urn, time_period) %>%
  as.data.frame()


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

# number of pupils known to be eligible for free school meals
cols_to_merge <- c("number_of_pupils_known_to_be_eligible_for_free_school_meals", 
                   "number_of_pupils_known_to_be_eligible_for_and_claiming_free_school_meals")
new_col <- "npupfsm_e"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# % of pupils known to be eligible for free school meals	
# Number of pupils know to be eligible for FSM expressed as a percentage of the total number of pupils

cols_to_merge <- c("perc_of_pupils_known_to_be_eligible_for_free_school_meals", 
                   "perc_of_pupils_known_to_be_eligible_for_and_claiming_free_school_meals")
new_col <- "pnpupfsm_e"

df <- merge_timelines_across_columns(data_in = spc, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)


# new_col_p <- "pnpupfsm_e"
# df[, new_col_p] <- df[, new_col] / df$npuptot__spc * 100

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

# percentage of pupils taking a free school meal on census day	
# new_col_p <- "pnpupfsm_t"
#df[, new_col_p] <- df[, new_col] / df$npuptot__spc * 100

cols_to_merge <- c("perc_of_pupils_taking_free_school_meals",
                   "perc_of_fsm_eligible_pupils_taking_free_school_meals",
                   "perc_of_pupils_taking_free_school_meals_on_census_day")
new_col <- "pnpupfsm_t"
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

# percentage of pupils known to be eligible for free school meals (School Performance Tables)	
# Number of pupils know to be eligible for FSM (School Performance Tables) expressed as a percentage of the total number of pupils (used for FSM calculation in Performance Tables)
new_col <- "pnpupfsm_e_spt"
spc[, new_col] <- spc$perc_of_pupils_known_to_be_eligible_for_free_school_meals__performance_tables_
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# number of pupils whose first language is known or believed to be other than English	
new_col <- "npupeal"
spc[, new_col] <- spc$number_of_pupils_whose_first_language_is_known_or_believed_to_be_other_than_english
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# % of pupils whose first language is known or believed to be other than English
# First language category expressed as a percentage of the total number of pupils of compulsory school age and above
# new_col_p <- "pnpupeal"
# df[, new_col_p] <- df[, new_col] / df[, "npupcaa__spc"] * 100

new_col <- "pnpupeal"
spc[, new_col] <- spc$perc_of_pupils_whose_first_language_is_known_or_believed_to_be_other_than_english
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# ethnic origin #
# Number of pupils by ethnic group	Includes pupils of compulsory school age and above only

# white British ethnic origin
new_col <- "numeowb" 
spc[, new_col] <- spc$number_of_pupils_classified_as_white_british_ethnic_origin
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

new_col <- "pnumeowb" 
spc[, new_col] <- spc$perc_of_pupils_classified_as_white_british_ethnic_origin
df <- merge(df, spc[, c(id_cols, new_col)], by = id_cols, all = T)

# new_col_p <- "pnumeowb" 
# df[, new_col_p] <- df[, new_col] / df[, "npupcaa__spc"] * 100

# Black ethnic origin
new_col <- "numeobl" 
tmp <- spc[, grepl("urn|time_period|as_caribbean|as_african|other_black", names(spc))]
tmp[, new_col] <- rowSums(tmp[, grepl("num", names(tmp))], na.rm = T)
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)

# new_col_p <- "pnumeobl" 
# df[, new_col_p] <- df[, new_col] / df[, "npupcaa__spc"] * 100

# Asian ethnic origin
new_col <- "numeoas" 
tmp <- spc[, grepl("urn|time_period|indian|paki|bangl|chin|other_asian", names(spc))]
tmp[, new_col] <- rowSums(tmp[, grepl("num", names(tmp))], na.rm = T)
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)

# new_col_p <- "pnumeoas" 
# df[, new_col_p] <- df[, new_col] / df[, "npupcaa__spc"] * 100

# total number of classes taught by one teacher
# one teacher classes as taught during a single selected period in each school on the day of the census
# replace all zeros with NAs
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
df <- df[with(df, order(urn, time_period)),]
data.table::fwrite(df, file = file.path(dir_data, "data_spc.csv"), row.names = F)

#### create var dict ####
dict <- data.frame(variable = names(df)[!grepl("_tag", names(df))])
dict$explanation <- c("academic year",
                      "unique reference number",
                      "Establishment (ESTAB) number",
                      "Establishment (ESTAB) number within its local authority",
                      "name of school",
                      "postcode of school",
                      "region name",
                      "region code",
                      "urban vs. rural",
                      "parlimentary constituency code",
                      "parlimentary constituency name",
                      "local authority code - old",
                      "local authority code - new",
                      "local authority name",
                      "disctrict code",
                      "disctrict name",
                      "ward code",
                      "ward name",
                      "cas ward code",
                      "cas ward name",
                      "date school opened",
                      "stage or level of education that a school provides",
                      "classification of schools based on their organisational structure and the education",
                      "categorises schools based on the phase of education they provide and groups them into broader categories for analysis and reporting purposes",
                      "specific classification of a school based on its governance, funding, and educational provision",
                      "is academy?",
                      "belongs to trust",
                      "form 7 classification of schools",
                      "form 7 classification of schools",
                      "middle school, bridge between primary (elementary) and secondary (high) education",
                      "size of school (grouped)",
                      "single sex or mixed school",
                      "religious denomination of school",
                      "admissions policy",
                      "headcount pupils",
                      "FTE pupils",
                      "number of pupils of compulsary age and above",
                      "number of pupils eligible for FSM",
                      "perc of pupils eligible for FSM",
                      "number of pupils taking FSM",
                      "perc of pupils taking FSM",
                      "number of pupils (SPT)",
                      "number of pupils eligible for FSM (SPT)",
                      "perc of pupils eligible for FSM (SPT)",
                      "number of EAL pupils",
                      "perc of EAL pupils",
                      "number of pupils classified as white British ethnic origin",
                      "perc of pupils classified as white British ethnic origin",
                      "number of pupils classified as Black ethnic origin",
                      #"perc of pupils classified as Black ethnic origin",
                      "number of pupils classified as Asian ethnic origin",
                      #"perc of pupils classified as Asian ethnic origin",
                      "number of classes taught by one teacher",
                      "number of pupils in classes taught by one teacher",
                      "average class size")
# save file
write.csv(dict, file = file.path(dir_misc, "meta_spc.csv"), row.names = F)

#### extract postcodes ####

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

