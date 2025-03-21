# School Workforce Census (SWC) #

# Data source: SWC 2023 (November).

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
dir_in <- file.path(dir_data, "school-workforce-in-england")

# determine year list (akin to other data sources)
years_list <- paste0(20, 10:23, 11:24)
id_cols <- c("time_period", "urn")

# Pupil to teacher ratios - school level #

# read in data
ptrs <- read.csv(file.path(dir_in, "2023", "data", "workforce_ptrs_2010_2023_sch.csv"))

ptrs <- ptrs %>%
  # rename columns 
  rename_with(., ~tolower(gsub("X...", "", ., fixed = T))) %>% 
  rename(urn = school_urn, laestab = school_laestab) %>%
  rename_with(., ~gsub("_name", "", .)) %>%
  # remove columns that are uninformative
  select(where(~length(unique(na.omit(.x))) > 1)) %>%
  as.data.frame()

# # select columns
# ptrs <- ptrs[, grepl("time_period|urn|fte|ratio", names(ptrs))]

# Teacher absences - school level #

# read in data
abs <- read.csv(file.path(dir_in, "2023", "data", "sickness_absence_teachers_sch.csv"))
names(abs) <- tolower(gsub("X...", "", names(abs), fixed = T))
names(abs)[names(abs) == "school_urn"] <- "urn"

# select columns
abs <- abs[, grepl("time_period|urn|abs|day", names(abs))]

# remove duplicated rows
abs <- abs[!duplicated(abs), ]

# Teacher pay - school level #

# read in data
pay <- read.csv(file.path(dir_in, "2023", "data", "workforce_teacher_pay_2010_2023_school.csv"))
names(pay) <- tolower(gsub("X...", "", names(pay), fixed = T))
names(pay)[names(pay) == "school_urn"] <- "urn"

# select columns
pay <- pay[, grepl("time_period|urn|mean|headcount|pay", names(pay))]

# remove duplicated rows
pay <- pay[!duplicated(pay), ]

# Teacher vacancies - school level #

# read in data
vac <- read.csv(file.path(dir_in, "2023", "data", "vacancies_number_rate_sch_2010_2023.csv"))
names(vac) <- tolower(gsub("X...", "", names(vac), fixed = T))
names(vac)[names(vac) == "school_urn"] <- "urn"

# select columns
vac <- vac[, grepl("time_period|urn|vac|rate|tmp", names(vac))]

# Size of the school workforce - school level #

# read in data
swf <- read.csv(file.path(dir_in, "2023", "data", "workforce_2010_2023_fte_hc_nat_reg_la_sch.csv"))
names(swf) <- tolower(gsub("X...", "", names(swf), fixed = T))
names(swf)[names(swf) == "school_urn"] <- "urn"

# select columns
swf <- swf[, grepl("time_period|urn|teach|business|admin", names(swf))]
swf <- swf[, !grepl("fte_ft|fte_pt|hc_pt|hc_ft|leader|head", names(swf))]

# Workforce teacher characteristics - school level #

pattern_wtc <- "workforce_teacher_characteristics_school_2010_2023"
dir_wtc <- file.path(dir_in, "2023", "supporting-files")
dir_tmp <- file.path(dir_wtc, pattern_wtc)

unzip = F
if (unzip) {
  # determine zipped folder
  zipped_folder <- list.files(path = dir_wtc, pattern = pattern_wtc, full.names = T)
  
  # create output folder
  dir.create(dir_tmp)
  
  # unzip
  unzip(zipped_folder, exdir = dir_tmp)
  
}


# create vector with file names
files <- list.files(path = dir_tmp, full.names = T)
files <- files[!grepl("meta", files)]

# determine cols to keep
cols_to_keep <- c(id_cols,
                  "gender", "age_group", "ethnicity_major",
                  "grade", "working_pattern", "qts_status", "on_route",
                  "full_time_equivalent", "headcount", "fte_school_percent", "headcount_school_percent")

for (f in 1:length(files)) {
  
  # read in data
  tmp <- read.csv(file = files[f])
  names(tmp) <- tolower(gsub("X...", "", names(tmp), fixed = T))
  
  # subset data to only include relevant schools
  names(tmp)[names(tmp) == "school_urn"] <- "urn"
  # tmp <- tmp %>% filter(urn %in% urn_list)
  
  # select relevant columns
  tmp <- tmp[, names(tmp) %in% cols_to_keep]
  
  # combine across years
  if (f == 1) {
    wtc <- tmp
  } else {
    wtc <- rbind.all.columns(wtc, tmp)
  }
  
}

# rename columns
names(wtc) <- gsub("headcount", "hc", names(wtc))
names(wtc) <- gsub("school_percent", "perc", names(wtc))
names(wtc) <- gsub("full_time_equivalent", "fte", names(wtc))

# replace values 
wtc<- wtc %>%
  mutate(across(matches("fte|hc"), ~na_if(., "x"))) %>% # x = not available - information has not been collected or there are no estimates available at this level of aggregation.
  mutate(across(matches("fte|hc"), ~na_if(., "z"))) %>% # z = not applicable - statistic cannot be produced. For example where a denominator is not available to produce a percentage.
  mutate(across(matches("fte|hc"), ~na_if(., "c"))) %>% # c = confidential - where presentation of data would disclose confidential information
  mutate(across(matches("fte|hc"), ~na_if(., "u"))) %>% # u = low reliability - values of the potentially low quality, for example where values of statistical significance have been calculated.
  # remove the comma and then convert the resulting string to a numeric type
  mutate(across(matches("fte|hc"), ~as.numeric(gsub(",", "", .)))) %>%
  # replace spaces
  mutate(across(where(is.character), ~gsub(" ", "_", .))) %>%
  as.data.frame()

# determine value variables
values <- c("hc", "fte", "hc_perc", "fte_perc")
# values <- c("hc")

# make into wide format
tmp <- wtc %>% 
  # TOTALS
  filter_at(vars(!matches("time|urn|fte|hc")), all_vars(. == "Total")) %>%
  select(all_of(c(id_cols, "hc", "fte"))) %>%
  right_join( # GENDER
    wtc %>%
      # filter(gender != "Total", gender != "Gender_Unclassified") %>%
      filter(gender != "Total") %>%
      tidyr::pivot_wider(id_cols = {id_cols},
                         names_from = gender,
                         names_glue = "{.value}_gender_{gender}",
                         values_from = {values}),
    by = id_cols) %>%
  right_join( # AGE
    wtc %>% 
      # filter(age_group != "Total", age_group != "Age_unclassified") %>%
      filter(age_group != "Total") %>%
      tidyr::pivot_wider(id_cols = {id_cols},
                         names_from = age_group,
                         names_glue = "{.value}_age_{age_group}",
                         values_from = {values}),
    by = id_cols) %>%
  right_join( # ETHNICITY
    wtc %>% 
      # filter(ethnicity_major != "Total", ethnicity_major != "Information_not_yet_obtained", ethnicity_major != "Refused") %>%
      filter(ethnicity_major != "Total") %>%
      tidyr::pivot_wider(id_cols = {id_cols},
                         names_from = ethnicity_major,
                         names_glue = "{.value}_ethnicity_{ethnicity_major}",
                         values_from = {values}),
    by = id_cols) %>%
  right_join( # GRADE
    wtc %>% 
      filter(grade != "Total") %>%
      tidyr::pivot_wider(id_cols = {id_cols},
                         names_from = grade,
                         names_glue = "{.value}_grade_{grade}",
                         values_from = {values}),
    by = id_cols) %>%
  right_join( # WORKING PATTERN
    wtc %>% 
      filter(working_pattern != "Total") %>%
      tidyr::pivot_wider(id_cols = {id_cols},
                         names_from = working_pattern,
                         names_glue = "{.value}_pattern_{working_pattern}",
                         values_from = {values}),
    by = id_cols) %>%
  right_join( # QTS STATUS
    wtc %>% 
      filter(qts_status != "Total") %>%
      tidyr::pivot_wider(id_cols = {id_cols},
                         names_from = qts_status,
                         names_glue = "{.value}_qts_{qts_status}",
                         values_from = {values}),
    by = id_cols) %>% 
  mutate(
    # fill gaps in total data
    tmp = rowSums(across(matches("hc_grade")), na.rm = T),
    hc = ifelse(is.na(hc), tmp, hc),
    tmp = rowSums(across(matches("fte_grade")), na.rm = T),
    fte = ifelse(is.na(fte), tmp, fte),
    
    # fill NAs with zeros where possible
    tmp = rowSums(across(matches("hc_gender_")), na.rm = T),
    across(matches("hc_gender_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_gender_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_gender_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_gender_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    
    tmp = rowSums(across(matches("hc_age_")), na.rm = T),
    across(matches("hc_age_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_age_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_age_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_age_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    
    tmp = rowSums(across(matches("hc_ethnicity_")), na.rm = T),
    across(matches("hc_ethnicity_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_ethnicity_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_ethnicity_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_ethnicity_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    
    tmp = rowSums(across(matches("hc_grade_")), na.rm = T),
    across(matches("hc_grade_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_grade_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_grade_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_grade_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    
    tmp = rowSums(across(matches("hc_pattern_")), na.rm = T),
    across(matches("hc_pattern_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_pattern_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_pattern_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_pattern_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    
    tmp = rowSums(across(matches("hc_qts_")), na.rm = T),
    across(matches("hc_qts_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_qts_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_qts_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_qts_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    tmp = NULL, 
    
    # compute aggregates
    hc_age_under_30 = rowSums(select(., "hc_age_Under_25", "hc_age_25_to_29"), na.rm = T),
    hc_perc_age_under_30 = hc_age_under_30/hc * 100,
    fte_age_under_30 = rowSums(select(., "fte_age_Under_25", "fte_age_25_to_29"), na.rm = T),
    fte_perc_age_under_30 = fte_age_under_30/fte * 100,
    
    hc_age_30_to_49 = rowSums(select(., "hc_age_30_to_39", "hc_age_40_to_49"), na.rm = T),
    hc_perc_age_30_to_49 = hc_age_30_to_49/hc * 100,
    fte_age_30_to_49 = rowSums(select(., "fte_age_30_to_39", "fte_age_40_to_49"), na.rm = T),
    fte_perc_age_30_to_49 = fte_age_30_to_49/fte * 100,
    
    hc_age_50_and_over = rowSums(select(., "hc_age_50_to_59", "hc_age_60_and_over"), na.rm = T),
    hc_perc_age_50_and_over = hc_age_50_and_over/hc * 100,
    fte_age_50_and_over = rowSums(select(., "fte_age_50_to_59", "fte_age_60_and_over"), na.rm = T),
    fte_perc_age_50_and_over = fte_age_50_and_over/fte * 100,
    
    # estimate average age of teachers at a school
    hc_avg_age      = (hc_age_Under_25 * 22.5 + hc_age_25_to_29 * 27 + hc_age_30_to_39 * 34.5 + 
      hc_age_40_to_49 * 44.5 + hc_age_50_to_59 * 54.5 + hc_age_60_and_over * 62.5)/hc,
    fte_avg_age     = (fte_age_Under_25 * 22.5 + fte_age_25_to_29 * 27 + fte_age_30_to_39 * 34.5 + 
      fte_age_40_to_49 * 44.5 + fte_age_50_to_59 * 54.5 + fte_age_60_and_over * 62.5)/fte
    
  ) %>% #as.data.frame()
  # select(matches("time|urn|Female|White|British|Classroom|hc|fte|age")) %>%
  select(matches("time|urn|Female|White|British|Classroom|age")) %>%
  select(matches("time_period|urn|fte_perc|avg_age")) %>%
  as.data.frame()

apply(tmp, 2, function(x) {sum(is.na(x))})

# tmpp <- tmp[, grepl("time_p|urn|fte", names(tmp))]
# tmpp <- tmpp[, !grepl("gender|ethn|perc|grade|pattern|qts", names(tmpp))]
# head(tmpp)

# combine all df #

urn_list <- unique(c(ptrs$urn, pay$urn, abs$urn, vac$urn, swf$urn, tmp$urn))

# create scaffold to safe data
scaffold <- merge(data.frame(time_period = as.numeric(years_list)),
                  data.frame(urn = urn_list))


# process data
df <- scaffold %>%
  # merge all dfs
  full_join(., ptrs, by = id_cols) %>%
  full_join(., pay, by = id_cols) %>% 
  full_join(., abs, by = id_cols) %>%
  full_join(., vac, by = id_cols) %>%
  full_join(., swf, by = id_cols) %>%
  full_join(., tmp, by = id_cols) %>%
  # replace with NAs
  mutate(
    # replace spaces and %
    across(where(is.character), ~gsub("^\\s+|\\s+$|%", "", .)),
    # x = not available - information has not been collected or there are no estimates available at this level of aggregation.
    across(where(is.character), ~na_if(., "x")), 
    # z = not applicable - statistic cannot be produced. For example where a denominator is not available to produce a percentage.
    across(where(is.character), ~na_if(., "z")), 
    # c = confidential - where presentation of data would disclose confidential information
    across(where(is.character), ~na_if(., "c")), 
    # u = low reliability - values of the potentially low quality, for example where values of statistical significance have been calculated.
    across(where(is.character), ~na_if(., "u")), 
    # replace comma and make numeric
    across(pupils_fte:last_col(), \(x) as.numeric(gsub(",", "", x)))) %>%
  # make colnames lower case
  rename_with(., tolower) %>%
  # group by schools
  group_by(urn) %>%
  mutate(
    # fill missing values: observations to be carried backward
    across(c(region_code, region, old_la_code, new_la_code, la, laestab, school, school_type),
           ~zoo::na.locf(., na.rm = FALSE, fromLast = TRUE)),
    # fill missing values: observations to be carried forward
    across(c(region_code, region, old_la_code, new_la_code, la, laestab, school, school_type),
           ~zoo::na.locf(., na.rm = FALSE, fromLast = FALSE)))  %>%
  ungroup() %>%
  # re-compute ratios
  mutate(pupil_to_qual_teacher_ratio = pupils_fte / qualified_teachers_fte ,
         pupil_to_qual_unqual_teacher_ratio = pupils_fte / teachers_fte,
         pupil_to_adult_ratio = pupils_fte / adults_fte) %>%
  # sort data
  arrange(urn, time_period) %>% as.data.frame()

# save data
#df <- df[with(df, order(urn, time_period)),]
data.table::fwrite(df, file = file.path(dir_data, "data_swf.csv"), row.names = F)
