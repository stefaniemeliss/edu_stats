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
lookup <- data.frame(time_period = as.numeric(years_list),
                     academic_year = as.numeric(substr(years_list, 1, 4)))
id_cols <- c("time_period", "urn")
id_cols <- c("time_period", "urn", "laestab")
id_cols <- c("time_period", "laestab")

get_urns <- F

# Pupil to teacher ratios - school level #

# read in data
ptr <- read.csv(file.path(dir_in, "2023", "data", "workforce_ptrs_2010_2023_sch.csv"))

ptr <- ptr %>%
  # rename columns 
  rename_with(., ~tolower(gsub("X...", "", ., fixed = T))) %>% 
  rename(urn_ptr = school_urn, laestab = school_laestab) %>%
  rename_with(., ~gsub("_name", "", .)) %>%
  # remove columns that are uninformative
  select(where(~length(unique(na.omit(.x))) > 1)) %>%
  as.data.frame()

# select columns
ptr <- ptr[, grepl("time_period|urn|laestab|fte|ratio|school|la|region|type", names(ptr))]
ptr <- ptr[, !grepl("code|number", names(ptr))]

# Teacher absences - school level #

# read in data
abs <- read.csv(file.path(dir_in, "2023", "data", "sickness_absence_teachers_sch.csv"))
names(abs) <- tolower(gsub("X...", "", names(abs), fixed = T))
names(abs)[names(abs) == "school_urn"] <- "urn_abs"
names(abs)[names(abs) == "school_laestab"] <- "laestab"

# select columns
abs <- abs[, grepl("time_period|urn|laestab|abs|day", names(abs))]

# remove duplicated rows
abs <- abs[!duplicated(abs), ]

# Teacher pay - school level #

# read in data
pay <- read.csv(file.path(dir_in, "2023", "data", "workforce_teacher_pay_2010_2023_school.csv"))
names(pay) <- tolower(gsub("X...", "", names(pay), fixed = T))
names(pay)[names(pay) == "school_urn"] <- "urn_pay"
names(pay)[names(pay) == "school_laestab"] <- "laestab"

# select columns
pay <- pay[, grepl("time_period|urn|laestab|mean|headcount|pay", names(pay))]

# remove duplicated rows
pay <- pay[!duplicated(pay), ]

# Teacher vacancies - school level #

# read in data
vac <- read.csv(file.path(dir_in, "2023", "data", "vacancies_number_rate_sch_2010_2023.csv"))
names(vac) <- tolower(gsub("X...", "", names(vac), fixed = T))
names(vac)[names(vac) == "school_urn"] <- "urn_vac"
names(vac)[names(vac) == "school_laestab"] <- "laestab"

# select columns
vac <- vac[, grepl("time_period|urn|laestab|vac|rate|tmp", names(vac))]

# Size of the school workforce - school level #

# read in data
swf <- read.csv(file.path(dir_in, "2023", "data", "workforce_2010_2023_fte_hc_nat_reg_la_sch.csv"))
names(swf) <- tolower(gsub("X...", "", names(swf), fixed = T))
names(swf)[names(swf) == "school_urn"] <- "urn_swf"
names(swf)[names(swf) == "school_laestab"] <- "laestab"

# select columns
swf <- swf[, grepl("time_period|urn|laestab|teach|business|admin", names(swf))]
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
cols_to_keep <- c(id_cols, "urn_wtc",
                  "characteristic_group", "characteristic",
                  "gender", "age_group", "ethnicity_major",
                  "grade", "working_pattern", "qts_status", "on_route",
                  "full_time_equivalent", "headcount", "fte_school_percent", "headcount_school_percent")

for (f in 1:length(files)) {
  
  # read in data
  tmp <- read.csv(file = files[f])
  names(tmp) <- tolower(gsub("X...", "", names(tmp), fixed = T))
  
  # subset data to only include relevant schools
  names(tmp)[names(tmp) == "school_urn"] <- "urn_wtc"
  names(tmp)[names(tmp) == "school_laestab"] <- "laestab"
  # tmp <- tmp %>% filter(urn %in% urn_list)
  
  # select relevant columns
  tmp <- tmp[, names(tmp) %in% cols_to_keep]
  
  # combine across years
  if (f == 1) {
    teach_char <- tmp
  } else {
    teach_char <- rbind.all.columns(teach_char, tmp)
  }
  
}

gc()
# rename columns
names(teach_char) <- gsub("headcount", "hc", names(teach_char))
names(teach_char) <- gsub("school_percent", "perc", names(teach_char))
names(teach_char) <- gsub("full_time_equivalent", "fte", names(teach_char))

# replace values 
teach_char <- teach_char %>%
  mutate(across(matches("fte|hc"), ~na_if(., "x"))) %>% # x = not available - information has not been collected or there are no estimates available at this level of aggregation.
  mutate(across(matches("fte|hc"), ~na_if(., "z"))) %>% # z = not applicable - statistic cannot be produced. For example where a denominator is not available to produce a percentage.
  mutate(across(matches("fte|hc"), ~na_if(., "c"))) %>% # c = confidential - where presentation of data would disclose confidential information
  mutate(across(matches("fte|hc"), ~na_if(., "u"))) %>% # u = low reliability - values of the potentially low quality, for example where values of statistical significance have been calculated.
  # remove the comma and then convert the resulting string to a numeric type
  mutate(across(matches("fte|hc"), ~as.numeric(gsub(",", "", .)))) %>%
  # replace spaces
  mutate(across(where(is.character), ~gsub(" ", "_", .))) %>%
  # re-compute the percentages 
  # determine total
  mutate(
    hc_total = hc[characteristic_group == "Total" & characteristic == "Total"],
    fte_total = fte[characteristic_group == "Total" & characteristic == "Total"],
    .by = c(time_period, laestab, urn_wtc)
  ) %>%
  # re-compute percentages
  mutate(
    hc_perc = hc / hc_total * 100,
    fte_perc = fte / fte_total * 100
  ) %>%
  # drop total columns
  select(-c(hc_total, fte_total)) %>%
  as.data.frame()


# determine value variables
values <- c("hc", "fte", "hc_perc", "fte_perc")
# values <- c("hc")

cols <- c(id_cols, "urn_wtc")

# make into wide format
wtc <- teach_char %>% 
  # TOTALS
  filter_at(vars(!matches("time|urn|laestab|fte|hc")), all_vars(. == "Total")) %>%
  select(all_of(c(cols, "hc", "fte"))) %>%
  right_join( # GENDER
    teach_char %>%
      # filter(gender != "Total", gender != "Gender_Unclassified") %>%
      filter(gender != "Total") %>%
      tidyr::pivot_wider(id_cols = {cols},
                         names_from = gender,
                         names_glue = "{.value}_gender_{gender}",
                         values_from = {values}),
    by = cols) %>%
  right_join( # AGE
    teach_char %>% 
      # filter(age_group != "Total", age_group != "Age_unclassified") %>%
      filter(age_group != "Total") %>%
      tidyr::pivot_wider(id_cols = {cols},
                         names_from = age_group,
                         names_glue = "{.value}_age_{age_group}",
                         values_from = {values}),
    by = cols) %>%
  right_join( # ETHNICITY
    teach_char %>% 
      # filter(ethnicity_major != "Total", ethnicity_major != "Information_not_yet_obtained", ethnicity_major != "Refused") %>%
      filter(ethnicity_major != "Total") %>%
      tidyr::pivot_wider(id_cols = {cols},
                         names_from = ethnicity_major,
                         names_glue = "{.value}_ethnicity_{ethnicity_major}",
                         values_from = {values}),
    by = cols) %>%
  right_join( # GRADE
    teach_char %>% 
      filter(grade != "Total") %>%
      tidyr::pivot_wider(id_cols = {cols},
                         names_from = grade,
                         names_glue = "{.value}_grade_{grade}",
                         values_from = {values}),
    by = cols) %>%
  right_join( # WORKING PATTERN
    teach_char %>% 
      filter(working_pattern != "Total") %>%
      tidyr::pivot_wider(id_cols = {cols},
                         names_from = working_pattern,
                         names_glue = "{.value}_pattern_{working_pattern}",
                         values_from = {values}),
    by = cols) %>%
  right_join( # QTS STATUS
    teach_char %>% 
      filter(qts_status != "Total") %>%
      tidyr::pivot_wider(id_cols = {cols},
                         names_from = qts_status,
                         names_glue = "{.value}_qts_{qts_status}",
                         values_from = {values}),
    by = cols)

# check which variables are complete
non_na_gender <- wtc %>% filter_at(vars(grep("fte_gender", names(wtc))),any_vars(!is.na(.))) # most incomplete
non_na_age <- wtc %>% filter_at(vars(grep("fte_age", names(wtc))),any_vars(!is.na(.))) # similarly complete
non_na_ethn <- wtc %>% filter_at(vars(grep("fte_ethn", names(wtc))),any_vars(!is.na(.))) # similarly complete
non_na_grade <- wtc %>% filter_at(vars(grep("fte_grade", names(wtc))),any_vars(!is.na(.))) # similarly complete
non_na_pattern <- wtc %>% filter_at(vars(grep("fte_pattern", names(wtc))),any_vars(!is.na(.))) # similarly complete
non_na_qts <- wtc %>% filter_at(vars(grep("fte_qts", names(wtc))),any_vars(!is.na(.))) # similarly complete
rm(list=ls(pattern="^non_na_"))
gc()

# MUTATE data
wtc <- wtc %>%
  mutate(
    # fill gaps in total data: grade chosen here but could have also used age, ethnicity, pattern or qts
    tmp_hc = rowSums(across(matches("hc_grade")), na.rm = T),
    hc = ifelse(is.na(hc), tmp_hc, hc),
    tmp_fte = rowSums(across(matches("fte_grade")), na.rm = T),
    fte = ifelse(is.na(fte), tmp_fte, fte)
    ,
    # fill NAs with zeros where possible: GENDER
    # tmp = get HC count based on all gender columns
    # if count is equal to HC (tmp == hc), then replace NA with 0 in all gender columns
    tmp = rowSums(across(matches("hc_gender_")), na.rm = T),
    across(matches("hc_gender_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_gender_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_gender_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_gender_"), ~ifelse(tmp == hc & is.na(.), 0, .))
    ,
    # fill NAs with zeros where possible: AGE
    # tmp = get HC count based on all age columns
    # if count is equal to HC (tmp == hc), then replace NA with 0 in all age columns
    tmp = rowSums(across(matches("hc_age_")), na.rm = T),
    across(matches("hc_age_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_age_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_age_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_age_"), ~ifelse(tmp == hc & is.na(.), 0, .))
    ,
    # fill NAs with zeros where possible: ETHNICITY
    # tmp = get HC count based on all ethnicity columns
    # if count is equal to HC (tmp == hc), then replace NA with 0 in all ethnicity columns
    tmp = rowSums(across(matches("hc_ethnicity_")), na.rm = T),
    across(matches("hc_ethnicity_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_ethnicity_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_ethnicity_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_ethnicity_"), ~ifelse(tmp == hc & is.na(.), 0, .))
    ,
    # fill NAs with zeros where possible: GRADE
    # tmp = get HC count based on all grade columns
    # if count is equal to HC (tmp == hc), then replace NA with 0 in all grade columns
    tmp = rowSums(across(matches("hc_grade_")), na.rm = T),
    across(matches("hc_grade_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_grade_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_grade_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_grade_"), ~ifelse(tmp == hc & is.na(.), 0, .))
    ,
    # fill NAs with zeros where possible: PATTERN
    # tmp = get HC count based on all pattern columns
    # if count is equal to HC (tmp == hc), then replace NA with 0 in all pattern columns
    tmp = rowSums(across(matches("hc_pattern_")), na.rm = T),
    across(matches("hc_pattern_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_pattern_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_pattern_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_pattern_"), ~ifelse(tmp == hc & is.na(.), 0, .))
    ,
    # fill NAs with zeros where possible: QTS
    # tmp = get HC count based on all qts columns
    # if count is equal to HC (tmp == hc), then replace NA with 0 in all qts columns
    tmp = rowSums(across(matches("hc_qts_")), na.rm = T),
    across(matches("hc_qts_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_qts_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("hc_perc_qts_"), ~ifelse(tmp == hc & is.na(.), 0, .)),
    across(matches("fte_perc_qts_"), ~ifelse(tmp == hc & is.na(.), 0, .))
    ,
    # delete tmp
    tmp = NULL)

# CHECK NA COUNT: zero for all but gender
na_count <- as.data.frame(apply(wtc, 2, function(x){sum(is.na(x))}))
na_count$var = row.names(na_count)
rm(na_count)
gc()

# COMPUTE GROUP AGGREGRATES
wtc <- wtc %>%
  mutate(
    # COMPUTE LARGER AGE GROUPS: UNDER 30
    # add up all groups that are combined in the larger group and divide by TOTAL hc or fte
    hc_age_under_30 = rowSums(select(., "hc_age_Under_25", "hc_age_25_to_29"), na.rm = T),
    hc_perc_age_under_30 = hc_age_under_30/hc * 100,
    fte_age_under_30 = rowSums(select(., "fte_age_Under_25", "fte_age_25_to_29"), na.rm = T),
    fte_perc_age_under_30 = fte_age_under_30/fte * 100,
    
    # COMPUTE LARGER AGE GROUPS: 30 - 49
    # add up all groups that are combined in the larger group and divide by TOTAL hc or fte
    hc_age_30_to_49 = rowSums(select(., "hc_age_30_to_39", "hc_age_40_to_49"), na.rm = T),
    hc_perc_age_30_to_49 = hc_age_30_to_49/hc * 100,
    fte_age_30_to_49 = rowSums(select(., "fte_age_30_to_39", "fte_age_40_to_49"), na.rm = T),
    fte_perc_age_30_to_49 = fte_age_30_to_49/fte * 100,
    
    # COMPUTE LARGER AGE GROUPS: OVER 49
    # add up all groups that are combined in the larger group and divide by TOTAL hc or fte
    hc_age_over_49 = rowSums(select(., "hc_age_50_to_59", "hc_age_60_and_over"), na.rm = T),
    hc_perc_age_over_49 = hc_age_over_49/hc * 100,
    fte_age_over_49 = rowSums(select(., "fte_age_50_to_59", "fte_age_60_and_over"), na.rm = T),
    fte_perc_age_over_49 = fte_age_over_49/fte * 100,
    
    # estimate average age of teachers at a school
    # 1. use the hc or fte in each category, multiply it by the category midpoint
    # 2. add up across categories 
    # 3. divide by hc or fte for KNOWN categories
    # 4. this INCLUDES any hc_age_Age_unclassified or fte_age_Age_unclassified
    hc_avg_age      = (hc_age_Under_25 * 22.5 + hc_age_25_to_29 * 27 + hc_age_30_to_39 * 34.5 + 
                         hc_age_40_to_49 * 44.5 + hc_age_50_to_59 * 54.5 + hc_age_60_and_over * 62.5)/hc,
    fte_avg_age     = (fte_age_Under_25 * 22.5 + fte_age_25_to_29 * 27 + fte_age_30_to_39 * 34.5 + 
                         fte_age_40_to_49 * 44.5 + fte_age_50_to_59 * 54.5 + fte_age_60_and_over * 62.5)/fte,
    
    # estimate average age of teachers at a school
    # 1. use the hc or fte in each category, multiply it by the category midpoint
    # 2. add up across categories 
    # 3. divide by hc or fte for KNOWN categories
    # 4. this OMITS any hc_age_Age_unclassified or fte_age_Age_unclassified
    hc_avg_age_known  = (hc_age_Under_25 * 22.5 + hc_age_25_to_29 * 27 + hc_age_30_to_39 * 34.5 + 
                           hc_age_40_to_49 * 44.5 + hc_age_50_to_59 * 54.5 + hc_age_60_and_over * 62.5)
    /(hc_age_Under_25 + hc_age_25_to_29 + hc_age_30_to_39 + hc_age_40_to_49 + hc_age_50_to_59 + hc_age_60_and_over),
    fte_avg_age_known = (fte_age_Under_25 * 22.5 + fte_age_25_to_29 * 27 + fte_age_30_to_39 * 34.5 + 
                           fte_age_40_to_49 * 44.5 + fte_age_50_to_59 * 54.5 + fte_age_60_and_over * 62.5)
    /(fte_age_Under_25 + fte_age_25_to_29 + fte_age_30_to_39 + fte_age_40_to_49 + fte_age_50_to_59 + fte_age_60_and_over)
  ) %>% 
  # SELECT columns to keep
  select(matches("time|urn|laestab|Female|White|British|Classroom|age")) %>%
  select(matches("time_period|urn|laestab|fte_perc|avg_age")) %>%
  as.data.frame()

apply(wtc, 2, function(x) {sum(is.na(x))})


## ISSUE WITH AVERAGE AGE ##
wtc$tmp <- wtc$fte_avg_age - wtc$fte_avg_age_known # determine how much we underestimated the age
wtc$tmp_rd <- round(wtc$tmp, 1) # round
# plot data that has noticeable underestimation
wtc[wtc$tmp_rd != 0,] %>% 
  ggplot(aes(x = tmp)) + 
  geom_histogram(stat = "bin", binwidth = 1, boundary = 1) + 
  stat_bin(geom='text', aes(label=..count..), binwidth = 1, boundary = 1, vjust = -.5)
wtc$tmp <- NULL
wtc$tmp_rd <- NULL
                                   

# GET URN NUMBERS #
if (get_urns) {
  
  # Step 1: Initial Input
  
  # get school identifiers from all dfs
  urn_list <- unique(c(ptr$urn_ptr, pay$urn_pay, abs$urn_abs, vac$urn_vac, swf$urn_swf, wtc$urn_wtc))
  laestab_list <- unique(c(ptr$laestab, pay$laestab, abs$laestab, vac$laestab, swf$laestab, wtc$laestab))
  
  # read in data
  est <- read.csv(file = file.path(dir_data, "data_establishments_search.csv"), na.strings = "")
  
  # select relevant laestab only and relevant columns
  est <- est[est$laestab %in% laestab_list | est$urn %in% urn_list, c("laestab", "urn", "opendate", "closedate")]

  # define academic years
  academic_years <- lookup$academic_year
  
  # Step 2a: Identify Schools with Single Entries
  
  laestab_s <- est %>%
    group_by(laestab) %>%
    summarise(n = n()) %>%
    filter(n == 1) %>%
    pull(laestab)
  
  # Step 2b: Identify Schools with Multiple Entries
  
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
  data.table::fwrite(urn, file = file.path(dir_misc, "data_swf_urn.csv"), row.names = F)
  
} else {
  
  # read in urn data
  urn <- read.csv(file = file.path(dir_misc, "data_swf_urn.csv"))
}

# COMBINE ALL DFs #

# get school identifiers from all dfs
urn_list <- unique(urn$urn)
laestab_list <- unique(urn$laestab)

# create scaffold to safe data
scaffold <- merge(data.frame(time_period = as.integer(years_list)),
                  # data.frame(urn = urn_list))
                  data.frame(laestab = laestab_list))

# process data
df <- scaffold %>%
  # merge with urn info
  full_join(., urn, by = id_cols) %>%
  # merge all dfs
  left_join(., ptr, by = id_cols) %>%
  left_join(., pay, by = id_cols) %>% 
  left_join(., abs, by = id_cols) %>%
  left_join(., vac, by = id_cols) %>%
  left_join(., swf, by = id_cols) %>%
  left_join(., wtc, by = id_cols) %>%
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
  # group_by(urn) %>%
  group_by(laestab) %>%
  mutate(
    # fill missing values: observations to be carried backward
    # across(c(region_code, region, old_la_code, new_la_code, la, laestab, school, school_type),
    across(c(region, la, school, school_type),
           ~zoo::na.locf(., na.rm = FALSE, fromLast = TRUE)),
    # fill missing values: observations to be carried forward
    # across(c(region_code, region, old_la_code, new_la_code, la, laestab, school, school_type),
    across(c(region, la, school, school_type),
           ~zoo::na.locf(., na.rm = FALSE, fromLast = FALSE)))  %>%
  ungroup() %>%
  # re-compute ratios
  mutate(pupil_to_qual_teacher_ratio = pupils_fte / qualified_teachers_fte ,
         pupil_to_qual_unqual_teacher_ratio = pupils_fte / teachers_fte,
         pupil_to_adult_ratio = pupils_fte / adults_fte) %>%
  # sort data
  # arrange(urn, time_period) %>% as.data.frame()
  arrange(laestab, time_period) %>% as.data.frame()

# save data
#df <- df[with(df, order(urn, time_period)),]
data.table::fwrite(df, file = file.path(dir_data, "data_swf.csv"), row.names = F)
