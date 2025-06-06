# Special educational needs in England #

# This publication contains information about pupils with special educational needs. 
# This information is derived from school census returns, general hospital school census 
# and school level annual school census (SALSC, independent schools) returns made to the department in January each year. 
# The school census contains pupil level data covering a wide range of information on the characteristics of schools and the pupils. 

options(scipen = 999)
# empty work space
rm(list = ls())
gc()

# load libraries
library(dplyr)
library(data.table)

devtools::source_url("https://github.com/stefaniemeliss/edu_stats/blob/main/functions.R?raw=TRUE")

# define directories
dir <- getwd()
dir_data <- file.path(dir, "data")
dir_misc <- file.path(dir, "misc")
dir_in <- file.path(dir_data, "special-educational-needs-in-england")

# determine year list (akin to other data sources)
years_list <- paste0(20, 10:23, 11:24)

id_cols <- c("time_period", "urn")

# rename folders that currently only have one year included (data collected in Jan)
rename_folders <- F
if (rename_folders) {
  # save df with old and new folder names
  start <- 2010
  finish <- 2019
  tmp <- data.frame(old = c(start:finish))
  tmp$new <- paste0(paste0(tmp$old-1,"-", gsub(20, "",tmp$old)))
  # add dirs
  tmp$from <- file.path(dir_in, tmp$old)
  tmp$to <- file.path(dir_in, tmp$new)
  # rename
  file.rename(from = c(tmp$from), to = c(tmp$to))
}

# determine years of interest
start <- 2010 # no school level data for 2009-10
finish <- 2023

files <- list.files(path = dir_in,
                    pattern = "UD|ud|nderlying",
                    recursive = T,
                    full.names = T)
files <- files[!grepl("meta|Meta", files)]
files

#### combine data ####

# loop over years #
for (i in seq_along(start:finish)) {
  
  year = c(start:finish)[i]
  
  # determine academic year
  academic_year <- paste0(year,"-", year+1)
  
  # read in data
  tmp <- read.csv(file = files[i], fileEncoding="latin1")
  
  # fix column names
  names(tmp) <- gsub("X...", "", names(tmp), fixed = T)
  names(tmp) <- gsub("ï..", "", names(tmp), fixed = T)

  # Replace consecutive full stops with a single underscore
  names(tmp) <- gsub("\\.+", "_", names(tmp))
  
  # subset data to only include relevant schools
  names(tmp) <- tolower(names(tmp))

  # filter to remove columns
  tmp <- tmp[, !grepl("primary|prov", names(tmp))]
  
  # check for any strings
  cat(academic_year, "\n")
  print(apply(tmp, 2, function(x) { unique(regmatches(x, gregexpr("[A-Za-z]+", x)))   }))
  
  # Figures are suppressed (“supp”) where they concern fewer than 10 pupils.
  tmp <- apply(tmp, 2, function(x) {ifelse(x == "x" | x == "z" | x == "." | x == "..", NA, as.numeric(x))}) %>%
    as.data.frame()
  
  # remove columns where all rows contain NA values
  tmp <- tmp[,colSums(is.na(tmp))<nrow(tmp)]
  
  # add year
  tmp$time_period <- as.numeric(gsub("-20", "", academic_year))
  
  # combine across years
  if (year == start) {
    sen <- tmp
  } else {
    sen <- rbind.all.columns(sen, tmp)
  }
  
}


### Clean information on school identifiers ###

# read in establishment data
est <- read.csv(file = file.path(dir_data, "data_establishments_search.csv"), na.strings = "")
est <- as.data.frame(fread(file.path(dir_data, "data_establishments_download.csv"), encoding = "UTF-8"))
est <- est[!is.na(est$laestab), ]

# select relevant laestab and urn only and relevant columns
est <- est[!duplicated(est), c("laestab", "urn", "establishmentname")]
names(est) <- c("laestab", "urn_est", "school")

# merge two columns for old LA code
cols_to_merge <- c("la_code_old", "old_la_code")
new_col <- "old_la_code_merged"

sen <- merge_timelines_across_columns(data_in = sen, 
                                      identifier_columns = id_cols, 
                                      column_vector = cols_to_merge,
                                      stem = new_col,
                                      data_out = sen)


# extract all id pairings
ids <- sen %>% 
  # replace NA in LAESTAB where possible
  mutate(
    laestab = ifelse(is.na(laestab) & !is.na(old_la_code_merged) & !is.na(estab),
                     as.numeric(paste0(old_la_code_merged, estab)),
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
  rename(urn_sen = urn,
         laestab_sen = laestab) %>%
  as.data.frame()

# create id lookup table for each urn #
id_lookup <- ids %>%
  # FIX URNs #
  left_join(., # add correct urn numbers for urns without a match
            # mapping between urn and laestab for all incorrect urns
            est[est$laestab %in% ids$laestab_sen[ids$urn_match == F], c("laestab", "urn_est")],
            join_by(laestab_sen == laestab)
  ) %>% 
  mutate(
    # combine both urn variables into one with the correct URN numbers
    urn = ifelse(urn_match, urn_sen, urn_est)
  ) %>%
  # FIX LAESTABS #
  left_join(., # get the correct laestab for each urn
            est, join_by(urn == urn_est)) %>%
  # select columns
  select(urn, urn_sen, laestab, school) %>%
  # remove duplicates
  filter(!duplicated(.)) %>%
  as.data.frame()


# fix id information in census
sen <- sen %>% 
  # replace NA in LAESTAB where possible
  mutate(
    laestab = ifelse(is.na(laestab) & !is.na(old_la_code_merged) & !is.na(estab),
                     as.numeric(paste0(old_la_code_merged, estab)),
                     laestab)
  ) %>%
  # remove columns that are uninformative
  select(where(~length(unique(na.omit(.x))) > 1)) %>% 
  # rename original id columns
  rename(urn_sen = urn, laestab_sen = laestab) %>%
  # add correct ids
  full_join(id_lookup, .) %>%
  # sort data
  arrange(laestab, time_period) %>%
  as.data.frame()


### extract relevant data ###

# select subset of columns
df <- sen %>% 
  # select columns
  select(time_period,
         urn,
         laestab, 
         school) %>%
  as.data.frame()
apply(df, 2, function(x){sum(is.na(x))})  

# load spt data
spt <- data.table::fread(file.path(dir_data, "data_spt_census.csv")) 
# select columns
spt <- spt[,  .SD, .SDcols = grepl("time_period|urn|laestab|sen", names(spt))]

# merge with SEND
sen <- merge(sen, spt, by = id_cols, all.x = T)

# number of pupils
cols_to_merge <- c("pupils", "total_pupils")
new_col <- "npuptot__sen"

df <- merge_timelines_across_columns(data_in = sen, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# School Action (SEN Code of Practice 2001)
# This stage involved the school providing additional or different support to help the child progress. 
# SEN Code of Practice 2014 replaced the terms "School Action" and "School Action Plus" with "SEN support."
cols_to_merge <- c("schoolaction", "school_action")
new_col <- "npup_school_action" # pupils on roll with SEN on School Action

tmp <- merge_timelines_across_columns(data_in = sen, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = sen)

# fix roundings
# data for 2012/13 - 2013/14 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = new_col, var_nrd = "tsena",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201213, 201314),
                     rounding_factor = 5,
                     data_in = tmp)

# select rows
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)

# School Action Plus (SEN Code of Practice 2001)
# This stage involved external specialists providing additional advice and support to the school to help meet the child's needs.
# SEN Code of Practice 2014 replaced the terms "School Action" and "School Action Plus" with "SEN support."
cols_to_merge <- c("schoolactionplus", "school_action_plus")
new_col <- "npup_school_action_plus" # pupils on roll with school  action plus

tmp <- merge_timelines_across_columns(data_in = sen, 
                                            identifier_columns = id_cols, 
                                            column_vector = cols_to_merge,
                                            stem = new_col,
                                            data_out = sen)

# fix roundings
# data for 2012/13 - 2013/14 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = new_col, var_nrd = "totsenap",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201213, 201314),
                     rounding_factor = 5,
                     data_in = tmp)
# combine
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)


# SEN support (SEN Code of Practice 2014)
# "SEN support" is the current system used in schools to help children with special educational needs 
# who do not have an Education, Health and Care (EHC) plan. 
new_col <- "npup_sen_support"
sen[, new_col] <- sen[, "sen_support"]

# fix roundings
# data for 2014/15 - 2016/17 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = new_col, var_nrd = "tsenelk",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201415, 201516, 201617),
                     rounding_factor = 5,
                     data_in = sen)

# combine 
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)

# statements of SEN (SEN Code of Practice 2001)
# A Statement of Special Educational Needs was the previous system used in England to outline the educational needs 
# and the provision required for children with significant special educational needs.
cols_to_merge <- c("statements", "statement")
new_col <- "npup_statement" # pupils on roll with SEN statement

tmp <- merge_timelines_across_columns(data_in = sen, 
                                      identifier_columns = id_cols, 
                                      column_vector = cols_to_merge,
                                      stem = new_col,
                                      data_out = sen)

# fix roundings
# data for 2012/13 - 2013/14 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = new_col, var_nrd = "totsenst",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201213, 201314),
                     rounding_factor = 5,
                     data_in = tmp)
# combine
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)

# EHC plan (SEN Code of Practice 2014)
# An Education, Health and Care plan is a legal document that describes a child or young person's special educational, health, and social care needs. 
# It also specifies the support they need and the outcomes they are working towards.
# EHC plans were introduced under the Children and Families Act 2014, replacing the Statements of SEN.
# statements or EHC plan (transfer of statements to an EHC plan is due to take place by April 2018)

cols_to_merge <- c("statement_ehc_plan", "ehc_plan")
new_col <- "npup_ehcp" # pupils on roll with EHC plan or statement

tmp <- merge_timelines_across_columns(data_in = sen, 
                                      identifier_columns = id_cols, 
                                      column_vector = cols_to_merge,
                                      stem = new_col,
                                      data_out = sen)

# fix roundings
# data for 2014/15 - 2016/17 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = new_col, var_nrd = "tsenelse",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201415, 201516, 201617),
                     rounding_factor = 5,
                     data_in = tmp)

# combine 
df <- merge(df, tmp[, c(id_cols, new_col)], by = id_cols, all = T)

# "SEND plan" (Special Educational Needs and Disabilities plan) = statements or EHC plan combined
# in SPT: TSENEL*SE* = Number of SEN pupils with a statement or EHC plan
# S = Statement
# E = Education, Health and Care (EHC) plan
# This metric indicates the total count of pupils within a school who have significant special educational needs 
# that require a formal plan or statement to ensure they receive the appropriate support and resources.
cols_to_merge <- c("npup_statement", "npup_ehcp")
new_col <- "npupsenelse" # pupils on roll with SEN statement or EHC plan

df <- merge_timelines_across_columns(data_in = df, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# pupil SEN status: Plan or intervention
# SEN provision - EHC plan/Statement of SEN or SEN support/School Action/School Action plus
# df[, "npupsen"] <- df$npupsenelse + df$npupsenelk
df[, "npupsen"] <- rowSums(df[, c("npup_school_action", "npup_school_action_plus",
                                  "npup_sen_support",
                                  "npup_statement", "npup_ehcp")], na.rm = T)

# replace zeros with NA for rows without data
df$tmp <- rowSums(is.na(df))

df[, "npupsen"] <- ifelse(df$tmp == 9, NA, df[, "npupsen"])
df[, "npupsen"] <- ifelse(df$tmp == 7 & !is.na(df$npuptot__sen), NA, df[, "npupsen"])

# remove all rows that are empty due to statistical suppression
df <- df[df$tmp != 9, ]

df$tmp <- NULL

#### save data ####
df <- df[with(df, order(laestab, time_period)),]
data.table::fwrite(df, file = file.path(dir_data, "data_sen.csv"), row.names = F)

#### create var dict ####
dict <- data.frame(variable = names(df)[!grepl("_tag", names(df))])
dict$explanation <- c("academic year",
                      "unique reference number",
                      "LAESTAB",
                      "school",
                      "total number of pupils",
                      "number of pupils with SEN on School Action",
                      "number of pupils with SEN on School Action Plus",
                      "number of pupils with SEN support (SEN Code of Practice 2014)",
                      "number of pupils with SEN statement (SEN Code of Practice 2001)",
                      "number of pupils with EHC plan or Statement of SEN (SEN Code of Practice 2014)",
                      "number of pupils with EHC plan or Statement of SEN (SEN Code of Practice 2014)",
                      "number of pupils with SEN provision (EHC plan/Statement of SEN or SEN support/School Action/School Action plus)"
                      )
# save file
write.csv(dict, file = file.path(dir_misc, "meta_sen.csv"), row.names = F)
