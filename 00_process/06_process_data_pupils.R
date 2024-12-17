### combine all pupil characteristics ####

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

# determine year list (akin to other data sources)
years_list <- paste0(20, 10:23, 11:24)

id_cols <- c("time_period", "urn")


#### data on school capacity ####

# data reported by local authorities in England, in the annual School Capacity (SCAP) survey, as of 1 May 2023

# 1. Includes mainstream state schools with capacity in any of the year groups from reception to year 11 on 1 May for the relevant academic year.
# 2. Primary places include all reported capacity in primary and middle deemed primary schools. Capacity excludes nursery places.
# 3. Secondary places include all reported capacity in secondary, middle-deemed secondary and all-through schools. Capacity includes sixth form places.
# 4. Number of pupils on roll for reception year group and above. Taken from the May school census for the relevant academic year, or gathered during the school capacity collection if census data was not available for a school.
# 5. 2019/20 data not available due to the cancellation of the 2020 School Capacity survey due to COVID-19.
# 6. Number of pupils in places that exceed their school's capacity is the difference between school places and number of pupils on roll, for schools where the number of pupils on roll is higher than the school’s reported capacity. Calculated at school level and then summed to national, regional or local authority level.

cap <- read.csv(file.path(dir_data, "school-capacity", "2022-23", "data", "school-capacity_200910-202223.csv"))
names(cap) <- tolower(gsub("X...", "", names(cap), fixed = T))

# change names
names(cap)[names(cap) == "school_urn"] <- "urn"

# subset data to only include relevant schools
cap <- cap %>% filter(time_period != 200910)

# remove columns
cap <- cap[, grepl("time_period|urn|pupils_on_roll", names(cap))]

# Figures are suppressed (“supp”) where they concern fewer than 10 pupils.
print(apply(cap, 2, function(x) { unique(regmatches(x, gregexpr("[A-Za-z]+", x)))   }))
cap <- apply(cap, 2, function(x) {ifelse(x == "z" | x == "x", NA, as.numeric(x))}) %>%
  as.data.frame()

# rename
names(cap)[c(-1, -2)] <- c("npuptot__cap")
# "npuptot__cap" = Number of pupils on roll

# check for duplicate entries
tmp <- cap %>% group_by(time_period, urn) %>% summarise(obs = n())

# Group the data by the 'urn' column
cap <- cap %>%
  group_by(time_period, urn) %>%
  # Use mutate to update columns within each group
  mutate(
    # Set npuptot__cap to NA if the group has more than one row
    npuptot__cap = ifelse(n() > 1, NA, npuptot__cap)
  ) %>%
  # Remove the grouping structure after the operation
  ungroup() %>%
  # Remove rows with any NA values
  filter(complete.cases(.))
  

#### read in previously created files ####

spc <- data.table::fread(file.path(dir_data, "data_spc.csv")) # census data collected in January of academic year - Spring census
sen <- data.table::fread(file.path(dir_data, "data_sen.csv"))
spt <- data.table::fread(file.path(dir_data, "data_spt_census.csv")) 
ks4 <- data.table::fread(file.path(dir_data, "data_spt_ks4.csv")) 

# add ks2 average percentile and z score data to spt

spt <- merge(spt, ks4[, .(time_period, urn, ks2a_perc, ks2a_zscore)], by = c("time_period", "urn"), all.x = T)
rm(ks4)

# fix postcode
spc[, school_postcode := gsub("%20", " ", school_postcode)]

# combine data.tables
df <- merge(spc, sen, by = id_cols, all.x = T)
df <- merge(df, spt, by = id_cols, all.x = T)
df <- merge(df, cap, by = id_cols, all.x = T)

df <- as.data.frame(df)

rm(cap, spc, sen, spt)

#### process combined data ####

# get total number of pupils #

# possible variables to use
tmp <- df[, c(id_cols, "npuptot__spc", "npuptot__sen", "npuptot__cap", "npuptot__sptcensus")]

# rounding applied to nearest 5 in total pupil headcount data collected in 201011 / 201112 / 201213 in SPC
# there is a difference between npuptot__spc & npuptot__sen only in the years 2013/14 - 2016/17
#   data in  npuptot__sen is rounded to nearest 5, data in npuptot__spc is not rounded
#   NOTE: data on total number of pupils is the same for SPC and SEN tables once SEN data is rounded to the nearest 5 for the years 2013/14 - 2016/17 (!)
# there is a difference between npuptot__spc & npuptot__sptcensus only in the years 2010/11 - 2012/13
#   data in  npuptot__spc is rounded to nearest 5, data in npuptot__sptcensus is not rounded
#   NOTE: data on total number of pupils is the same for SPC and SPT tables once SPT data is rounded to the nearest 5 for the years 2010/11 - 2012/13 (!)

col_tot <- "npuptot"

tmp <- fix_roundings(var_rd = "npuptot__spc", var_nrd = "npuptot__sptcensus",
                     new_var = col_tot,
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201011, 201112, 201213),
                     rounding_factor = 5,
                     data_in = df)

# select rows
df <- merge(df, tmp[, c(id_cols, col_tot)], by = id_cols, all.x = T)

# select columns
tmp <- df[, c(id_cols, 
              
              "estab", "laestab", 
              "school", "school_postcode", "region", "region_code", "urban_rural",
              "old_la_code", "la",
              "phase_of_education", "school_type", "phase_type_grouping", "type_of_establishment",         
              "sex_of_school_description", "denomination", "admissions_policy", "idaci_decile",
              
              col_tot, "npuptot__sen", "npuptot__cap", "npuptot__sptcensus")]

# check NAs
apply(tmp, 2, FUN = function(x){sum(is.na(x))})

# for those that have NAs in npuptot, fill with information included in capacity data
tmp[, col_tot] <- ifelse(is.na(tmp[, col_tot]), tmp$npuptot__cap, tmp[, col_tot])
apply(tmp, 2, FUN = function(x){sum(is.na(x))})

out <- tmp[, c(id_cols,               
               "estab", "laestab", 
               "school", "school_postcode", "region", "region_code", "urban_rural",
               "old_la_code", "la",
               "phase_of_education", "school_type", "phase_type_grouping", "type_of_establishment",           
               "sex_of_school_description", "denomination", "admissions_policy", "idaci_decile",
               col_tot)]
out <- out[!duplicated(out),]
gc()

# with the total number of pupils on role, calculate percentage of pupils based on SPC data #

# number of pupils female
new_col <- "npupf"
col_n <- "npupf"
col_p <- "pnpupf"

out <- merge(out, df[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# % of pupils known to be eligible for free school meals	
# Number of pupils know to be eligible for FSM expressed as a percentage of the total number of pupils

col_n <- "npupfsm_e"
col_p <- "pnpupfsm_e"

out <- merge(out, df[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# percentage of pupils taking a free school meal on census day	

col_n <- "npupfsm_t"
col_p <- "pnpupfsm_t"

out <- merge(out, df[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# FSM calculation in Performance Tables
# npup_calcspt - Number of pupils (used for FSM calculation in Performance Tables)
# npupfsm_e_spt - number of pupils known to be eligible for free school meals (School Performance Tables)
# pnpupfsm_e_spt - percentage of pupils known to be eligible for free school meals (School Performance Tables)
out <- merge(out, df[, c(id_cols, "npup_calcspt", "npupfsm_e_spt", "pnpupfsm_e_spt")], by = id_cols)
out <- out[!duplicated(out),]

# % of pupils whose first language is known or believed to be other than English
col_n <- "npupeal"
col_p <- "pnpupeal"

out <- merge(out, df[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# % of pupils classified as white British ethnic origin

col_n <- "npupeowb"
col_p <- "pnpupeowb"

out <- merge(out, df[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# % of pupils classified as Black ethnic origin

col_n <- "npupeobl"
col_p <- "pnpupeobl"

out <- merge(out, df[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# % of pupils classified as Asian ethnic origin

col_n <- "npupeoas"
col_p <- "pnpupeoas"

out <- merge(out, df[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# class size data
# nclt1t - total number of classes taught by one teacher
# npupclt1t - total number of pupils in classes taught by one teacher

col_n <- "npupclt1t"
col_tot <- "nclt1t"

out <- merge(out, df[, c(id_cols, col_tot, col_n)], by = id_cols, all = T)

# avgclsize - average size of one teacher classes = Number of pupils divided by number of classes
col_p <- "avgclsize"
out[, col_p] <- out[, col_n] / out[, col_tot]
out <- out[!duplicated(out),]

# special educational needs data #

col_tot <- "npuptot"

# School Action and School Action Plus (SEN Code of Practice 2001)

col_n <- "npupsena" # pupils on roll with SEN on School Action
col_p <- "pnpupsena"

# fix roundings
# data for 2012/13 - 2013/14 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = col_n, var_nrd = "tsena",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201213, 201314),
                     rounding_factor = 5,
                     data_in = df)

# select rows
out <- merge(out, tmp[, c(id_cols, col_n)], by = id_cols, all = T)
# compute perc
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# check <- c("tsena", "psena")
# out <- merge(out, df[, c(id_cols, check)], by = id_cols)

col_n <- "npupsenap" # pupils on roll with SEN on School Action Plus
col_p <- "pnpupsenap"

# fix roundings
# data for 2012/13 - 2013/14 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = col_n, var_nrd = "totsenap",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201213, 201314),
                     rounding_factor = 5,
                     data_in = df)

# select rows
out <- merge(out, tmp[, c(id_cols, col_n)], by = id_cols, all = T)
# compute perc
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# SEN Statement (SEN Code of Practice 2001)

col_n <- "npupsenst" # pupils on roll with SEN statement
col_p <- "pnpupsenst"

# fix roundings
# data for 2012/13 - 2013/14 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = col_n, var_nrd = "totsenst",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201213, 201314),
                     rounding_factor = 5,
                     data_in = df)

# select rows
out <- merge(out, tmp[, c(id_cols, col_n)], by = id_cols, all = T)
# compute perc
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# eligible pupils with SEN support (SEN Code of Practice 2014)

col_n <- "npupsenelk2014" # pupils on roll with SEN support 
col_p <- "pnpupsenelk2014"

# fix roundings
# data for 2014/15 - 2016/17 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = col_n, var_nrd = "tsenelk",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201415, 201516, 201617),
                     rounding_factor = 5,
                     data_in = df)

# select rows
out <- merge(out, tmp[, c(id_cols, col_n)], by = id_cols, all = T)
# compute perc
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# check <- c("tsenelk", "psenelk")
# out <- merge(out, df[, c(id_cols, check)], by = id_cols)

# SEN pupils with a statement or EHC plan (SEN Code of Practice 2014)
col_n <- "npupsenehcst" # pupils on roll with SEN statement or EHC plan
col_p <- "nnpupsenehcst"

# fix roundings
# data for 2014/15 - 2016/17 reported in SEN is rounded in comparison to numbers reported in SPT
tmp <- fix_roundings(var_rd = col_n, var_nrd = "tsenelse",
                     identifier_columns = id_cols,
                     col_to_filter = "time_period",
                     filter = c(201415, 201516, 201617),
                     rounding_factor = 5,
                     data_in = df)

# select rows
out <- merge(out, tmp[, c(id_cols, col_n)], by = id_cols, all = T)
# compute perc
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# check <- c("tsenelse", "psenelse")
# out <- merge(out, df[, c(id_cols, check)], by = id_cols)

# pupils with SEN support (all years)

col_n <- "npupsenelk" # eligible pupils on roll with SEN support
col_p <- "pnpupsenelk"

# combine School Action and School Action Plus (SEN Code of Practice 2001)
out[, "npupsenelk2001"] <- out$npupsena + out$npupsenap # both were fixed above

cols_to_merge <- c("npupsenelk2001", "npupsenelk2014") # also fixed above

tmp <- out[, c(id_cols, cols_to_merge)]
tmp <- tmp[!duplicated(tmp),]
tmp <- merge_timelines_across_columns(data_in = tmp, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = col_n,
                                     data_out = tmp)

out <- merge(out, tmp[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# pupils with EHC plan/Statement of SEN (all years)

col_n <- "npupsenelse" # pupils on roll with SEN statement or EHC plan
col_p <- "pnpupsenelse"

# combine School Action and School Action Plus (SEN Code of Practice 2001)

cols_to_merge <- c("npupsenst", "npupsenehcst") # fixed above

tmp <- out[, c(id_cols, cols_to_merge)]
tmp <- tmp[!duplicated(tmp),]
tmp <- merge_timelines_across_columns(data_in = tmp, 
                                      identifier_columns = id_cols, 
                                      column_vector = cols_to_merge,
                                      stem = col_n,
                                      data_out = tmp)

out <- merge(out, tmp[, c(id_cols, col_n)], by = id_cols, all = T)
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
out <- out[!duplicated(out),]

# pupils with SEN provision - EHC plan/Statement of SEN or SEN support/School Action/School Action plus

col_n <- "npupsen" # pupils on roll with SEN statement or EHC plan
col_p <- "pnpupsen"

out[, col_n] <- out$npupsenelse + out$npupsenelk # fixed above
out[, col_p] <- out[, col_n] / out[, col_tot] * 100
apply(out, 2, FUN = function(x){sum(is.na(x))})

# percentile ks2 average performance

out <- merge(out, df[, c(id_cols, "ks2a_perc", "ks2a_zscore")], by = id_cols, all = T)

# write file #

# copy df
data <- out[]

# deleta all columns not necessary
data[, grepl("npupfsm_t", names(data))] <- NULL
data[, grepl("spt", names(data))] <- NULL
data[, grepl("sena", names(data))] <- NULL
data[, grepl("senst", names(data))] <- NULL
data[, grepl("ehc", names(data))] <- NULL
data[, grepl("20", names(data))] <- NULL

apply(data, 2, FUN = function(x){sum(is.na(x))})

# save file
data <- data[with(data, order(urn, time_period)),]
data.table::fwrite(data, file = file.path(dir_data, "data_pupils.csv"), row.names = F)

# # code to debug!
# col_n <- "npupfsm_e"
# col_p <- "pnpupfsm_e"
# out <- merge(out, df[, c(id_cols, col_n, col_p)], by = id_cols)
# col_p <- "test"
# out[, col_p] <- out[, col_n] / out[, col_tot] * 100
# 
# psych::describe(round(out$test, 1) - out[, col_p])