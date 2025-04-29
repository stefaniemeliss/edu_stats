# school performance tables - census data #

# Data source: the DfE’s January school census for 2024.

options(scipen = 999)
# empty work space
rm(list = ls())
gc()

# load libraries
library(dplyr)

devtools::source_url("https://github.com/stefaniemeliss/edu_stats/blob/main/functions.R?raw=TRUE")

# define directories
dir <- getwd()
dir_data <- file.path(dir, "data")
dir_misc <- file.path(dir, "misc")
dir_in <- file.path(dir_data, "performance-tables")

# determine year list (akin to other data sources)
years_list <- paste0(20, 10:23, 11:24)

id_cols <- c("time_period", "urn")

##### performance tables #####

# determine years of interest
start <- 2010
finish <- 2023

for (year in start:finish) {
  
  # skip covid years
  if(year == 2019) next
  
  # determine academic year
  academic_year <- paste0(year,"-", year+1)
  
  # determine folder for academic year
  dir_year <- file.path(dir_in, academic_year)
  
  # read in census data
  tmp <- read.csv(file = file.path(dir_year, paste0(academic_year, "_england_census.csv")))
  names(tmp) <- gsub("X...", "", names(tmp), fixed = T)
  
  # subset data to only include relevant schools
  names(tmp) <- tolower(names(tmp))
  
  # check for any strings
  cat(academic_year, "\n")
  print(apply(tmp, 2, function(x) { unique(regmatches(x, gregexpr("[A-Za-z]+", x)))   }))
  
  # exclude national-level data
  tmp <- tmp %>% filter(! toupper(urn) %in% c("NAT"))
  
  # replace spaces and %
  tmp <- apply(tmp, 2, function(x) {ifelse(grepl(" |%", x), gsub(" |%", "", x), x)}) %>% 
    as.data.frame()
  
  # Figures are suppressed (“supp”) where they concern fewer than 10 pupils.
  tmp <- apply(tmp, 2, function(x) {ifelse(x == "SUPP" | x == "NP" | x == "" | x == " ", NA, as.numeric(x))}) %>%
    as.data.frame()
  
  # Exclude rows that fully consist of NA values
  tmp <- tmp[apply(tmp, 1, function(row) !all(is.na(row))), ]
  
  # add year
  tmp$time_period <- as.numeric(gsub("-20", "", academic_year))
  
  # get meta data
  tmp_meta <- read.csv(file = file.path(dir_year, paste0(academic_year, "_census_meta.csv")))
  names(tmp_meta) <- gsub("X...", "", tolower(names(tmp_meta)), fixed = T)
  names(tmp_meta)[names(tmp_meta) == "field.reference"] <- "variable"
  names(tmp_meta)[names(tmp_meta) == "field.name"] <- "label"
  tmp_meta <- tmp_meta[, c("variable", "label")]
  tmp_meta$time_period <- as.numeric(gsub("-20", "", academic_year))
  
  # combine across years
  if (year == start) {
    census <- tmp
    meta <- tmp_meta
  } else {
    census <- rbind.all.columns(census, tmp)
    meta <- rbind.all.columns(meta, tmp_meta)
  }
  
}

# ------ process meta data ------ #
meta <- meta[with(meta, order(variable, time_period)), ]
# save meta data #
write.csv(meta, file = file.path(dir_misc, "meta_spt_census.csv"), row.names = F)

# ------ process census data ------ #

### add data from FOI 2025-0008906 request ###

# read in file
file = file.path(dir_misc, "FOI_2025-0008906_SMeliss_FSM6_Final.xlsx")
foi <- xlsx::read.xlsx(file = file, sheetIndex = 1, startRow = 3)

# process data
foi$numfsmever <- foi$FSM6
foi$time_period <- 201920

# select columns
foi <- foi[, c(id_cols, "numfsmever")]

# add to census data
census <- rbind.all.columns(census, foi)

# remove duplicate entries from census
census <- census[!duplicated(census), ]

### Clean information on school identifiers ###

# read in establishment data
est <- read.csv(file = file.path(dir_data, "data_establishments_search.csv"), na.strings = "")
est <- as.data.frame(fread(file.path(dir_data, "data_establishments_download.csv"), encoding = "UTF-8"))
est <- est[!is.na(est$laestab), ]

# select relevant laestab and urn only and relevant columns
est <- est[!duplicated(est), c("laestab", "urn", "establishmentname")]
names(est) <- c("laestab", "urn_est", "school")

# extract all id pairings
ids <- census %>% 
  # replace NA in LAESTAB where possible
  mutate(
    laestab = ifelse(is.na(laestab) & !is.na(la) & !is.na(estab),
                     as.numeric(paste0(la, estab)),
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
  rename(urn_spt = urn,
         laestab_spt = laestab) %>%
  as.data.frame()

# create id lookup table for each urn #
id_lookup <- ids %>%
  # FIX URNs #
  left_join(., # add correct urn numbers for urns without a match
            # mapping between urn and laestab for all incorrect urns
            est[est$laestab %in% ids$laestab_spt[ids$urn_match == F], c("laestab", "urn_est")],
            join_by(laestab_spt == laestab)
  ) %>% 
  mutate(
    # combine both urn variables into one with the correct URN numbers
    urn = ifelse(urn_match, urn_spt, urn_est)
  ) %>%
  # FIX LAESTABS #
  left_join(., # get the correct laestab for each urn
            est, join_by(urn == urn_est)) %>%
  # select columns
  select(urn, urn_spt, laestab, school) %>%
  # remove duplicates
  filter(!duplicated(.)) %>%
  as.data.frame()


# fix id information in census
census <- census %>% 
  # replace NA in LAESTAB where possible
  mutate(
    laestab = ifelse(is.na(laestab) & !is.na(la) & !is.na(estab),
                     as.numeric(paste0(la, estab)),
                     laestab)
  ) %>%
  # remove columns that are uninformative
  select(where(~length(unique(na.omit(.x))) > 1)) %>% 
  # rename original id columns
  rename(urn_spt = urn, laestab_spt = laestab) %>%
  # add correct ids
  full_join(id_lookup, .) %>%
  # sort data
  arrange(laestab, time_period) %>%
  as.data.frame()

### Merge timelines across columns ###

# select subset of columns
df <- census %>% 
  # select columns
  select(time_period,
         urn,
         laestab, 
         school) %>%
  as.data.frame()
apply(df, 2, function(x){sum(is.na(x))})  

# Total number of pupils on roll (all ages)
# TOTPUPSENDN - Total number of pupils on roll (all ages) - 2011-11 to 2013/14
# NOR - Total number of pupils on roll from 2014/15 onwards
cols_to_merge <- c("totpupsendn", "nor")
new_col <- "npuptot__sptcensus"

df <- merge_timelines_across_columns(data_in = census, 
                                     identifier_columns = id_cols, 
                                     column_vector = cols_to_merge,
                                     stem = new_col,
                                     data_out = df)

# select other columns
df <- merge(df, census[, c(id_cols,
                            "numfsm", # NUMFSM - Number of pupils eligible for free school meals
                            "numfsmever", # NUMFSMEVER - Number of pupils eligible for FSM at any time during the past 6 years
                            "totpupfsmdn", # TOTPUPFSMDN - Number of pupils used in FSM calculation
                            "numeal", # NUMEAL - Number of pupils with English not as first language
                            "totpupealdn", # TOTPUPEALDN - Number of pupils of compulsory school age and above
                            "tsena", # TSENA - Number of pupils on roll with SEN on School Action
                            "totsenap", # TOTSENAP - Total pupils with school  action+
                            "totsenst", # TOTSENST - Total pupils with SEN statement
                            #"tsensap",  # TSENSAP - Number of pupils SEN statement or on School Action Plus  - IGNORE?
                            "tsenelk", # TSENELK - Number of eligible pupils with SEN support
                            "tsenelse" # TSENELSE - Number of SEN pupils with a statement or EHC plan
                            )], 
             by = c(id_cols), all = T)


# remove duplicates
df <- df[!duplicated(df), ]

# save data
df <- df[with(df, order(laestab, time_period)),]
data.table::fwrite(df, file = file.path(dir_data, "data_spt_census.csv"), row.names = F)
