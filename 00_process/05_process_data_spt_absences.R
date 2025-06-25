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
in_dir <- file.path(dir_data, "performance-tables")

# determine year list (akin to other data sources)
years_list <- paste0(20, 10:22, 11:23)

##### performance tables #####

# determine years of interest
start <- 2010
finish <- 2022

id_cols <- c("time_period", "urn")

for (year in start:finish) {
  
  # skip covid years
  if(year == 2019 | year == 2020) next
  
  # determine academic year
  academic_year <- paste0(year,"-", year+1)
  
  # determine folder for academic year
  dir_year <- file.path(in_dir, academic_year)

  # read in absences data
  abs <- read.csv(file = file.path(dir_year, paste0(academic_year, "_england_abs.csv")))
  names(abs) <- gsub("X...", "", names(abs), fixed = T)
  
  # subset data to only include relevant schools
  names(abs) <- tolower(names(abs))
  
  # check for any strings
  cat(academic_year, "\n")
  print(apply(abs, 2, function(x) { unique(regmatches(x, gregexpr("[A-Za-z]+", x)))   }))
  
  # exclude national-level data
  abs <- abs %>% filter(! toupper(la) %in% c("NAT"))
  
  # Figures are suppressed (“supp”) where they concern fewer than 10 pupils.
  abs <- apply(abs, 2, function(x) {ifelse(x == "SUPP", NA, as.numeric(x))}) %>% as.data.frame()
  
  # add year
  abs$time_period <- as.numeric(gsub("-20", "", academic_year))
  
  # combine across years
  if (year == start) {
    df <- abs
  } else {
    df <- rbind.all.columns(df, abs)
  }
  
}

## VARIABLES ##

# PERCTOT	- Percentage of overall absence	PCT	Percentage of overall absence (authorised and unauthorised) for the full 2018/19 academic year.
# PPERSABS10 - Percentage of enrolments who are persistent absentees	PCT	Percentage of enrolments who are persistent absentees - missing 10% or more of possible sessions across the full 2018/19 academic year.


# create scaffold to safe data
urn_list <- unique(df$urn)
sum(is.na(urn_list))

scaffold <- merge(data.frame(time_period = as.numeric(years_list)),
                  data.frame(urn = as.numeric(urn_list)))

# merge with scaffold
df <- merge(scaffold, df, by = id_cols, all.x = T)

# remove duplicates
df <- df[!duplicated(df), ]

# save data
df <- df[with(df, order(urn, time_period)),]
data.table::fwrite(df, file = file.path(dir_data, "data_spt_absences.csv"), row.names = F)
