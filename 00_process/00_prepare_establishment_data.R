# Empty the workspace in the global environment except for this function
rm(list = ls())

# Define the directory
dir <- getwd()

# Create subfolders if they do not exist
dir_data <- file.path(dir, "data")
if (!dir.exists(dir_data)) {
  dir.create(dir_data)
}

dir_misc <- file.path(dir, "misc")
if (!dir.exists(dir_misc)) {
  dir.create(dir_misc)
}

# Load necessary libraries
library(data.table)

# source: https://get-information-schools.service.gov.uk/Search
stem <- "Establishment_search"
# All establishments --> Download these search results --> Full set of data + Links



# source: https://get-information-schools.service.gov.uk/Downloads
stem <- "Establishment_downloads"
# Files available to download from 28 November 2024
# Establishment downloads (select all)
# Establishment fields CSV, 61.94 MB
# Establishment links CSV, 2.25 MB

# process data #

# Establishment links
links <- fread(file = file.path(dir_misc, stem, "links_edubasealldata20241128.csv"))
names(links) <- tolower(names(links))

# Establishment fields
fields <- fread(file = file.path(dir_misc, stem, "edubasealldata20241128.csv"))

# fix col names
names(fields) <- tolower(names(fields))
names(fields) <- gsub("(", "", names(fields), fixed = T)
names(fields) <- gsub(")", "", names(fields), fixed = T)
names(fields) <- gsub(" ", "_", names(fields), fixed = T)

# select cols
dt <- fields[, .SD, .SDcols = 
               grepl("urn|la_code|establishmentn|statut|boarders|nurs|sixth|gender|relig|dioc|admiss|trust|urban|country|street|town|postcode|open|close|typeofe|uprn|phase|status|gor", names(fields))]

# Subset data
dt <- dt[la_code != 0 & # no data from schools that are not affiliated with a LA 
           !is.na(establishmentnumber) & # or don't have an estab number
           !country_name %in% c("Gibraltar", "Jersey") & # or are in Jersey or Gibralta
           !grepl("Welsh|Further education|Miscellaneous|Higher education institutions", typeofestablishment_name) & # only schools
           !grepl("Wales|Not Applicable", gor_name)] # or Wales

# Ensure laestab_number has leading zeros and concatenate with dfe_number
dt[, establishmentnumber := sprintf("%04d", establishmentnumber)]
dt[, laestab := as.numeric(paste0(la_code, establishmentnumber))]

# extract key
key <- dt[, .(laestab, urn, establishmentname)]

# subset links data so that it only includes relevant URNs
links <- links[urn %in% key[, urn]]
# process date
links[, linkestablisheddate := ifelse(linkestablisheddate == "", NA_character_, linkestablisheddate)]
links[, linkestablisheddate := as.Date(linkestablisheddate, format = "%d-%m-%Y")]

# add link data
dt2 <- merge(dt, links, by = "urn", all.x = T)

# format dates
dt[, opendate := ifelse(opendate == "", NA_character_, opendate)]
dt[, opendate := as.Date(opendate, format = "%d-%m-%Y")]
dt[, closedate := ifelse(closedate == "", NA_character_, closedate)]
dt[, closedate := as.Date(closedate, format = "%d-%m-%Y")]

# replace 99[99] with NA
dt[, diocese_code := ifelse(diocese_code == "0000" | diocese_code == "9999", NA_character_, diocese_code)]
dt[, religiouscharacter_code := ifelse(religiouscharacter_code == 99, NA, religiouscharacter_code)]
dt[, previousla_code := ifelse(previousla_code == 999 | previousla_code == 0, NA, previousla_code)]
dt[, urbanrural_code := ifelse(urbanrural_code == "99", NA_character_, urbanrural_code)]
dt[, reasonestablishmentopened_code := ifelse(reasonestablishmentopened_code == 99, NA, reasonestablishmentopened_code)]
dt[, reasonestablishmentclosed_code := ifelse(reasonestablishmentclosed_code == 99, NA, reasonestablishmentclosed_code)]

# make all empty cells NA_character_
dt[, (names(dt)[sapply(dt, is.character)]) := lapply(.SD, function(x) { ifelse(x == "", NA_character_, x)}), .SDcols = sapply(dt, is.character)]

# fix religion #
# Define a pattern to match Christian denominations
christian_patterns <- c("Church", "Catholic", "Christian", "Anglican", "Methodist", "Adventist", "Evangelical", "Protestant", "Moravian", "Quaker", "Baptist")
# Create a regex pattern
pattern <- paste(christian_patterns, collapse = "|")

# Identify Christian denominations
dt[, religiouscharacter_christian := grepl(pattern, religiouscharacter_name, ignore.case = TRUE)]

out <- dt[, .(laestab, urn, la_code, establishmentnumber, establishmentname,
              street, postcode, town, gor_name,
              typeofestablishment_code, typeofestablishment_name, 
              establishmentstatus_name, opendate, reasonestablishmentopened_code, reasonestablishmentopened_name,
              closedate, reasonestablishmentclosed_code, reasonestablishmentclosed_name,
              phaseofeducation_code, phaseofeducation_name, statutorylowage, statutoryhighage,
              boarders_name, nurseryprovision_name, officialsixthform_name,
              gender_name, religiouscharacter_name, religiouscharacter_christian, diocese_name,
              admissionspolicy_name, urbanrural_name,
              trustschoolflag_name, trusts_code, trusts_name,
              previousla_code, previousestablishmentnumber
)]
# sort
setorder(out, laestab)

# save file
fwrite(out, file = file.path(dir_data, "data_establishments.csv"))
