# Empty the workspace in the global environment except for this function
rm(list = ls())
gc()

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

#### source: https://get-information-schools.service.gov.uk/Search ####
# date downloaded 02/12/2024
stem <- "Establishment_search"
# All establishments --> Download these search results --> Full set of data + Links
res <- fread(file = file.path(dir_misc, stem, "results.csv"), fill = Inf)

# fix col names
names(res) <- tolower(names(res))
names(res) <- gsub("(", "", names(res), fixed = T)
names(res) <- gsub(")", "", names(res), fixed = T)
names(res) <- gsub(" ", "_", names(res), fixed = T)

# select cols
dt <- res[, .SD, .SDcols = 
               grepl("urn|la_code|establishmentn|statut|boarders|nurs|sixth|gender|relig|dioc|admiss|trust|urban|country|street|town|postcode|open|close|typeofe|uprn|phase|status|gor|linked|v1", names(res))]

# # Subset data
# dt <- dt[la_code != 0 & # no data from schools that are not affiliated with a LA 
#            !is.na(establishmentnumber) & # or don't have an estab number
#            country_name %in% c("United Kingdom", "") & # or are in Jersey or Gibralta
#            !grepl("Welsh|Further education|Miscellaneous|Higher education institutions|Online provider", typeofestablishment_name) & # only schools
#            !grepl("Wales|Not Applicable", gor_name)] # or Wales

# Ensure laestab_number has leading zeros and concatenate with dfe_number
dt[, establishmentnumber := sprintf("%04d", establishmentnumber)]
dt[, laestab := as.numeric(paste0(la_code, establishmentnumber))]

# format dates
dt[, opendate := ifelse(opendate == "", NA_character_, opendate)]
dt[, opendate := as.Date(opendate, format = "%d-%m-%Y")]
dt[, closedate := ifelse(closedate == "", NA_character_, closedate)]
dt[, closedate := as.Date(closedate, format = "%d-%m-%Y")]

# concatenate info on linked establishments
dt[, links := do.call(paste, c(.SD, sep = " ")), .SDcols = 31:42]

# make all empty cells NA_character_
dt[, (names(dt)[sapply(dt, is.character)]) := lapply(.SD, function(x) { ifelse(x == "", NA_character_, x)}), .SDcols = sapply(dt, is.character)]

# linked data #

# data in wide format
linked <- dt[, .SD, .SDcols = 
            grepl("urn|laestab|establishmentname|linked|v1", names(dt))]

# reformat
linked[, any := fifelse(linked_establishments == "Does not have links", FALSE, TRUE)]
linked[, linked_establishments := fifelse(linked_establishments == "Does not have links", NA_character_, linked_establishments)]

# rename
names(linked)[grepl("linked|v1", names(linked))] <- paste0("linked_urn_", 1:sum(grepl("linked|v1", names(linked))))

# add associated urn
linked[, linked_urn_0 := paste(urn, "default urn")]

# data in long format
dt_long <- melt(linked, id.vars = c("urn", "establishmentname", "laestab", "any"), variable.name = "variable", value.name = "linked_establishments")

dt_long[, variable := as.character(variable)] # for sorting

# extract urns in columns
dt_long[, linked_urn := as.numeric(stringr:: str_extract(linked_establishments, "^\\d{6}"))]

# remove all rows with NAs
dt_long <- dt_long[!is.na(linked_urn)]

# create the variable 'linked_laestab'
dt_long[dt_long, linked_laestab := i.laestab, on = .(linked_urn = urn)]

# create column to compare laestab
dt_long[, check_laestab := laestab == linked_laestab]

# create the variable 'linked_name'
dt_long[dt_long, linked_establishmentname := i.establishmentname, on = .(linked_urn = urn)]


# sort
setorder(dt_long, urn, variable)

# save file
fwrite(dt_long, file = file.path(dir_data, "data_linked_establishments.csv"))

# fix religion #
# Define a pattern to match Christian denominations
christian_patterns <- c("Church", "Catholic", "Christian", "Anglican", "Methodist", "Adventist", "Evangelical", "Protestant", "Moravian", "Quaker", "Baptist")
# Create a regex pattern
pattern <- paste(christian_patterns, collapse = "|")

# Identify Christian denominations
dt[, religiouscharacter_christian := grepl(pattern, religiouscharacter_name, ignore.case = TRUE)]

# fix urbanicity
dt[, urbanicity := ifelse(grepl("Urban", urbanrural_name), "Urban",
                          ifelse(grepl("Rural", urbanrural_name), "Rural", NA))]

# fix gender
dt[, sex_students := fifelse(dt[, gender_name] == "Girls" | dt[, gender_name] == "Boys", "Single-sex",
                             fifelse(dt[, gender_name] == "Mixed", "Co-ed", dt[, gender_name]))]


# extract postcodes and process deprivation data #

extract_postcodes = F

if(extract_postcodes){
  
  # extract
  pcd <- as.data.frame(na.omit(unique(dt$postcode)))
  
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
  save_in_chunks(pcd, chunk_size = 10000, file_prefix = "postcodes_establishments_search", output_folder = dir_misc)
  
}

# outside of script, upload files to https://imd-by-postcode.opendatacommunities.org/imd/2019 to get deprivation data

# process deprivation data
process_deprivation_data = F

if(process_deprivation_data){
  
  # list files
  file_list <- list.files(path = dir_misc, pattern = "2019-deprivation-by-postcodes_establishments_search", full.names = T)
  
  # read and row bind csv files
  depr <- data.table::rbindlist(lapply(file_list, data.table::fread))
  
  # file file
  data.table::fwrite(depr, file.path(dir_misc, "data_deprivation_2019_by_school_postcodes_establishments_search.csv"))
  
}

# add IDACI
add_deprivation_data = T

if (add_deprivation_data) {
  
  # read in file
  depr <- data.table::fread(file.path(dir_misc, "data_deprivation_2019_by_school_postcodes_establishments_search.csv"))
  
  # Income Deprivation Affecting Children Index (IDACI)
  # IDACI decile categorises areas into ten groups (deciles) based on the proportion of children living in income-deprived households. 
  # Each decile represents 10% of areas, with decile 1 being the most deprived and decile 10 being the least deprived.
  
  # change col names
  depr[, postcode := Postcode]
  depr[, idaci_decile := `IDACI Decile`]
  
  # subset data
  depr <- depr[`Postcode Status` != "**UNMATCHED**", .(postcode, idaci_decile)]
  
  # merge with df
  dt <- merge(dt, depr, by = "postcode")

}

# add latitude and longitute

add_coord_data = T

if (add_coord_data) {
  
  # read in ONS data
  ons <- data.table::fread(file.path(dir_misc, "ONSPD_NOV_2024", "Data", "ONSPD_NOV_2024_UK.csv"))

  # get school postcodes
  postcodes <- c(unique(dt[, postcode]))
  
  # filter ONS data
  ons <- ons[pcds %in% postcodes, ]
  ons[, postcode := pcds]
  ons <- ons[, .(postcode, lat, long)]
  
  # merge 
  dt <- merge(dt, ons, by = "postcode", all.x = T)
}
  

# select columns
out <- dt[, .(laestab, urn, la_code, establishmentnumber, establishmentname,
              street, postcode, town, gor_name, lat, long,
              typeofestablishment_name, 
              establishmentstatus_name, opendate, reasonestablishmentopened_name,
              closedate, reasonestablishmentclosed_name,
              phaseofeducation_name, statutorylowage, statutoryhighage,
              boarders_name, nurseryprovision_name, officialsixthform_name,
              gender_name, sex_students, religiouscharacter_name, religiouscharacter_christian, diocese_name,
              admissionspolicy_name, urbanrural_name, urbanicity, idaci_decile,
              trustschoolflag_name, trusts_name, links
)]

# sort
setorder(out, urn)

# save file
fwrite(out, file = file.path(dir_data, "data_establishments_search.csv"))


#### source: https://get-information-schools.service.gov.uk/Downloads ####
stem <- "Establishment_downloads"
# date downloaded 02/12/2024
# Establishment downloads (select all)
# Establishment fields CSV, 61.94 MB
# Establishment links CSV, 2.25 MB

# process data #

# Establishment links
links <- fread(file = file.path(dir_misc, stem, "links_edubasealldata20241202.csv"))
names(links) <- tolower(names(links))

# Establishment fields
fields <- fread(file = file.path(dir_misc, stem, "edubasealldata20241202.csv"))

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
fwrite(out, file = file.path(dir_data, "data_establishments_download.csv"))

