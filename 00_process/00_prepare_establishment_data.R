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
library(rvest)
library(dplyr)
library(foreach)
library(doParallel)

# Source external functions
devtools::source_url("https://github.com/stefaniemeliss/edu_stats/blob/main/functions.R?raw=TRUE")


#### source: https://get-information-schools.service.gov.uk/Search ####
# date downloaded 02/12/2024
stem <- "Establishment_search"
# All establishments --> Download these search results --> Full set of data + Links
res <- fread(file.path(dir_misc, stem, "results.csv"), fill = TRUE, header = TRUE, sep = ",")


# fix col names
names(res) <- tolower(names(res))
names(res) <- gsub("(", "", names(res), fixed = T)
names(res) <- gsub(")", "", names(res), fixed = T)
names(res) <- gsub(" ", "_", names(res), fixed = T)

# Convert encoding using iconv
res$establishmentname <- iconv(res$establishmentname, from = "latin1", to = "UTF-8")

# select cols
dt <- res[, .SD, .SDcols = 
            grepl("urn|la_code|establishmentn|statut|boarders|nurs|sixth|gender|relig|dioc|admiss|trust|urban|country|street|town|postcode|open|close|typeofe|uprn|phase|status|gor|linked|v1", names(res))]

dt <- res[, .SD, .SDcols = 
            grepl("urn|la_code|establishmentn|statut|boarders|nurs|sixth|gender|relig|dioc|admiss|trust|urban|country|street|town|postcode|open|close|typeofe|uprn|phase|status|gor|parlia|district|ward|la_name|linked|v1", names(res))]

# # Subset data
# dt <- dt[la_code != 0 & # no data from schools that are not affiliated with a LA
#            !is.na(establishmentnumber) & # or don't have an estab number
#            country_name %in% c("United Kingdom", "") & # or are in Jersey or Gibralta
#            !grepl("Welsh|Further education|Miscellaneous|Higher education institutions|Online provider", typeofestablishment_name) & # only schools
#            !grepl("Wales|Not Applicable", gor_name)] # or Wales

# concatenate info on linked establishments
start <- which(names(dt) == "linked_establishments")
dt[, links := do.call(paste, c(.SD, sep = " ")), .SDcols = start:ncol(dt)]

# Ensure laestab_number has leading zeros and concatenate with dfe_number
dt[, establishmentnumber_str := ifelse(!is.na(establishmentnumber), sprintf("%04d", establishmentnumber), NA)]
dt[, laestab := ifelse(!is.na(establishmentnumber), as.numeric(paste0(la_code, establishmentnumber_str)), NA)]

# format dates
dt[, opendate := ifelse(opendate == "", NA_character_, opendate)]
dt[, opendate := as.Date(opendate, format = "%d-%m-%Y")]
dt[, closedate := ifelse(closedate == "", NA_character_, closedate)]
dt[, closedate := as.Date(closedate, format = "%d-%m-%Y")]


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
fwrite(dt_long, file = file.path(dir_data, "data_linked_establishments.csv"), bom = T)

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
  
  years <- c(2015, 2019)
  
  for (year in years) {
    
    # list files
    file_list <- list.files(path = dir_misc, pattern = paste0(year, "-deprivation-by-postcodes_establishments_search"), full.names = T)
    
    # read and row bind csv files
    imd <- data.table::rbindlist(lapply(file_list, data.table::fread))
    
    # file file
    data.table::fwrite(imd, file.path(dir_misc, paste0("data_deprivation_", year, "_by_school_postcodes_establishments_search.csv")))
    
  }
  
}

# add IDACI
add_deprivation_data = F

if (add_deprivation_data) {
  
  years <- c(2015, 2019)
  
  for (year in years) {
    
    
    # read in file
    imd <- data.table::fread(file.path(dir_misc, paste0("data_deprivation_", year, "_by_school_postcodes_establishments_search.csv")))
    
    # Income Deprivation Affecting Children Index (IDACI)
    # IDACI decile categorises areas into ten groups (deciles) based on the proportion of children living in income-deprived households. 
    # Each decile represents 10% of areas, with decile 1 being the most deprived and decile 10 being the least deprived.
    
    # change col names
    imd[, postcode := Postcode]
    imd[, paste0("idaci_decile_", year) := `IDACI Decile`]
    imd[, paste0("imd_employment_decile_", year) := `Employment Decile`]
    
    # subset data
    imd <- imd[`Postcode Status` != "**UNMATCHED**", ] # remove all rows for which the tool did not find a match
    
    select <- c("postcode", paste0("idaci_decile_", year), paste0("imd_employment_decile_", year))
    imd <- imd[, ..select]
    
    # merge with df
    dt <- merge(dt, imd, by = "postcode", all.x = T)
    
  }
  
}

# add latitude and longitute

add_coord_data = F

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
              street, postcode, town, gor_name, la_name, districtadministrative_name, administrativeward_name, parliamentaryconstituency_name, #lat, long,
              typeofestablishment_name, 
              establishmentstatus_name, opendate, reasonestablishmentopened_name,
              closedate, reasonestablishmentclosed_name,
              phaseofeducation_name, statutorylowage, statutoryhighage,
              boarders_name, nurseryprovision_name, officialsixthform_name,
              gender_name, sex_students, religiouscharacter_name, religiouscharacter_christian, diocese_name,
              admissionspolicy_name, urbanrural_name, urbanicity, #idaci_decile_2015, idaci_decile_2019, imd_employment_decile_2015, imd_employment_decile_2019,
              trustschoolflag_name, trusts_name, links
)]

# sort
setorder(out, urn)

# save file
fwrite(out, file = file.path(dir_data, "data_establishments_search.csv"), bom = T)


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
links$linkname <- iconv(links$linkname, from = "latin1", to = "UTF-8")

# Establishment fields
fields <- fread(file = file.path(dir_misc, stem, "edubasealldata20241202.csv"))

# fix col names
names(fields) <- tolower(names(fields))
names(fields) <- gsub("(", "", names(fields), fixed = T)
names(fields) <- gsub(")", "", names(fields), fixed = T)
names(fields) <- gsub(" ", "_", names(fields), fixed = T)

fields$establishmentname <- iconv(fields$establishmentname, from = "latin1", to = "UTF-8")

# select cols
dt <- fields[, .SD, .SDcols = 
               grepl("urn|la_code|establishmentn|statut|boarders|nurs|sixth|gender|relig|dioc|admiss|trust|urban|country|street|town|postcode|open|close|typeofe|uprn|phase|status|gor", names(fields))]

# # Subset data
# dt <- dt[la_code != 0 & # no data from schools that are not affiliated with a LA
#            !is.na(establishmentnumber) & # or don't have an estab number
#            !country_name %in% c("Gibraltar", "Jersey") & # or are in Jersey or Gibralta
#            !grepl("Welsh|Further education|Miscellaneous|Higher education institutions", typeofestablishment_name) & # only schools
#            !grepl("Wales|Not Applicable", gor_name)] # or Wales

# Ensure laestab_number has leading zeros and concatenate with dfe_number
dt[, establishmentnumber_str := ifelse(!is.na(establishmentnumber), sprintf("%04d", establishmentnumber), NA)]
dt[, laestab := ifelse(!is.na(establishmentnumber), as.numeric(paste0(la_code, establishmentnumber_str)), NA)]

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
setorder(out, urn)

# save file
fwrite(out, file = file.path(dir_data, "data_establishments_download.csv"), bom = T)

# Establishment groups
groups <- fread(file = file.path(dir_misc, stem, "academiesmatmembership20241202.csv"))

# fix col names
names(groups) <- tolower(names(groups))
names(groups) <- gsub("(", "", names(groups), fixed = T)
names(groups) <- gsub(")", "", names(groups), fixed = T)
names(groups) <- gsub(" ", "_", names(groups), fixed = T)

groups$establishmentname <- iconv(groups$establishmentname, from = "latin1", to = "UTF-8")
groups$group_name <- iconv(groups$group_name, from = "latin1", to = "UTF-8")

# filter rows
groups <- groups[group_status != "Closed"]
groups <- groups[establishmentstatus_name != "Closed"]
groups <- groups[group_type == "Multi-academy trust"]

# select cols
groups <- groups[, .SD, .SDcols = 
                grepl("urn|la_|number|id|date|phase|reason|establishmentname|group_name", names(groups))]
groups <- groups[, .SD, .SDcols = 
             !grepl("n_code|d_code|companies|ofsted", names(groups))]

# add laestab
groups[, laestab := as.numeric(gsub("/", "", dfe_number))]

# add region
lookup <- fields[, la_name, gor_name]
lookup <- lookup[!duplicated(lookup)]
lookup <- lookup[!gor_name %in% c("Not Applicable", "Wales (pseudo)")]

groups <- merge(lookup, groups, by = "la_name", all.y = T)
groups <- groups[!is.na(urn)] # remove MATs without any schools

# save file
fwrite(groups, file = file.path(dir_data, "data_establishments_groups.csv"), bom = T)


#### source: https://get-information-schools.service.gov.uk/Groups/Search?SelectedTab=Groups&b=1&b=4&search-by=all&searchtype=GroupAll ####
# date downloaded 02/04/2025
gc()
stem <- "Establishment_groups"
# All establishments --> Download these search results --> Full set of data + Links
groups <- fread(file = file.path(dir_misc, stem, "GroupExtract.csv"), fill = Inf)

# Focus on MATs
groups <- groups[`Group Type` == "Multi-academy trust"]

# Focus on MATs with linked establishments
groups <- groups[`Number of linked providers` > 0]

# save UIDs for querying
mat_uids <- groups[, UID] 
# mat_uids <- mat_uids[1:3] # debug

query_schools <- F
if (query_schools) {
  
  # Set up parallel processing
  num_cores <- detectCores() - 2  # Use two less than the total number of cores
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  # Parallel processing of MAT UIDs using foreach
  results_list <- foreach(mat_uid = mat_uids, .packages = c("rvest", "dplyr", "stringr")) %dopar% {
    mat_schools <- get_schools_for_mat(mat_uid)
    return(mat_schools)
  }
  
  # Stop the cluster
  stopCluster(cl)
  
  # Combine the results into a single data frame
  results <- bind_rows(results_list)
  
  # change column names
  names(results) <- tolower(names(results))
  names(results) <- gsub("_name", "", names(results))
  names(results) <- gsub("school_", "", names(results))
  
  # format date
  results$joined_date <- as.Date(results$joined_date, format = "%d %B %Y")
  
  # add region
  results <- results %>%
    left_join(., lookup, by = join_by(local_authority == la_name))
  
  # save file
  fwrite(results, file = file.path(dir_data, "data_establishments_groups.csv"), bom = T)
}

