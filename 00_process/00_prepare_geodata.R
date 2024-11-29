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

stem <- "ONSPD_NOV_2024"


# DOWNLOAD data: ONS Postcode Directory (November  2024) for the UK #
download_data = F
if (download_data) {
  # Determine header information
  headers <- c(
    `user-agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Safari/537.36'
  )
  
  # determine source url
  url = "https://www.arcgis.com/sharing/rest/content/items/b54177d3d7264cd6ad89e74dd9c1391d/data"
  
  # retrieve information url = # retrieve information from URL 
  request <- httr::GET(url = url, httr::add_headers(.headers=headers))
  
  # retrieve raw content from request
  bin <- httr::content(request, "raw")
  
  # write binary data to file
  file_name <- file.path(dir_misc, paste0(stem, ".zip"))
  writeBin(bin, file_name)
  
  # unzip folder and remove
  if (!dir.exists(file.path(dir_misc, stem))) {
    dir.create(file.path(dir_misc, stem))
  }
  unzipped <- unzip(file_name, exdir = file.path(dir_misc, stem))
  file.remove(file_name)
  
  # clear RAM
  rm(request, bin)
  gc()
  
}


# process data #

# ONS Postcode Directory (November  2024) for the UK
dt <- fread(file = file.path(dir_misc, stem, "Data", "ONSPD_NOV_2024_UK.csv"))

# filter for England
dt <- dt[ctry == "E92000001"]

# remove missing data
dt <- dt[lat != 100 & long != 0]

# filter for active postcodes
dt <- dt[is.na(doterm)]

# Extract the area code (everything before the first number)
# pcd2 = Unit postcode – 8 character version, 5th character always blank, and 3rd and 4th characters may be blank ()
dt[, pcd_area := sub(" .*", "", pcd2)]
# rename var
dt[, region_code := rgn]

# reduce columns
dt <- dt[, .(pcd_area, region_code)]

# remove duplicates
dt <- unique(dt)

# free RAM
gc()

# Load the lookup table
lookup_table <- fread(file = file.path(dir_misc, stem, "Documents", "Region names and codes EN as at 12_20 (RGN).csv"))
lookup_table[, region := RGN20NM]
lookup_table <- lookup_table[, .(RGN20CD, region)]

# Merge with the lookup table to get regions
dt <- merge(dt, lookup_table, by.x = "region_code", by.y = "RGN20CD", all.x = TRUE)

# save lookup file
fwrite(dt, file = file.path(dir_misc, "lookup_postcodearea_region.csv"))
