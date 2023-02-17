library(sf)
library(tigris)
library(dplyr)
library(data.table)

input_dir <- "data/input"
output_dir <- "data/output"

# make state-level files
if(!dir.exists(input_dir)) dir.create(input_dir)

# make geocoded output file directory
if(!dir.exists(output_dir)) dir.create(output_dir)

ch7 = data.table::fread("~/Dropbox (MIT)/tanvi/geocoded_addresses/ch7.csv")
ch7 = ch7[matchtype != ""]

lapply(state.abb, function(x) {
  fcode <- tigris:::validate_state(x, .msg = FALSE)
  if(!dir.exists(file.path(input_dir, x))) dir.create(file.path(input_dir, x))
  fwrite(ch7[statefp == as.numeric(fcode)], file.path(input_dir, x, "ch7.csv"))
})

rm(ch7)
gc()


ch13 = data.table::fread("~/Dropbox (MIT)/tanvi/geocoded_addresses/ch13.csv")
ch13 = ch13[matchtype != ""]

lapply(state.abb, function(x) {
  fcode <- tigris:::validate_state(x, .msg = FALSE)
  if(!dir.exists(file.path(input_dir, x))) dir.create(file.path(input_dir, x))
  fwrite(ch13[statefp == as.numeric(fcode)], file.path(input_dir, x, "ch13.csv"))
})

rm(ch13)
gc()

# makes things nice and easy to reload
# but saves files locally 
# set to false if you'd rather not store things
options(tigris_use_cache = T)

# get block geographies for each decade
# blocks are not available for 1990
# so grabbing block groups instead

years = c(1990, 2000, 2010, 2020)
for (y in years) {
  message(y)
  purrr::map(state.abb, function(state) {
    message(state)
    outfile = file.path(output_dir, state, paste0(y, "_joined.csv"))
    if(!file.exists(outfile)){
      ch7_data = fread(file.path(input_dir, state, "ch7.csv"))
      ch13_data = fread(file.path(input_dir, state, "ch13.csv"))
      
      ch7_data[, filing_type := "Chapter 7"]
      ch13_data[, filing_type := "Chapter 13"]
      
      data = rbind(ch7_data, ch13_data)
      
      rm(ch7_data, ch13_data)
      
      # only have less detailed tracts for 1990
      if(y == 1990) {
        tigris_function = function(state, year) tigris::block_groups(state = state, year = year, cb = T)
        cb = T
      } else {
        tigris_function = tigris::blocks
      }
      
      # this is super hacky and i'm sorry 
      # but it does work, and it's not _that_ slow ? 
      # I'm sure someone who knows the sf package better
      # would know what to do here
      data = data %>% 
        mutate(point = purrr::map2(lat, lon, 
                                   .f = function(x, y) st_point(c(y, x)))) %>% 
        st_sf(crs = "NAD83") # change the coordinate reference system
                             # to match the census
      
      census_geo <- tigris_function(state = state, year = y)
      
      join <- st_join(data, census_geo, left = F)
      join <- as.data.table(join)
      setnames(join, colnames(join), tolower(gsub("00", "", colnames(join))))
      if(!dir.exists(file.path(output_dir, state))) dir.create(file.path(output_dir, state), recursive = T)
      fwrite(join, outfile)
    }
  })
}

# build appended version w/ all states & filing types
l = list.files("data/output", recursive = T)
lapply(l, function(x) {
  data = fread(paste0("data/output/", x))
  year = stringr::str_extract(x, "[0-9]{4}")
  data[,geography_year := year]
  fwrite(data, "data/output/geocoded_data.txt", append = T, sep = "|")
})




