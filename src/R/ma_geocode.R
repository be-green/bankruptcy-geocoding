library(data.table)
library(tidygeocoder)
library(stringr)
library(future)
library(furrr)

# get our parallelism going
future::plan(multisession)

# function for census geocoding in chunks
geocode_address_file <- function(address_df) {
  
  # limit per query is 10k
  # seems like no total limit
  # so nrow modulo 10k + 1 is number of chunks
  # could potentially do this in parallel if we wanted to I think
  # but better to not get rate limited
  # 10k seems to take ~40 seconds
  num_loop_chunks = nrow(address_df) %/% 10000 + 1
  
  address_list<- future_map(1:num_loop_chunks, function(i) {
    
    # go through addresses in 10k chunks
    start_num = (i - 1) * 10000 + 1
    
    # don't want to overshoot number of total addresses
    end_num = min(c(i * 10000, nrow(address_df)))
    
    message("Geocoding chunk ", i)
    address_subset = address_df[start_num:end_num,]
    
    # call census API
    coded <- geocode(address_subset, 
                     street = Debtor1_address1,
                     city = Debtor1_city,
                     state = Debtor1_state,
                     postalcode = Debtor1_zip,
                     return_addresses = T,
                     return_input = T,
                     full_results = T, 
                     method = "census",
                     api_options =
                       list(census_return_type = "geographies")
                     )
    coded <- as.data.table(coded)
    message("Chunk ", i, " ", 
            nrow(coded[!is.na(lat)]) / 10000 * 100, 
            "% successful")
    coded[]
  })
  
  # put it all in one table
  # and return it 
  rbindlist(address_list)
}

addresses <- fread("data/MA.csv")

start <- proc.time()
geocoded_addresses <- geocode_address_file(addresses)
end <- proc.time()

end - start

# we are happy with these (for mass it's ~62k / 77k)
# let's move on to the misses
matches <- geocoded_addresses[match_indicator == "Match"]

# of the 15k misses, po boxes are a large chunk
# let's analyze those separately
# because we might have to make some choices
po_boxes <- geocoded_addresses[str_detect(input_address, "PO BOX") & 
                                 (is.na(match_indicator) |
                                    match_indicator != "Match")]

# census reports NAs when there are ties which is strange
# these tend to be in a different class than the non-matches
# there are ~800 of these 
# the rest are straight up misses
# both tend to have lots of apartments and special characters
other_misses <- 
  geocoded_addresses[!str_detect(input_address, "PO BOX") & 
                                 (is.na(match_indicator) |
                                    match_indicator != "Match")]

# some of these have apartment numbers
# i'm going to remove those because they might be preventing matches
# there are ~815 of those or a little more than 1% of the full sample
other_misses[, Debtor1_address1 := str_to_upper(Debtor1_address1)]
other_misses[, Debtor1_address1 := str_replace_all(Debtor1_address1,
                                                   "APT [0-9-]+", "")]
other_misses[, Debtor1_address1 := str_replace_all(Debtor1_address1,
                                                   "Unit [0-9-]+", "")]

# some of the misses have special characters that could be messing things
# up (e.g. 1/2, #4, 32-34)
# i'm removing the 1/2's assuming that they should be part of 
# a building with the whole number,
# and just taking the first part of any address with a range
# I think this should be pretty safe, but want to be clear about 
# choices
other_misses[, Debtor1_address1 := str_replace_all(Debtor1_address1, 
                                                   "1/2", "")]
other_misses[, Debtor1_address1 := str_replace_all(Debtor1_address1, 
                                                   "#", "")]
other_misses[, Debtor1_address1 := str_replace_all(Debtor1_address1, 
                                                   "-[0-9]+", "")]
# several of the zipcodes got miscoded somehow
# and ended up with the form 123456789.0 when originally
# I think they were a 9 digit zip
other_misses[, Debtor1_zip := str_replace_all(Debtor1_zip, 
                                              fixed(".0"),
                                              "")]

# prep for re-geocoding
other_misses <- other_misses[,.SD, .SDcols = colnames(addresses)]

# re-geocode, this gets rid of ~9k misses
other_misses <- geocode_address_file(other_misses)

# combine stuff
matches <- rbind(
  matches,
  other_misses[match_indicator == "Match"],
  fill = T
)

# get new misses
other_misses <- other_misses[match_indicator != "Match"]

# all matches vs. all misses
num_matches <- nrow(matches)
num_misses <- nrow(rbind(
  other_misses,
  po_boxes,
  fill = T
))

message("Match rate: ", 
        round(num_matches / (num_matches + num_misses) * 100),
        "%")

message("Match rate, without considering ties and PO Boxes: ", 
        round(num_matches / (num_matches + nrow(other_misses)) * 100),
        "%")

fwrite(matches, file = "data/matches.csv")

fwrite(rbind(
  other_misses,
  po_boxes,
  fill = T
), file = "data/misses.csv")
