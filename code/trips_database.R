##################################################-
##################################################-
##
## Creating indego trips database ----
##
##################################################-
##################################################-


#========================================#
# Setting up ----
#========================================#

#---------------------------------#
# Loading libraries ----
#---------------------------------#

library(tidyverse)
library(lubridate)
library(magrittr)
library(DBI)
library(dbplyr)
library(fs)
library(glue)

#---------------------------------#
# Loading station info ----
#---------------------------------#

stations <- read_csv("./data/raw/indego-stations-2018-1-19.csv") %>% as_tibble()

#---------------------------------#
# Initializing database ----
#---------------------------------#

indego_trip_db <- dbConnect(RSQLite::SQLite(), "./data/indego_trip_db.sqlite3")


#========================================#
# Finding missing dates and times ----
#========================================#


# all_hours <-
#     data_frame(
#         start_time_hourly =
#             seq(
#                 as_datetime("2017-01-01 00:00:00", tz = "US/Eastern"),
#                 as_datetime("2017-12-31 23:00:00", tz = "US/Eastern"),
#                 "hour"
#             ))
# 
# 
# missing_hours <- 
#     tbl(citibike_trip_db, "citibike_trips") %>%
#     filter(year == 2017) %>%
#     select(start_time) %>%
#     collect() %>%
#     mutate(
#         start_time            = as_datetime(start_time, tz = "US/Eastern"),
#         start_time_hourly     = floor_date(start_time, "hours")
#     ) %>% 
#     count(start_time_hourly) %>% 
#     left_join(all_hours, .) %>% 
#     filter(is.na(n)) %>% 
#     mutate(start_time_hourly_num = as.numeric(start_time_hourly))
# 
# 
# missing_months <- 
#     missing_hours %>% 
#     mutate(
#         start_time_month = floor_date(start_time_hourly, "months") %>% 
#             as_date() %>% 
#             format.Date(x = ., format = "%Y%m")
#     ) %>% 
#     distinct(start_time_month)


#====================================================#
# Getting data frame of file names to extract ----
#====================================================#


# file_dates <- 
#     missing_months %>% 
#     pull(start_time_month) %>% 
#     str_c(., collapse = "|")


trip_files <- 
    dir_info("./data/raw/", recursive = TRUE) %>%
    filter(str_detect(path, "\\.zip")) 


#====================================================#
# Extracting trip data and saving to database ----
#====================================================#

## running loop ##

for (i in 1:nrow(trip_files)) {
    
    
    ### unzipping files ###
    
    unzip(
        zipfile = trip_files$path[i],
        exdir = "./data/raw/unzipped"
    )
    
    
    ## getting name of unzipped file ##
    
    unzipped_files <- dir_info("./data/raw/unzipped", recursive = TRUE)
    
    
    ## cleaning the data ##
    
    trip_unzip <- 
        
        
        ## reading csv ##
        
        read_csv(unzipped_files$path) %>% as_tibble() %>%
        
        
        ## getting rid of the problems attribute, which can get huge ##
        
        `attr<-`("problems", NULL) %>%
        
        
        ## parsing datetime columns ##
        
        mutate(
            
            start_time =
                parse_date_time(start_time,
                                orders = c("ymd HMS", "ymd HM", "mdy HMS", "mdy HM"),
                                tz     = "US/Eastern"),
            
            end_time =
                parse_date_time(end_time,
                                orders = c("ymd HMS", "ymd HM", "mdy HMS", "mdy HM"),
                                tz     = "US/Eastern")
        ) %>% 
        
        
        ## accounting for varying column names ##
        
        rename_at(
            vars(matches("start_station\\b")), 
            funs(str_replace(., "start_station", "start_station_id"))) %>% 
        
        rename_at(
            vars(matches("end_station\\b")), 
            funs(str_replace(., "end_station", "end_station_id"))) %>% 
        
        rename(
            trip_duration           = duration,
            start_station_latitude  = start_lat,
            start_station_longitude = start_lon,
            end_station_id          = end_station_id,
            end_station_latitude    = end_lat,
            end_station_longitude   = end_lon,
            bike_id                 = bike_id
        ) %>% 
        
    
        ## coercing and otherwise recoding columns ##
        
        mutate(
            start_time_chr          = as.character(start_time),
            start_time_utc          = with_tz(start_time, tzone = "UTC"),
            start_time_utc_chr      = as.character(start_time_utc),
            start_time_date         = as_date(start_time),
            start_time_min          = floor_date(start_time, unit = "minute"),
            start_time_hour         = floor_date(start_time, unit = "hour"),
            
            end_time_min            = floor_date(end_time, unit = "minute"),
            end_time_hour           = floor_date(end_time, unit = "hour"),
            
            year                    = year(start_time) %>% as.integer(),
            month                   = month(start_time) %>% as.integer(),
            day                     = day(start_time) %>% as.integer(),
            yday                    = yday(start_time) %>% as.integer(),
            qday                    = qday(start_time) %>% as.integer(),
            wday                    = wday(start_time) %>% as.integer(),
            
            trip_id                 = as.integer(trip_id),
            trip_duration           = as.integer(trip_duration),
            start_station_id        = as.integer(start_station_id),
            start_station_latitude  = as.double(start_station_latitude),
            start_station_longitude = as.double(start_station_longitude),
            end_station_id          = as.integer(end_station_id),
            end_station_latitude    = as.double(end_station_latitude),
            end_station_longitude   = as.double(end_station_longitude),
            bike_id                 = as.integer(bike_id),
            plan_duration           = as.integer(plan_duration)
            
        ) %>% 
        
        
        ## adding station names ##
        
        left_join(., 
                  stations %>% select(station_id, station_name), 
                  by = c("start_station_id" = "station_id")) %>% 
        
        rename(start_station_name = station_name) %>% 
        
        
        left_join(., 
                  stations %>% select(station_id, station_name), 
                  by = c("end_station_id" = "station_id")) %>% 
        
        rename(end_station_name = station_name)
    
    
    ## writing to database ##
    
    dbWriteTable(
        indego_trip_db,
        "trips",
        value = trip_unzip,
        append = TRUE,
        temporary = FALSE
    )
    
    
    ### Print some pretty details of your progress ###
    
    cat("===============================\n")
    cat("Loop", i, "\n")
    cat(nrow(trip_unzip), "rows added")
    cat("\n-------------------------------\n")
    
    print(glue(
        "|-- ",
        "{trip_unzip$start_time_date %>% min() %>% as.character()}",
        " - ",
        "{trip_unzip$start_time_date %>% max() %>% as.character()}",
        " --|"
    ))
    
    cat("-------------------------------\n")  
    
    ## removing the previous data from memory to avoid duplicates ##
    
    rm(trip_unzip)
    
    ## removing the unzipped files ##
    
    file.remove(unzipped_files$path)
    
    gc()
    
}

dbDisconnect(indego_trip_db)


#---------------------------------#
# Creating indexes ----
#---------------------------------#

indego_trip_db <- dbConnect(RSQLite::SQLite(), "./data/indego_trip_db.sqlite3")

tbl(indego_trip_db, "trips") %>% glimpse()

db_create_index(indego_trip_db, "trips", "year")
db_create_index(indego_trip_db, "trips", "month")
db_create_index(indego_trip_db, "trips", "day")
db_create_index(indego_trip_db, "trips", "yday")
db_create_index(indego_trip_db, "trips", "qday")
db_create_index(indego_trip_db, "trips", "wday")
db_create_index(indego_trip_db, "trips", "start_time_hour")
db_create_index(indego_trip_db, "trips", "start_time_date")
db_create_index(indego_trip_db, "trips", "end_time_hour")
# db_create_index(indego_trip_db, "trips", "end_time_date")
db_create_index(indego_trip_db, "trips", "start_station_id")
db_create_index(indego_trip_db, "trips", "start_station_name")
db_create_index(indego_trip_db, "trips", "end_station_id")
db_create_index(indego_trip_db, "trips", "end_station_name")
db_create_index(indego_trip_db, "trips", "bike_id")
db_create_index(indego_trip_db, "trips", "trip_route_category")

db_list_tables(indego_trip_db)

## checking indexes ##

dbGetQuery(indego_trip_db, "SELECT * FROM sqlite_master WHERE type = 'index'")

dbExecute(indego_trip_db, "VACUUM")


#######################

dbDisconnect(indego_trip_db)

############################################################
############################################################
