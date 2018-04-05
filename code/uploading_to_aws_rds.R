##################################################-
##################################################-
##
## Uploading to AWS RDS ----
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
library(RPostgres)
library(rstudioapi)
library(iterators)

#---------------------------------#
# Initializing databases ----
#---------------------------------#

indego_trip_db <- dbConnect(RSQLite::SQLite(), "./data/indego_trip_db.sqlite3")

indego_trip_db_aws <-
    dbConnect(
        Postgres(),
        host = "mydbinstance.cx8zelibyz0n.us-east-1.rds.amazonaws.com",
        port = "5432",
        dbname = "test_db",
        user = askForPassword("Database user"),
        password = askForPassword("Database password")
    )

#========================================#
# Reading and writing in chunks ----
#========================================#

#---------------------------------#
# operations to add data ----
#---------------------------------#

## iterator to keep track of chunk number ##

i_inf <- icount()


## opening up results set ##

trips_res <- indego_trip_db %>% dbSendQuery("SELECT * FROM trips")


## chunking operations ##

while (!dbHasCompleted(trips_res)) {
    
    
    ## fetch chunk ##
    
    trips_chunk <- dbFetch(trips_res, n = 1e5)
    
    
    ## printing pretty info ##
    
    cat("Chunk", nextElem(i_inf))
    cat("\n-------------------------------\n")
    # cat("|== ")
    # cat(min(trips_clean_chunk$last_updated_ct_date),
    #     "-",
    #     max(trips_clean_chunk$last_updated_ct_date))
    # cat(" ==|")
    # cat("\n-------------------------------\n") 
    
    
    ## writing to main database ##
    
    dbWriteTable(
        indego_trip_db_aws,
        "trips",
        value = trips_chunk,
        append = TRUE,
        temporary = FALSE
    )
    
}


## clear open results set ##

dbClearResult(trips_res)

#---------------------------------#
# checking results ----
#---------------------------------#

tbl(indego_trip_db, "trips") %>% select(trip_id) %>% collect() %>% nrow()
tbl(indego_trip_db_aws, "trips") %>% select(trip_id) %>% collect() %>% nrow()

tbl(indego_trip_db, "trips") %>% glimpse()
tbl(indego_trip_db_aws, "trips") %>% glimpse()


## disconnecting ##

dbDisconnect(indego_trip_db)
dbDisconnect(indego_trip_db_aws)


#========================================#
# Adding indexes ----
#========================================#

#---------------------------------#
# Connecting to databases ----
#---------------------------------#

indego_trip_db <- dbConnect(RSQLite::SQLite(), "./data/indego_trip_db.sqlite3")

indego_trip_db_aws <-
    dbConnect(
        Postgres(),
        host = "mydbinstance.cx8zelibyz0n.us-east-1.rds.amazonaws.com",
        port = "5432",
        dbname = "test_db",
        user = askForPassword("Database user"),
        password = askForPassword("Database password")
    )

#---------------------------------#
# checking indexes ----
#---------------------------------#

dbGetQuery(indego_trip_db, "SELECT * FROM sqlite_master WHERE type = 'index'")
dbGetQuery(indego_trip_db_aws, "SELECT * FROM sqlite_master WHERE type = 'index'")

#---------------------------------#
# adding indexes ----
#---------------------------------#

db_create_index(indego_trip_db_aws, "trips", "year")
db_create_index(indego_trip_db_aws, "trips", "month")
db_create_index(indego_trip_db_aws, "trips", "day")
db_create_index(indego_trip_db_aws, "trips", "yday")
db_create_index(indego_trip_db_aws, "trips", "qday")
db_create_index(indego_trip_db_aws, "trips", "wday")
db_create_index(indego_trip_db_aws, "trips", "start_time_hour")
db_create_index(indego_trip_db_aws, "trips", "start_time_date")
db_create_index(indego_trip_db_aws, "trips", "end_time_hour")
# db_create_index(indego_trip_db, "trips", "end_time_date")
db_create_index(indego_trip_db_aws, "trips", "start_station_id")
db_create_index(indego_trip_db_aws, "trips", "start_station_name")
db_create_index(indego_trip_db_aws, "trips", "end_station_id")
db_create_index(indego_trip_db_aws, "trips", "end_station_name")
db_create_index(indego_trip_db_aws, "trips", "bike_id")
db_create_index(indego_trip_db_aws, "trips", "trip_route_category")

db_list_tables(indego_trip_db)
db_list_tables(indego_trip_db_aws)

#---------------------------------#
# reviewing indexes ----
#---------------------------------#

dbGetQuery(indego_trip_db, "SELECT * FROM sqlite_master WHERE type = 'index'")

dbGetQuery(indego_trip_db_aws, "SELECT * FROM pg_indexes WHERE tablename NOT LIKE 'pg%'")

#---------------------------------#
# disconnecting ----
#---------------------------------#

dbDisconnect(indego_trip_db)
dbDisconnect(indego_trip_db_aws)

######################################################################################
######################################################################################
