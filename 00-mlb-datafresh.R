library(DBI)
library(RMySQL)
library(pitchRx)
library(dplyr)
library(dbplyr)
library(stringr)

## Functions at Top

get_quant_score <- function(des) {
    score <- (
        as.integer(str_detect(des, "Called Strike")) * -(1/3) +
            as.integer(str_detect(des, "Foul")) * -(1/3) +
            as.integer(str_detect(des, "In play, run")) * 1.0 +
            as.integer(str_detect(des, "In play, out")) * -1.0 +
            as.integer(str_detect(des, "In play, no out")) * 1.0 +
            as.integer(str_detect(des, "^Ball$")) * 0.25 +
            as.integer(str_detect(des, "Swinging Strike")) * -(1/2.5) +
            as.integer(str_detect(des, "Hit By Pitch")) * 1.0 +
            as.integer(str_detect(des, "Ball In Dirt")) * 0.25 +
            as.integer(str_detect(des, "Missed Bunt")) * -(1/3) +
            as.integer(str_detect(des, "Intent Ball")) * 0.25
    )
    return(score)
}
get_qual_score <- function(des) {
    score <- (
        as.integer(str_detect(des, "homer")) * 2 +
            as.integer(str_detect(des, "line")) * 1.5 +
            as.integer(str_detect(des, "sharp")) * 1.5 +
            as.integer(str_detect(des, "grounds")) * -1 +
            as.integer(str_detect(des, "flies")) * -1 +
            as.integer(str_detect(des, "soft")) * -2 +
            as.integer(str_detect(des, "pop")) * -2 +
            as.integer(str_detect(des, "triples")) * 1.5 +
            as.integer(str_detect(des, "doubles")) * 1.0 +
            as.integer(str_detect(des, "error")) * 0.5
    )
    return(score)
}

fix_quant_score <- function(event) {
    score <- (
        as.integer(str_detect(event, "Groundout")) * -2 +
            as.integer(str_detect(event, "Forceout")) * -2 +
            as.integer(str_detect(event, "Field Error")) * -2 
    )
    return(score)
}

drop_scrape_tables <- function() {

    my_scrape_db_dbi <- DBI::dbConnect(RMySQL::MySQL(), 
                        host = Sys.getenv("mlb_db_hostname"),
                        dbname = Sys.getenv("mlb_db_scrape"),
                        user = Sys.getenv("mlb_db_username"),
                        password = Sys.getenv("mlb_db_password")
    )

    print("R program running: cleaning up - wiping scape database")

    dbGetQuery(my_scrape_db_dbi, "DROP TABLE IF EXISTS pitch")
    dbGetQuery(my_scrape_db_dbi, "DROP TABLE IF EXISTS action")
    dbGetQuery(my_scrape_db_dbi, "DROP TABLE IF EXISTS runner")
    dbGetQuery(my_scrape_db_dbi, "DROP TABLE IF EXISTS po")
    dbGetQuery(my_scrape_db_dbi, "DROP TABLE IF EXISTS atbat")

    print("R program running: cleaning up - scape database tables dropped")
    dbDisconnect(my_scrape_db_dbi)

}

datafresh <- function(day) {
    print(paste0("R program running: DataFresh scraping and loading: ", format(day,"%m-%d-%y")))

    #connection used by datfresh to store processed MLB data long term - via DBI::dbWriteTable()
    #connection used by DBI::dbWriteTable() to write new dataframe to MYSQL database
    my_mlb_db <- DBI::dbConnect(RMySQL::MySQL(), 
                        host = Sys.getenv("mlb_db_hostname"),
                        dbname = Sys.getenv("mlb_db_dbname"),
                        user = Sys.getenv("mlb_db_username"),
                        password = Sys.getenv("mlb_db_password")
    )

    #connection used by pitchrx scrape function via dbply/dplyr
    my_scrape_db <- src_mysql(dbname = Sys.getenv("mlb_db_scrape"), host = Sys.getenv("mlb_db_hostname"), port = 3306, user = Sys.getenv("mlb_db_username"), password = Sys.getenv("mlb_db_password"))
    #try (if there are games) to scrape for this day using the scrape connection and write into the scrape db
    try(scrape(start = format(day,"%Y-%m-%d"), end = format(day,"%Y-%m-%d"), suffix = "inning/inning_all.xml", connect = my_scrape_db$con))
    # clean up the scrape connection 
    # when we are finished with our connection used by the scrape funtion - clean up per Hadley Wickham -> https://github.com/tidyverse/dplyr/issues/950
    rm(my_scrape_db)
    # repeat for all days in range

    # establish DBI connection to scrape database to check if tables are there
    my_scrape_db_dbi <- DBI::dbConnect(RMySQL::MySQL(), 
                    host = Sys.getenv("mlb_db_hostname"),
                    dbname = Sys.getenv("mlb_db_scrape"),
                    user = Sys.getenv("mlb_db_username"),
                    password = Sys.getenv("mlb_db_password"))

    ## as long as there are pitch and atbat tables in the scrape database - we should be okay
    ## if not clean up and don't do anything else for this day 
    if ( (dbExistsTable(my_scrape_db_dbi, "pitch")) && (dbExistsTable(my_scrape_db_dbi, "atbat")) ) {

        # close the connection that checked if data was in the scrape database
        dbDisconnect(my_scrape_db_dbi)

        print("R program running: pulling pitch and atbat dataframes from scrape database")

        ### Load pitch and atbat data frames
        my_scrape_db <- src_mysql(dbname = Sys.getenv("mlb_db_scrape"), host = Sys.getenv("mlb_db_hostname"), port = 3306, user = Sys.getenv("mlb_db_username"), password = Sys.getenv("mlb_db_password"))

        # don't close connections until your done with the data.  use %>% collect() to run the queries now, otherwise, queries are ececuted later when dataframe objects are used
        # %>% collect() this actually runs the queries and stores the data in the data frame
        # Collect all table data for atbat and pitch tables
        # This doesn't work... too many common columns?
        #atbat_untouched <- tbl(my_scrape_db, "atbat") %>% collect()
        #pitch_untouched <- tbl(my_scrape_db, "pitch") %>% collect()
        # this does work... follows original code from Jason Battles
        # this does work... collect everything from pitch table except event_num, inning, inning_side, next_, play_guid, url
        # this does work... collect everything from atbat table except event_num, next_, play_guid, url
        pitch_untouched <- select(tbl(my_scrape_db, "pitch"), gameday_link, num, ax, ay, az, break_angle, break_length, break_y, cc, code, count, des, end_speed, id, mt, nasty, on_1b, on_2b, on_3b, pfx_x, pfx_z, pitch_type, px, pz, spin_dir, spin_rate, start_speed, sv_id, sz_bot, sz_top, tfs, tfs_zulu, type, type_confidence, vx0, vy0, vz0, x, x0, y, y0, z0, zone) %>% collect()
        atbat_untouched <- select(tbl(my_scrape_db, "atbat"), gameday_link, date, num, pitcher, batter, b_height, pitcher_name, p_throws, batter_name, stand, atbat_des, event, inning, inning_side) %>% collect()
        
        # Dropping columns whose name contain "_es" at the end | not keeping spanish language versions of data
        # columns are not always included and can cause database load challenges
        atbatsDF <- atbat_untouched[,!grepl("_es$",names(atbat_untouched))]
        pitchesDF <- pitch_untouched[,!grepl("_es$",names(pitch_untouched))]

        # Date stored as character class  - "2018_04_01" | adjust to actual date values
        atbatsDF$date <- as.Date(atbatsDF$date , "%Y_%m_%d")

        print("R program running: performing inner join on pitch and atbat data")

        # join filtered atbats to all pitches
        pitchesJoin <- collect(inner_join(pitchesDF, atbatsDF))

        print("R program running: applying propriatory scoring methods")

        # score Qual and Quant mutate
        joined <- pitchesJoin %>% mutate(quant_score_des = get_quant_score(des),
                                        fix_quant_score = fix_quant_score(event) * (des == 'In play, run(s)'),
                                        quant_score = quant_score_des + fix_quant_score,
                                        qual_score = get_qual_score(atbat_des) * (type == 'X'),
                                        hitter_val = quant_score + qual_score)

        print("R program running: pre-processing data for machine learning")

        # convert to factor variables
        joined$pitch_type <- as.factor(joined$pitch_type) 
        joined$des <- as.factor(joined$des) 
        joined$type <- as.factor(joined$type)
        joined$count <- as.factor(joined$count) 
        joined$event <- as.factor(joined$event) 
        joined$p_throws <- as.factor(joined$p_throws)
        joined$zone <- as.factor(joined$zone)
        joined$stand <- as.factor(joined$stand)
        joined$inning <- as.factor(joined$inning)
        joined$inning_side <- as.factor(joined$inning_side)

        # convert FS and FT to SInkers 
        levels(joined$pitch_type)[levels(joined$pitch_type)=="FS"] <- "SI"
        levels(joined$pitch_type)[levels(joined$pitch_type)=="FT"] <- "SI"
        levels(joined$pitch_type)[levels(joined$pitch_type)=="FC"] <- "SL"
        levels(joined$pitch_type)[levels(joined$pitch_type)=="KC"] <- "KN"

        # Decide Good (1) or Bad (0)
        joined.classic <- joined %>% mutate(hv_binary = ifelse(hitter_val < 0, 1, 0))

        #create zone and pitch type pairs
        joined.classic <- joined.classic %>% mutate(ptz=paste(pitch_type,zone, sep = "_"))

        #remove infrequent pitch types
        joined.classic.pitchedit <- joined.classic %>% filter(pitch_type != c('EP','FO','PO','SC'))

        #view missing data
        #visna(joined.classic, tp = TRUE, col = "blue")

        #create subsets of pitcher stance and batter stance 
        #Rh <- joined.classic %>% filter(stand == "R")
        #Lh <- joined.classic %>% filter(stand == "L")

        #Rpitch <- joined.classic %>% filter(p_throws == "R")
        #Lpitch <- joined.classic %>% filter(p_throws =="L")

        #RhRp <- Rh %>% filter(p_throws == "R")
        #RhLp <- Rh %>% filter(p_throws == "L")
        #LhRp <- Lh %>% filter(p_throws == "R")
        #LhLp <- Lh %>% filter(p_throws == "L")

        #Primary Component Plots... need to update table names per above convention
        #PCP <- ggparcoord(data = joined_classic[order(joined_classic$hv_binary, decreasing = FALSE),], columns = c(40,46,30,16,17,32,88), groupColumn = "hv_binary", title = "Factors v Pitcher Outcome", alpha = .01) PCP_cat <- ggparcoord(data = joined.temp[order(joined.temp$GoodBadQual, decreasing = TRUE),], columns = c(40,46,30,32,88), groupColumn = "GoodBadQual", title = "Categorical Factors v Pitcher Outcome")
        #RpRh_pcp <- ggparcoord(data = RhRp[order(RhRp$hv_binary, decreasing = FALSE),], columns = c(8,9,11,14,27,28), groupColumn = "hv_binary", title = "RpRh PCP v Pitcher Outcome")
        #RpLh_pcp <- ggparcoord(data = RpitchLh[order(RpitchLh$GoodBadQual, decreasing = TRUE),], columns = c(16,17,30,32,40,46,88), groupColumn = "GoodBadQual", title = "RpLh PCP v Pitcher Outcome")
        #LpRh_pcp <- ggparcoord(data = LpitchRh[order(LpitchRh$GoodBadQual, decreasing = TRUE),], columns = c(16,17,30,32,40,46,88), groupColumn = "GoodBadQual", title = "LpRh PCP v Pitcher Outcome")
        #LpLh_pcp <- ggparcoord(data = LpitchLh[order(LpitchLh$GoodBadQual, decreasing = TRUE),], columns = c(16,17,30,32,40,46,88), groupColumn = "GoodBadQual", title = "LpLh PCP v Pitcher Outcome")

        #create data for Swinging Strikes outside the strike zone. 
        #SS_NonSZ_Rh <- Rpitch %>% filter (des == "Swinging Strike" & zone == c(11,12,13,14))
        #SS_NonSZ_Lh <- Lpitch %>% filter (des == "Swinging Strike" & zone == c(11,12,13,14))

        #export features of interest, with hv_binary label 1 if <0, else 0
        var.interest <- joined.classic.pitchedit %>% select(3,5,6,8:13,16,18,22,27:29)

        print("R program running: storing results in database")

        DBI::dbWriteTable(my_mlb_db, "rawdata_joined", joined.classic.pitchedit, append = TRUE)
        # DBI::dbWriteTable(my_mlb_db, "rawdata_ML", var.interest, append = TRUE)

        # Specify any database table adjustments on first creation of long term tables 
        if (db_table_creation) {
            dbGetQuery(my_mlb_db, "ALTER TABLE `rawdata_joined` CHANGE `date` `date` DATE NULL DEFAULT NULL")
            # dbGetQuery(my_mlb_db, "ALTER TABLE `rawdata_ML` CHANGE `date` `date` DATE NULL DEFAULT NULL")
            
            # Tables have been created and adjustments made - adjustments will not need to be made again
            db_table_creation <<- FALSE
        }

        # Close open database connections
        # dbDisconnect for DBI connection
        dbDisconnect(my_mlb_db)
        # RM for DPLYR connection
        rm(my_scrape_db)

        # we have all the MLB data we want for this ingest period.  Clean up / wipe the scrape databse - so we have a clean next run

        dates_success <<- append(dates_success, as.Date(day, format="%m-%d-%y"))

        drop_scrape_tables() 
        
    } else {
        
        # close the connection that checked if data was in the scrape database
        dbDisconnect(my_scrape_db_dbi)
        # close this connection also
        dbDisconnect(my_mlb_db)

        print(paste0("R program running: Unable to scrape, process & load data for: ", format(day,"%m-%d-%y")))

        # something went wrong with the scrape for this day.  Clean up / wipe the scrape databse - so we have a clean next run
        # keeping track of failed dates for summary at end
        dates_failed <<- append(dates_failed, as.Date(day, format="%m-%d-%y"))

        print("R program running: aborting load for this day")
        drop_scrape_tables()

    }

}

## End of Functions

## MAIN LOOP
print("R program running")

## Define Season Dates
season_start_2017 <- as.Date("04-02-17",format="%m-%d-%y")
season_start_2018 <- as.Date("03-29-18",format="%m-%d-%y")
season_start_2019 <- as.Date("03-20-19",format="%m-%d-%y")
season_start_2020 <- as.Date("03-20-20",format="%m-%d-%y")
season_end_2017 <- as.Date("11-01-17",format="%m-%d-%y")
season_end_2018 <- as.Date("09-30-18",format="%m-%d-%y")
season_end_2019 <- as.Date("09-30-19",format="%m-%d-%y")
season_end_2020 <- as.Date("09-30-20",format="%m-%d-%y")

## setup to pull all historical data beginning with big_start date
big_start <- season_start_2017

# connect to database
my_mlb_db <- DBI::dbConnect(RMySQL::MySQL(), 
                    host = Sys.getenv("mlb_db_hostname"),
                    dbname = Sys.getenv("mlb_db_dbname"),
                    user = Sys.getenv("mlb_db_username"),
                    password = Sys.getenv("mlb_db_password")
)

## Is there a database table rawdata_joined?
if (dbExistsTable(my_mlb_db, "rawdata_joined")) {
  print ("rawdata_joined exists")
  
  # database table exists
  db_table_creation <<- FALSE

  last_date_stored <- dbGetQuery(my_mlb_db, "SELECT MAX(date) AS \"Max Date\" FROM rawdata_joined")
  start <- as.Date(str_replace_all(last_date_stored, "_", "-")) + 1
  
} else {
  # database table doesn't exist!
  start <- big_start
  # global variable set when long term database tables are created for the first time
  db_table_creation <<- TRUE
}

## pull data from start till yesterday
today <- Sys.Date()

## create an empty vector to keep track of days where scape fails
dates_failed <- integer(0)
class(dates_failed) <- "Date"

## create an empty vector to keep track of days where scape succeeds
dates_success <- integer(0)
class(dates_success) <- "Date"

# ensure there isn't any stale data in the temp scrape databse
drop_scrape_tables()

while (start < today) {
    # Update start date.... 
    # Jump the date forward if before season starts
    # Jump the date fworard if after season ends
    start_date_year <- as.numeric(format(start,"%Y"))
    if (start_date_year == 2017) {
        if (start < season_start_2017) {
            start = season_start_2017
        } else if (start > season_end_2017) {
            start = season_start_2018
            next
        }
    } else if (start_date_year == 2018) {
        if (start < season_start_2018) {
            start = season_start_2018
        } else if (start > season_end_2018) {
            # Jump the date fworard to next season start date
            # re-evaluate date
            start = season_start_2019
            next
        }
    } else if (start_date_year == 2019) {
        if (start < season_start_2019) {
            start = season_start_2019
        } else if (start > season_end_2019) {
            start = season_start_2020
            next
        }
    }

    # Run DataFresh
    datafresh(start)

    # Jump the date forward to the next day
    start = start + 1

}

dbDisconnect(my_mlb_db)

print(paste0("R program complete: DataFresh successfully scraped and loaded: ", length(dates_success), " days"))
print(paste0("R program complete: DataFresh failed to scrape and load: ", length(dates_failed), " days"))