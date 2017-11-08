install.packages("RSQLite")
install.packages("dplyr")
install.packages("dbplyr")
install.packages("ggplot2")
install.packages("pitchRx")

library(RSQLite)
library(dplyr)
library(dbplyr)
library(ggplot2)
library(pitchRx)

setwd("/root/db")

filename <- "pitchRxProd.sqlite3"
if (file.exists(filename)) file.remove(filename)

my_dbProd <- src_sqlite(filename, create = TRUE)

Today <- Sys.Date()
TodayYearDay = as.POSIXlt(c(Today))$yday  #Year Day for Today
#Today <- as.Date("2018-06-10")            # for testing
#TodayYearDay = as.POSIXlt(c(Today))$yday  # for testing
SeasonStartYearDay = as.POSIXlt(c(as.Date("2018-03-29")))$yday
SeasonEndYearDay = as.POSIXlt(c(as.Date("2018-09-30")))$yday
PostSeasonEndYearDay = as.POSIXlt(c(as.Date("2018-10-31")))$yday

#pick startdate and end date
#March
if (TodayYearDay <= PostSeasonEndYearDay) {  # is it before the end of the Post season
  if (TodayYearDay > SeasonEndYearDay) { # well, it Is Post Season
    print("It is Post Season - grab 90 days of data and Post Season data")
    startdate = Today - 90 - (TodayYearDay - SeasonEndYearDay) - 1
    enddate = Today
    splitSeasonsPreviousDays = 0
  } else if (TodayYearDay >= SeasonStartYearDay + 90 ) { # Regular Season
    print("It is Mid Season - grab 90 days of data")
    startdate = Today - 90
    enddate = Today
    splitSeasonsPreviousDays = 0
  } else if (TodayYearDay <= SeasonStartYearDay) {       # Before Regular Season
    print("Use Last Years data")
    enddate = Today - TodayYearDay - (365 - PostSeasonEndYearDay)
    startdate = Today - TodayYearDay - (365 - PostSeasonEndYearDay) - 122
    splitSeasonsPreviousDays = 0
  } else {
    print("It is Mid Season, but it is too early to get a full 90 days this year")
    startdate = Today - (TodayYearDay - SeasonStartYearDay)
    enddate = Today
    splitSeasonsPreviousDays = 120 - (TodayYearDay - SeasonStartYearDay) 
  }
  
} else  {         # it is After Post Season Or Later at the end of the year
  print("It is After post season toward the end of the year")
  startdate = Today - (TodayYearDay - PostSeasonEndYearDay) - 122
  enddate = Today - (TodayYearDay - PostSeasonEndYearDay)
  splitSeasonsPreviousDays = 0
} 

message(sprintf("Today: %s", Today))
message(sprintf("Start Date: %s", startdate))
message(sprintf("End Date: %s", enddate))
message(sprintf("currnet season days: %s", enddate - startdate))
message(sprintf("Split season previous season days: %s\n", splitSeasonsPreviousDays))

#Historical Method
#startdate <- Today - 90

#Pull Current Season Days
scrape(start = startdate, enddate, suffix = "inning/inning_all.xml", connect = my_dbProd$con)
#Pull any Split Season Days
if (splitSeasonsPreviousDays > 0) {
  previousSeasonStartdate = Today - TodayYearDay - (365 - PostSeasonEndYearDay) - splitSeasonsPreviousDays
  PreviousSeasonEnddate = Today - TodayYearDay - (365 - PostSeasonEndYearDay)
  scrape(start = startdate, enddate, suffix = "inning/inning_all.xml", connect = my_dbProd$con)
}

message(sprintf("Split Season Start Date: %s", previousSeasonStartdate))
message(sprintf("Split Season End Date: %s", PreviousSeasonEnddate))
message(sprintf("Split Season days: %s", PreviousSeasonEnddate - previousSeasonStartdate))


dbSendQuery(my_dbProd$con, "CREATE INDEX url_atbat ON atbat(url)") 
dbSendQuery(my_dbProd$con, "CREATE INDEX url_pitch ON pitch(url)")
dbSendQuery(my_dbProd$con, "CREATE INDEX pitcher_index ON atbat(pitcher_name)")
dbSendQuery(my_dbProd$con, "CREATE INDEX des_index ON pitch(des)")
