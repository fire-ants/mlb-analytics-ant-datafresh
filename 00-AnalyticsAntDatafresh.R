install.packages("RSQLite")
install.packages("dplyr")
install.packages("ggplot2")
install.packages("pitchRx")

library(RSQLite)
library(dplyr)
library(ggplot2)
library(pitchRx)

setwd("/db")

filename <- "pitchRxProd.sqlite3"
if (file.exists(filename)) file.remove(filename)

my_dbProd <- src_sqlite(filename, create = TRUE)

Today <- Sys.Date()
startdate <- Today - 90
scrape(start = startdate, end = Today, suffix = "inning/inning_all.xml", connect = my_dbProd$con)

dbSendQuery(my_dbProd$con, "CREATE INDEX url_atbat ON atbat(url)") 
dbSendQuery(my_dbProd$con, "CREATE INDEX url_pitch ON pitch(url)")
dbSendQuery(my_dbProd$con, "CREATE INDEX pitcher_index ON atbat(pitcher_name)")
dbSendQuery(my_dbProd$con, "CREATE INDEX des_index ON pitch(des)")