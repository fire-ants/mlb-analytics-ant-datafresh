## load libraries


## Use dplyer to create SQLite database
library(dplyr)
library(dbConnect)
#my_db2016 <- src_sqlite("pitchRx2016.sqlite3", create = TRUE)
my_dbProd <- src_sqlite("pitchRxProd.sqlite3", create = TRUE)

Today <- Sys.Date()
ThirtyDaysAgo <- Today - 30

#confirm empty
#my_db2016
my_dbProd


## scrape 2016 game data and store in the database
#library(pitchRx)
#scrape(start = "2016-04-03", end = "2016-11-02", suffix = "inning/inning_all.xml", connect = my_db1$con)
#scrape(start = "2016-04-01", end = "2016-10-31", suffix = "inning/inning_all.xml", connect = my_db2016$con)
scrape(start = ThirtyDaysAgo, end = Today, suffix = "inning/inning_all.xml", connect = my_dbProd$con)


# To speed up execution time, create an index on these three fields.

dbSendQuery(my_dbProd$con, "CREATE INDEX url_atbat ON atbat(url)")
dbSendQuery(my_dbProd$con, "CREATE INDEX url_pitch ON pitch(url)")
dbSendQuery(my_dbProd$con, "CREATE INDEX pitcher_index ON atbat(pitcher_name)")
dbSendQuery(my_dbProd$con, "CREATE INDEX des_index ON pitch(des)")
