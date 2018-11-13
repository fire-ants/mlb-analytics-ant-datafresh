library(pitchRx)
library(dplyr)
library(dbplyr)

cat("R program running")

names(s <- Sys.getenv())
#print(Sys.getenv("mlb_db_hostname"))

my_db <- src_postgres(dbname = Sys.getenv("mlb_db_dbname"), host=Sys.getenv("mlb_db_hostname"), port = 5432, user = Sys.getenv("mlb_db_username"), password = Sys.getenv("mlb_db_password")) 
#scrape(start = "2018-04-02", end = "2018-04-04", suffix = "inning/inning_all.xml", connect = my_db$con)

#sql_command <- paste("UPDATE pg_settings SET setting =", Sys.Date(), "WHERE name = 'latest_date'")
#dbGetQuery(con, sql_command)

#atbat_tbl <- tbl(my_db, "atbat")
#pitch_tbl <- tbl(my_db, "pitch")

#print(nrow(atbat_tbl))