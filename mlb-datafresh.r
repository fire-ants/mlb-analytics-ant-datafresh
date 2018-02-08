library("RPostgreSQL")
library("DBI")
library("methods")

hello <- function( name ) {
  sprintf( "Hi, %s", name );
}
print("                   ")
print("                   ")
print("                   ")
print("                   ")

hello('Jason')

# create a connection
# save the password that we can "hide" it as best as we can by collapsing it
pw <- {
  "mDaQM6rrkRs2dJU4jEIuiywaywI3Db_f"
}

# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "kcbquxyk",
                 host = "baasu.db.elephantsql.com", port = 5432,
                 user = "kcbquxyk", password = pw)
rm(pw) # removes the password

#print(con)

# check for the cartable
#dbExistsTable(con, "cartable")
print("checking for database")

databases = dbGetQuery(con, "SELECT datname FROM pg_database WHERE datistemplate = FALSE")
# TRUE
print(databases)