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

setwd("/db")

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
  message(sprintf("Split Season Start Date: %s", previousSeasonStartdate))
  message(sprintf("Split Season End Date: %s", PreviousSeasonEnddate))
  message(sprintf("Split Season days: %s", PreviousSeasonEnddate - previousSeasonStartdate))
}

dbSendQuery(my_dbProd$con, "CREATE INDEX url_atbat ON atbat(url)") 
dbSendQuery(my_dbProd$con, "CREATE INDEX url_pitch ON pitch(url)")
dbSendQuery(my_dbProd$con, "CREATE INDEX pitcher_index ON atbat(pitcher_name)")
dbSendQuery(my_dbProd$con, "CREATE INDEX des_index ON pitch(des)")

### Here is where Machine Learning Data Munging begins.
pitch16 <- select(tbl(my_dbProd, "pitch"), gameday_link, num, des, type, tfs, tfs_zulu, id, end_speed, pitch_type, count, zone)
atbat16 <- select(tbl(my_dbProd, "atbat"), gameday_link, num, pitcher, batter, b_height, pitcher_name, p_throws, batter_name, stand, atbat_des, event, inning, inning_side)

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
# join filtered atbats to all pitches
pitchesJoin <- collect(inner_join(pitch16, atbat16))

# score Qual and Quant mutate
joined <- pitchesJoin %>% mutate(quant_score_des = get_quant_score(des),
                                 fix_quant_score = fix_quant_score(event) * (des == 'In play, run(s)'),
                                 quant_score = quant_score_des + fix_quant_score,
                                 qual_score = get_qual_score(atbat_des) * (type == 'X'),
                                 hitter_val = quant_score + qual_score)

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
Rh <- joined.classic %>% filter(stand == "R")
Lh <- joined.classic %>% filter(stand == "L")

Rpitch <- joined.classic %>% filter(p_throws == "R")
Lpitch <- joined.classic %>% filter(p_throws =="L")

RhRp <- Rh %>% filter(p_throws == "R")
RhLp <- Rh %>% filter(p_throws == "L")
LhRp <- Lh %>% filter(p_throws == "R")
LhLp <- Lh %>% filter(p_throws == "L")

#Primary Component Plots... need to update table names per above convention
#PCP <- ggparcoord(data = joined_classic[order(joined_classic$hv_binary, decreasing = FALSE),], columns = c(40,46,30,16,17,32,88), groupColumn = "hv_binary", title = "Factors v Pitcher Outcome", alpha = .01) PCP_cat <- ggparcoord(data = joined.temp[order(joined.temp$GoodBadQual, decreasing = TRUE),], columns = c(40,46,30,32,88), groupColumn = "GoodBadQual", title = "Categorical Factors v Pitcher Outcome")
RpRh_pcp <- ggparcoord(data = RhRp[order(RhRp$hv_binary, decreasing = FALSE),], columns = c(8,9,11,14,27,28), groupColumn = "hv_binary", title = "RpRh PCP v Pitcher Outcome")
#RpLh_pcp <- ggparcoord(data = RpitchLh[order(RpitchLh$GoodBadQual, decreasing = TRUE),], columns = c(16,17,30,32,40,46,88), groupColumn = "GoodBadQual", title = "RpLh PCP v Pitcher Outcome")
#LpRh_pcp <- ggparcoord(data = LpitchRh[order(LpitchRh$GoodBadQual, decreasing = TRUE),], columns = c(16,17,30,32,40,46,88), groupColumn = "GoodBadQual", title = "LpRh PCP v Pitcher Outcome")
#LpLh_pcp <- ggparcoord(data = LpitchLh[order(LpitchLh$GoodBadQual, decreasing = TRUE),], columns = c(16,17,30,32,40,46,88), groupColumn = "GoodBadQual", title = "LpLh PCP v Pitcher Outcome")

#create data for Swinging Strikes outside the strike zone. 
SS_NonSZ_Rh <- Rpitch %>% filter (des == "Swinging Strike" & zone == c(11,12,13,14))
SS_NonSZ_Lh <- Lpitch %>% filter (des == "Swinging Strike" & zone == c(11,12,13,14))

#export features of interest, with hv_binary label 1 if <0, else 0
var.interest <- joined.classic.pitchedit %>% select(3,5,6,8:13,16,18,22,27:28,31)
write.csv(var.interest, file = paste(format(Sys.Date(), "HVal-%Y-%m-%d"), "csv", sep = "."))

