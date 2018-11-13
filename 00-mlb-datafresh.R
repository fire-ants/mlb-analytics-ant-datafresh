library(pitchRx)
library(dplyr)
library(dbplyr)

cat("R program running")

names(s <- Sys.getenv())
#print(Sys.getenv("mlb_db_hostname"))

my_db <- src_mysql(dbname = Sys.getenv("mlb_db_dbname"), host=Sys.getenv("mlb_db_hostname"), port = 3306, user = Sys.getenv("mlb_db_username"), password = Sys.getenv("mlb_db_password")) 
scrape(start = "2018-04-02", end = "2018-04-08", suffix = "inning/inning_all.xml", connect = my_db$con)

### Load pitch and atbat data frames
pitchesDF <- select(tbl(my_db, "pitch"), gameday_link, num, des, type, tfs, tfs_zulu, id, end_speed, pitch_type, count, zone)
atbatsDF <- select(tbl(my_db, "atbat"), gameday_link, num, pitcher, batter, b_height, pitcher_name, p_throws, batter_name, stand, atbat_des, event, inning, inning_side)

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
pitchesJoin <- collect(inner_join(pitchesDF, atbatsDF))

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
#write.csv(var.interest, file = paste(format(Sys.Date(), "HVal-%Y-%m-%d"), "csv", sep = "."))
#write.csv(var.interest, file = "rawdata_ML.csv")

dbWriteTable(my_db, "rawdata_ML", value = var.interest, overwrite = TRUE)




#sql_command <- paste("UPDATE pg_settings SET setting =", Sys.Date(), "WHERE name = 'latest_date'")
#dbGetQuery(con, sql_command)

#atbat_tbl <- tbl(my_db, "atbat")
#pitch_tbl <- tbl(my_db, "pitch")

#print(nrow(atbat_tbl))