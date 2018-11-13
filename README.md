# mlb-datagrabber-microservice
Microservice for extracting data

This repo utilizes 2 programs for 2 different environments

**00-AnalyticsAntDatafresh.R** is the master control program from initiating the docker container image. See Dockerfile.

**00-mlb-datafresh.R** is the program that updates MLB data on Pivotal Application Services and stores it in the PostgresSQL db.
