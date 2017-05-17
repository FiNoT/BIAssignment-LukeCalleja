# Luke Calleja IT-SWD6.2A
# IICT6011 - Business Intelligence and Reporting
# Assignment

#Setting the working directory
#Session > Set Working Directory > Choose Directory > Select the required folder
#setwd("F:/MCAST/BSC Second Year/Business Intelligence & Reporting/Assignment/BIAssignment")

#Importing Datasets
#Go to top right pane > Import Dataset > Import CSV > select the csv file in folder
#city <- read_csv("F:/MCAST/BSC Second Year/Business Intelligence & Reporting/Assignment/csvs/city.csv")
#client <- read_csv("F:/MCAST/BSC Second Year/Business Intelligence & Reporting/Assignment/csvs/client.csv")
#order <- read_csv("F:/MCAST/BSC Second Year/Business Intelligence & Reporting/Assignment/csvs/order.csv")
#region <- read_csv("F:/MCAST/BSC Second Year/Business Intelligence & Reporting/Assignment/csvs/region.csv")

#Cleaning of Data
#Region cleaning
region$countryId <- NULL

#City Cleaning
city$cityName <- NULL

#Order Cleaning
order$storeId <- NULL

#Client Cleaning
client$firstName <- NULL
client$lastName <- NULL
client$gender <- NULL
client$dateOfBirth <- NULL
client$maritialStatus <- NULL
client$postCode <- NULL
client$occupationId <- NULL
client$levelId <- NULL
client$incomeId <- NULL
client$carCount <- NULL
client$childCount <- NULL
client$childAtHomeCount <- NULL
client$isHomeOwner <- NULL
client$accountDate <- NULL
client$membershipLevelId <- NULL