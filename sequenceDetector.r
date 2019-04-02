###############################################################################################################
library(RODBC)
library(dplyr, warn.conflicts = FALSE)
library(tibbletime)
library(Matrix)
require(graphics)
require(rgl)
require(mgcv)
library(PST)
library(arules)
library(RODBC)
library(lubridate, warn.conflicts = FALSE)
require(graphics)
require(rgl)
require(mgcv)
library(dplyr)
library("scatterplot3d")
library(rlist)
library(shiny)
library(shinythemes)
library(ggplot2)
library(DescTools)

#EX82442 _ MLP
#EX82461 _ MLP
#EX82995 _ P05

# Connection to DB
con <- 'Driver={SQL Server};Server=edpsighprddb1.cpdprd.pt;Database=SIGH-AC-PRD;Trusted_Connection=yes'
channel <- odbcDriverConnect(con)
#Querying DB
result <- sqlQuery(channel, "SELECT UserID, DataLog, HoraLog, Trans FROM edp.sal_P16
WHERE DataLog >= '20190320' AND TpReg IN('AUW', 'AUY', 'AU3') AND Trans NOT IN('') AND UserID LIKE 'E%'
ORDER BY UserID, DataLog, HoraLog")
close(channel)
rm(channel, con)
#Querying Over
trans <- gsub(' ', '', result$Trans)
data <- result$DataLog
user <- gsub(" ", "", result$UserID)
dataset <- data.frame(trans, data, user)


start.time <- Sys.time()
taskLength <<- 2
finalArray <- vector()

# Increment sequence size L 

while (taskLength <= 15) {

patterns <- tapply(dataset$trans, dataset$user, function(x) {
    if (length(unique(x)) == 1) {
        return(0)
    }
    if (length(x) <= taskLength) {
        return(0)                         
    }
    if (length(x) < 100) {
        return(0)
    }
    x <- paste(x, collapse = "-")
    sequence <- seqdef(x)
    y <- cprob(sequence, taskLength, prob = TRUE)
    return(y)
})

matches <- sapply(patterns, function(x) {
    if (x != 0) { 
        n <- x[, dim(x)[2]]
        dif <- diff(n)
        if (length(dif[dif == 0]) == length(dif)) {
            return(1)
        } else {
            return(0)
        }
    } else {
        return(-1)
    }
})

#finalArray <- append(finalArray, length(matches[matches == 1 ]))
finalArray <- append(finalArray, matches)
taskLength <- taskLength + 1
#rm(matches, patterns)


end.time <- Sys.time()
time.taken <- end.time - start.time
print(time.taken)
}

#trans <- gsub("-", "", tras, fixed = TRUE)
#pos = gregexpr('121231', trans) # Returns positions of every match in a string

    