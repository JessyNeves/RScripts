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
result <- sqlQuery(channel, "SELECT UserID, DataLog, HoraLog, Trans FROM edp.sal_MLP
WHERE UserID = 'EX82461' AND DataLog > '20190101' AND TpReg IN('AUW', 'AUY', 'AU3') AND Trans NOT IN('')
ORDER BY UserID, DataLog, HoraLog")
close(channel)
rm(channel, con)
#Querying Over
trans <- gsub(' ', '', result$Trans)
data <- result$DataLog
dataset <- data.frame(trans, data)

start.time <- Sys.time()

taskLength <<- 2
finalArray <- vector()

while (taskLength < 9) { 

patterns <- tapply(dataset$trans, dataset$data, function(x) {
    if (length(x) <= taskLength) {
        return(0)
    }
    x <- paste(x, collapse = "-")
    sequence <- seqdef(x)
    y <- cprob(sequence, taskLength, prob = TRUE)
    return(y)
})

gc()

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
    finalArray <- append(finalArray, length(matches[matches == 1 ]))
    taskLength <- taskLength + 1
    rm(matches, patterns)
}

end.time <- Sys.time()
time.taken <- end.time - start.time
print(time.taken)

print(finalArray)
print(length(unique(data)))
#trans <- gsub("-", "", tras, fixed = TRUE)
#pos = gregexpr('121231', trans) # Returns positions of every match in a string
