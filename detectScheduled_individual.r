###############################################################################################################
library(RODBC)
library(lubridate, warn.conflicts = FALSE)
require(graphics)
require(rgl)
require(mgcv)
library(dplyr)
library("scatterplot3d") # load
library(rlist)
library(ggplot2)
library(spatialEco)
library(bupaR)
library(stringr)
# Connection to DB
con <- ''
channel <- odbcDriverConnect(con)

result <- sqlQuery(channel, "SELECT UserID, DataLog, HoraLog, Trans FROM *
WHERE UserID = 'EX82461' AND DataLog >= '20190301' AND TpReg IN('AUW', 'AUY', 'AU3') AND Trans NOT IN('SESSION_MANAGER', '')
ORDER BY UserID, DataLog, HoraLog")

# unixTime represents second 0, origin of time.
data <- result$DataLog
hora <- result$HoraLog
# Store and remove garbage
user <- gsub(" ", "", result$UserID)
trans <- gsub(" ", "", result$Trans)
# Convert data format to use native functions.
data <- lapply(data, function(x) {
    x = sub("([[:digit:]]{4,4})$", " \\1", x)
    x = sub("([[:digit:]]{2,2})$", " \\1", x)
})
data <- unlist(data)
data <- as.Date(data, "%Y%m%d")

# Convert hour format to use native functions.
hora <- substr(hora, 1, nchar(hora) - 2)
hora <- sapply(hora, function(x) {
    if (nchar(x) == 0) 
        x <- paste('00:00:00', x, sep = '')
    if (nchar(x) < 2)
        x = paste('00:00:0', x, sep = '')
    if (nchar(x) < 3)
        x = paste('00:00', x, sep = '')
    if (nchar(x) < 4)
        x = paste('00:0', x, sep = '')
    if (nchar(x) < 5)
        x = paste('00', x, sep = '')
    x = sub("([[:digit:]]{4,4})$", ":\\1", x)
    x = sub("([[:digit:]]{2,2})$", ":\\1", x)
})
hora <- gsub("::", ":", hora)
hora2 <- substr(hora, 1, nchar(hora) - 6)
hist(as.numeric(hora2))

thetime <- paste(data, hora, sep = " ")
thetime <- insert.values(thetime, "2018-01-01 00:00:00", 1)
thetime <- unique(thetime)
user <- insert.values(user, "INVALIDO", 1)
trans <- insert.values(trans, "INVALIDO", 1)
data <- insert.values(data, "INVALIDO", 1)
thetime <- as.POSIXct(thetime)

dating <- data.frame(table(cut(thetime, breaks = "2 min")))
dating <- unique(dating)
dating <- data.frame(dating, weekdays(as.Date(dating$Var1)))
MyDatesDF <- data.frame(dating, grp = 1)
1-length(MyDatesDF[MyDatesDF$Freq > 0,]$Var1)/length(MyDatesDF$Var1)
MyDatesDF <- MyDatesDF[MyDatesDF$Freq > 0,]

teste <- str_sub(MyDatesDF$Var1, 12, 19)
exp <- (table(teste))
