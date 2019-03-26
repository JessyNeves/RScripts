# LINHAS DE INSTALAÇÃO

#install.packages("RODBC")
#install.packages("lubridate")
#install.packages("rgl")
#install.packages("mgcv")
#install.packages("dplyr")
#install.packages("scatterplot3d")
#install.packages("rlist")
#install.packages("shiny")
#install.packages("shinythemes")
#install.packages("ggplot2")

# SCRIPT 

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
    library(stringr)
    library(spatialEco)


    # Connection to DB
    con <- 'Driver={SQL Server};Server=edpsighprddb1.cpdprd.pt;Database=SIGH-AC-PRD;Trusted_Connection=yes'
    channel <- odbcDriverConnect(con)
    # Menu ; Which system? When? All Transactions? 
    one <- "p05"
    two <- "p15"
    three <- "p16"
    four <- "p25"
    five <- "mlp"
    todas <- "Trans NOT IN('SESSION_MANAGER', '')"
    criticas <- "Trans IN('ZPX_CONSULTA_SE16','ZPO_GEST_TB', 'ZPF_VAL_CRED_SDD', 'ZLF_AJUSTA_B2C', 'SE16', 'FPE2M', 'FP40', 'FP04', 'VA01','EA62', 'FPP2', 'CAA2')"
    custom <- "TOBEREPLACED"
    print("Insira o número correspondente ao sistema a analisar: 1, 2, 3, 4 ou 5")
    # Menu Options + Stored Result 
    system <- switch(menu(c("P05", "P15", "P16", "P25", "MLP")) + 1, cat("Nothing done\n"), one, two, three, four, five)
    print("Seleção/Inserção das Transações:")
    transactions <- switch(menu(c("Todas as transações", "Apenas transações críticas", "Inserir Manualmente")) + 1, cat("Nothing done\n"), todas, criticas, custom)
    if (transactions == custom) {
        custom <- readline(prompt = "Insira as transações desejadas separadas por vírgula e entre plicas. Ex: 'ZPX', 'SE99', 'FP14'")
        custom <- paste("TRANS IN(", custom, sep = "")
        custom <- paste(custom, ")", sep = "")
    }
    # Reads Date
    bigBang <- readline(prompt = "Insira a data de inicio. Formato: 20191001 (2019/10/01 - Ano / Mês / Dia ): ")
    # Reads Date
    bigCrunch <- readline(prompt = "Insira a data de fim. Formato: 20191001 (2019/10/01 - Ano / Mês / Dia ): ")
    # Unfinished Query. Lacks system ; First Day Date ; Last Day Date
    query <- "SELECT timestamp=Datediff ( second, '2018-12-01 00:00:00',SUBSTRING(DataLog, 1, 4) + '-' + SUBSTRING(DataLog, 5, 2) + '-' + SUBSTRING(DataLog, 7,2) + ' ' + SUBSTRING(HoraLog, 1, 2) + ':' + SUBSTRING(HoraLog, 3, 2) + ':' + SUBSTRING(HoraLog, 5,2)),
    UserID, DataLog, HoraLog FROM edp.sal_INSERT_SYSTEM_HERE
    WHERE UserID LIKE 'E%' AND UserID NOT LIKE 'ESA%' AND DataLog >= 'INSERT_FIRST_DAY_HERE' AND DataLog <= 'INSERT_LAST_DAY_HERE' AND WHICH_TRANSACTIONS
    ORDER BY UserID, DataLog, HoraLog"

    # Replace System to Analyze 
    query <- gsub("INSERT_SYSTEM_HERE", system, query)
    # Replace First Day Date 
    query <- gsub("INSERT_FIRST_DAY_HERE", bigBang, query)
    # Replace Last Day Date 
    query <- gsub("INSERT_LAST_DAY_HERE", bigCrunch, query)
    # Replace Trnsactions
    query <- gsub("WHICH_TRANSACTIONS", transactions, query)
    # Verify Dates (If end > beginning. If interval isn't too big)

    if (!(bigCrunch >= bigBang))
        stop("Data Inicial é posterior à Data Final.")

    # Verify Dates (if interval isn't too big)
    if (abs(as.numeric(bigBang) - as.numeric(bigCrunch)) > 230) {
        print("AVISO: O intervalo introduzido pode ser demasiado grande. Prosseguir?")
        system <- switch(menu(c("Sim", "Não")) + 1,
       cat("Nothing done\n"), "Sim", stop("Parado por ordem do utilizador"))
    }

    #Starts counting
    start.time <- Sys.time()
    # Query DB
    print("Querying DB. Aguarde...")
    result <- sqlQuery(channel, query)
    if (grepl('ERROR', result[[2]]))
        stop("Couldn't Query DB. Possibly can't interpret Query")
    print("Querying Over. Pré-Processamento da informação...")

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
    weekdays <- weekdays(data, abbreviate = TRUE)


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

    thetime <- paste(data, hora, sep = " ")
    thetime <- insert.values(thetime, "2018-01-01 00:00:00", 1)
    user <- insert.values(user, "INVALIDO", 1)
    trans <- insert.values(trans, "INVALIDO", 1)
    data <- insert.values(data, "INVALIDO", 1)
    weekdays <- insert.values(weekdays, "INVALIDO", 1)
    thetime <- as.POSIXct(thetime)
    dataset <- data.frame(user, thetime, trans, data, weekdays)
    dataset <- dataset[dataset$weekdays != "sßb",]
    dataset <- dataset[dataset$weekdays != "dom",]

    #Detect Scheduled Alarm 
    scheduledAlarm <- tapply(dataset$thetime, dataset$user, function(x) {
        x <- unique(x)
        dating <- data.frame(table(cut(x, breaks = "30 sec")))
        dating <- data.frame(dating, weekdays(as.Date(dating$Var1)))
        MyDatesDF <- data.frame(dating, grp = 1)
        print(log2(1 - length(MyDatesDF[MyDatesDF$Freq > 0,]$Var1) / length(MyDatesDF$Var1)))
        MyDatesDF <- MyDatesDF[MyDatesDF$Freq > 0,]
        teste <- str_sub(MyDatesDF$Var1, 12, 19)
        exp <- (table(teste))
        return(max(exp))
    })

    numberOfDays<- tapply(dataset$data, dataset$user, function(x) {
        x <- unique(x)
        return(length(x))
    })

    size <- tapply(dataset$data, dataset$user, function(x) {
        return(length(x))
    })
                                  
    results <- data.frame(numberOfDays, size, scheduledAlarm, numberOfDays - scheduledAlarm)
    # If scheduled = 1, high entropy. transactions distributed through time. no scheduling
    results <- results[results$scheduledAlarm > 2,]


