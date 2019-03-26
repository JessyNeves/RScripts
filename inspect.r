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
#install.packages("DescTools")

# SCRIPT 
{

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

    # Connection to DB
    con <- 'Driver={SQL Server};Server=edpsighprddb1.cpdprd.pt;Database=SIGH-AC-PRD;Trusted_Connection=yes'
    channel <- odbcDriverConnect(con)
    # Menu ; Which system? When? All Transactions? 
    one <- "p05"
    two <- "p15"
    three <- "p16"
    four <- "p25"
    five <- "mlp"
    todas <- "Trans NOT IN('')"
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
    UserID, DataLog, HoraLog, Trans, Data, Programa, TpReg, Terminal, NomeProcSAP FROM edp.sal_INSERT_SYSTEM_HERE
    WHERE UserID LIKE 'E%' AND UserID NOT LIKE 'ESA%' AND DataLog >= 'INSERT_FIRST_DAY_HERE' AND DataLog <= 'INSERT_LAST_DAY_HERE' AND TpReg IN('AUW', 'AUY', 'AU3') AND WHICH_TRANSACTIONS
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

    # Close Channel and Handle Variable
    close(channel)
    rm(channel, con)
    hora <- as.numeric(result$HoraLog)
    user <- as.factor(gsub(' ', '', result$UserID))

    # Creates two quarters - Regular Hour and After Hour
    j <- 1
    while (j <= length(hora)) {
        if (hora[[j]] >= 0 && hora[[j]] < 8000000)
            hora[[j]] <- 1
        if (hora[[j]] >= 8000000 && hora[[j]] < 22000000)
            hora[[j]] <- 0
        if (hora[[j]] >= 22000000 && hora[[j]] <= 23595900)
            hora[[j]] <- 1
        j <- j + 1
    }

    #Pair the information on a Data Frame and Table the DataFrame
    dataset <- data.frame(hora, user)
    tabled = table(dataset)
    rm(hora, user)
    gc()

    print("A analisar sistema... Em seguida irá abrir, automaticamente, uma Janela no Browser.")

    #Calculate amount of dubious transactions per user (%)
    foraDeHoras <- tapply(dataset$hora, dataset$user, function(x) {
        tabled = prop.table(table(x))
        if (!is.na(tabled[2])) {
            return(tabled[2])
        } else {
            return(0)
        }
    })
    gc()
    rm(dataset, j, tabled)

    # Store and gsub to remove garbage associated -> RODBC introduces garbage;
    hora <- result$HoraLog
    data <- result$DataLog
    user <- gsub(" ", "", result$UserID)
    trans <- gsub(" ", "", result$Trans)
    programa <- gsub(" ", "", result$Programa)
    tipo <- gsub(" ", "", result$TpReg)
    terminal <- gsub(" ", "", result$Terminal)
 

    # Transform Hour; Removes Seconds and Minutes (hours only, to be used after)
    horaUnitaria <- substr(hora, 1, nchar(hora) - 6)

    # Converting hours to seconds
    timestamp <- result$timestamp
    # Create Dataset 
    dataset <- data.frame(data, horaUnitaria, timestamp, user, programa, tipo, terminal, trans)

    ####### Number/Amount of Transactions ########
    size <- tapply(dataset$timestamp, dataset$user, function(x) {
        size <- length(x)
        return(size)
    })

    ###### Number of Different Programs Used #####
    noProgramas <- tapply(dataset$programa, dataset$user, function(x) {
        return(length(unique(x)))
    })

    ###### Number of Different Terminals Used #####
    noTerminals <- tapply(dataset$terminal, dataset$user, function(x) {
        return(length(unique(x)))
      })

    ###### Checks if User is calling another User. 
    crossTerminal <- tapply(dataset$terminal, dataset$user, function(x) {
        w <- grepl("^EX", x)
        k <- unlist(which(w == TRUE))
        
        return (k)
    })                

    ##### % TpReg / User #####
    tpReg <- tapply(dataset$tipo, dataset$user, function(x) {
        return(length(unique(x)))
    })

    ##### SESSION_MANAGER Counter #####
    sessionMan <- tapply(dataset$trans, dataset$user, function(x) {
        y <- length(x[x == 'SESSION_MANAGER'])
        return( y / length(x))
     })

    ###### Number of Transactions that count for the timeSignature calculation #####
    signatureSize <- tapply(dataset$timestamp, dataset$user, function(x) {
        if (length(x) >= 20) {
            middle <- diff(x)
            # Ignore intervals greater than 48 Hours.
            middle <- middle[middle < 15000]
            return(length(middle))
        } else {
            return(length(x))
        }
    })

    ####### Detection Through Iterative Variable Time Window ########
    isBot <- tapply(dataset$timestamp, dataset$user, function(x) {
        conductor <- 50
        count <- 0
        tail <- 0
        size <- length(x)
        print(size)

        while (conductor < size) {
            subset <- x[tail:conductor]
            signature <- mean(diff(subset))
            if (signature < 1) {
                count <- count + 1
                return(1)
            } else {
                # If no anomaly is found, reduce window 
                aux_conductor <- conductor - 1
                while (aux_conductor - tail >= 30) {
                    aux_subset <- x[tail:aux_conductor]
                    aux_signature <- mean(diff(aux_subset))
                    if (aux_signature < 1) {
                        count <- count + 1
                        return(1)
                    }
                    aux_conductor <- aux_conductor - 1
                }
            }
            conductor <- conductor + 1
            tail <- tail + 1
        }
        return(0)
    })

    # Calculate Time Signature for each user
    timeSignature <- tapply(dataset$timestamp, dataset$user, function(x) {
        if (length(x) >= 20) {
            middle <- diff(x)
            # Ignore intervals greater than 48 Hours.
            middle <- middle[middle < 3600]
            return(floor(mean((middle))))
        } else {
            return(-1000)
        }
    })

    # Calculate Time Signature for each user [ Eliminates differences higher than 20min ]
    timeSignature2 <- tapply(dataset$timestamp, dataset$user, function(x) {
        if (length(x) >= 20) {
            middle <- diff(x)
            # Ignore intervals greater than 48 Hours.
            middle <- middle[middle < 1200]
            return(floor(mean((middle))))
        } else {
            return(-1000)
        }
    })

    # Calculate Time Signature for each user
    timeSignatureVar <- tapply(dataset$timestamp, dataset$user, function(x) {
        if (length(x) >= 20) {
            middle <- diff(x)
            # Ignore intervals greater than 1 Hour.
            # We want to focus when the user is actually using the computer/software
            middle <- middle[middle < 1200]
            return(floor(var((middle))))
        } else {
            return(-1000)
        }
    })

    ####### Variedade Transaccional #########
    varTransacional <- tapply(dataset$trans, dataset$user, function(x) {
        x <- unique(x)
        return(length(x))
    })

    #######  Hour Fashion ########
    modaHoraria <- tapply(dataset$horaUnitaria, dataset$user, function(x) {
        tabled <- (table(x) / length(x))
        final <- var(tabled, na.rm = TRUE)
        return(final * 100)
    })

    ####### What's the highest repetition regarding the transaction number per day? ########
    repetibilidadeTrans <- tapply(dataset$data, dataset$user, function(x) {
        if (length(x) < 20)
            return(-1)
        tabled <- sort(table(x))
        # Makes sure user is active at least 3 days
        if (length(tabled) < 3)
            return(-1)
        return((max(table(tabled)) / length(unique(x))) * 100)
    })

    ####### % Days of Activity (Universe of the Query) ########
    days <- length(unique(data))
    diasActivo <- tapply(dataset$data, dataset$user, function(x) {
        return(diasActivo <- (length(unique(x)) / days) * 100)
    })

    data <- substr(result$Data, 1, nchar(result$Data - 11))
    dataset$data <- as.Date(data)

    ####### Week-End Count (Universe of the Query) ########
    weeknd <- tapply(dataset$data, dataset$user, function(x) {
        temp <- (weekdays(x, abbr = TRUE))
        temp <- temp[temp == "sßb" | temp == "dom"]
        return(length(temp)/length(data))
    })

    #######  Monday Count (Universe of the Query) ########
    monday <- tapply(dataset$data, dataset$user, function(x) {
        temp <- (weekdays(x, abbr = TRUE))
        temp <- temp[temp == "seg"]
        return(length(temp) / length(x))
    })

    #######  Tuesday Count (Universe of the Query) ########
    tuesday <- tapply(dataset$data, dataset$user, function(x) {
        temp <- (weekdays(x, abbr = TRUE))
        temp <- temp[temp == "ter"]
        return(length(temp) / length(x))
    })

    #######  Wednesdays Count (Universe of the Query) ########
    wednesday <- tapply(dataset$data, dataset$user, function(x) {
        temp <- (weekdays(x, abbr = TRUE))
        temp <- temp[temp == "qua"]
        return(length(temp) / length(x))
    })

    #######  Thursday Count (Universe of the Query) ########
    thursday <- tapply(dataset$data, dataset$user, function(x) {
        temp <- (weekdays(x, abbr = TRUE))
        temp <- temp[temp == "qui"]
        return(length(temp) / length(x))
    })

    #######  Friday Count (Universe of the Query) ########
    friday <- tapply(dataset$data, dataset$user, function(x) {
        temp <- (weekdays(x, abbr = TRUE))
        temp <- temp[temp == "sex"]
        return(length(temp) / length(x))
    })

    ##### Var from Weekly Activity #####
    varSemana <- tapply(dataset$data, dataset$user, function(x) {
        temp <- (weekdays(x, abbr = TRUE))
        sex <- abs((length(temp[temp == "sex"]) / length(x)) - 0.2)
        qui <- abs((length(temp[temp == "qui"]) / length(x)) - 0.2)
        qua <- abs((length(temp[temp == "qua"]) / length(x)) - 0.2)
        ter <- abs((length(temp[temp == "ter"]) / length(x)) - 0.2)
        seg <- abs((length(temp[temp == "seg"]) / length(x)) - 0.2)
        avg <- (seg + ter + qua + qui + sex) / 5
        return(avg)
    })

    # (size - signatureSize ) / size   
    report <- data.frame(signatureSize, ( (size - signatureSize)/size ) , size, timeSignature, timeSignature2, (timeSignature - timeSignature2), timeSignatureVar, isBot, foraDeHoras, repetibilidadeTrans, diasActivo, noProgramas, noTerminals, tpReg, varTransacional, varSemana, weeknd, monday, tuesday, wednesday, thursday, friday, sessionMan, modaHoraria)
    report2 <- report[report$size > 100,]
    # Timer End, Diff and Print
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    print(time.taken)

    report2 <- select(report2, - c(monday, tuesday, weeknd, tpReg, wednesday, thursday, friday))
    load("aiPoweredDecision")
    aiDecision1 <- predict(gaussian16, report2, type = "response")
    aiDecision2 <- predict(logReg05, report2, type = "response")
    aiDecision3 <- predict(logReg16, report2, type = "response")
    report2 <- cbind(report2, aiDecision1, aiDecision2, aiDecision3)

    # Create new dataframe with synthetic features  
    x <- data.frame(log2(report2$X..size...signatureSize..size.), log2(report2$timeSignature + 0.01))
    xyz <- c("a", "b")
    colnames(x) <- xyz
    a <- kmeans(x, centers = 4, nstart = 1000)
    x <- cbind(x, a$cluster)
    x <- cbind(x, row.names(report2))

    # Garbage Collector
    gc()
    plot3d(report2$aiDecision1, report2$aiDecision2, report2$aiDecision3, col = ifelse(report2$aiDecision3 > 0.6, "red", "black"))
    # Interactive Plot for Better Inspection 
  ui <- fluidPage(
     # Título Gráfico 1
    fluidRow(
    column(width = 5, h4("Clustering de Utilizadores Resultante de Feature Engineering"))
    ),
    # Plot Gráfico 1 
  fluidRow(
    column(width = 5, plotOutput("plot1", height = 500, width = 1200, brush = brushOpts(id = "plot1_brush")))
    ),
    # Plot Brush
  fluidRow(
    column(width = 10, h4("Selecção de Pontos"), verbatimTextOutput("brush_info"))
    )
 )

  server <- function(input, output) {
        output$plot1 <- renderPlot({
            ggplot(x, aes((b), (a), color = `a$cluster`)) + geom_point() + geom_vline(xintercept = 5.0, col = "red") + geom_hline(yintercept = -5.0, col = "red")
        })

        output$brush_info <- renderPrint({
            brushedPoints(x, input$plot1_brush, xvar = "b", yvar = "a")
        })
    }
    # Launch GUI via Browser 
    shinyApp(ui, server)

    # Remoção de variáveis secundárias / inutilizadas / abstratas
    #report2 <- select(report2, - c(signatureSize, X..size...signatureSize..size., isBot, X.timeSignature...timeSignature2., sessionMan, varSemana))
}