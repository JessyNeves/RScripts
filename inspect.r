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

# Connection to DB
con <- 'Driver={SQL Server};Server=XXXX;Database=XXXX;Trusted_Connection=yes'
channel <- odbcDriverConnect(con)
# Menu ; Which system? When? All Transactions? 
one    <- "p05"
two    <- "p15"
three  <- "p16"
four   <- "p25"
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
bigCrunch <- readline( prompt = "Insira a data de fim. Formato: 20191001 (2019/10/01 - Ano / Mês / Dia ): ")
# Unfinished Query. Lacks system ; First Day Date ; Last Day Date
query <- "SELECT timestamp=Datediff ( second, '2018-12-01 00:00:00',SUBSTRING(DataLog, 1, 4) + '-' + SUBSTRING(DataLog, 5, 2) + '-' + SUBSTRING(DataLog, 7,2) + ' ' + SUBSTRING(HoraLog, 1, 2) + ':' + SUBSTRING(HoraLog, 3, 2) + ':' + SUBSTRING(HoraLog, 5,2)),
UserID, DataLog, HoraLog, Trans, Data FROM XXXXX
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
if ( abs(as.numeric(bigBang) - as.numeric(bigCrunch)) > 230) {
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

hora <- result$HoraLog
user <- result$UserID
trans <- result$Trans
data <- result$DataLog

# Remove associated garbage from UserID
user <- gsub(' ', '', user)
trans <- gsub(' ', '', result$Trans)
# Convert hour format to use native functions.
horaUnitaria <- substr(hora, 1, nchar(hora) - 6)
# Converting hours to seconds
timestamp <- result$timestamp
dataset <- data.frame(data, horaUnitaria, timestamp, user)

####### SIZE ########
size <- tapply(dataset$timestamp, dataset$user, function(x) {
    size <- length(x)
    return(size)
})
# Signature Size
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
            while (aux_conductor - tail >= 25) {
                aux_subset <- x[tail:aux_conductor]
                aux_signature <- mean(diff(aux_subset))
                if (aux_signature < 1) {
                    count <- count + 1
                    toReturn <- 1
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
        middle <- middle[middle < 15000]
        return(floor(mean((middle))))
    } else {                                     
        return(-1000)
    }
})
####### Variedade Transaccional #########
varTransacional <- tapply(result$Trans, result$UserID, function(x) {
    x <- unique(x)
    return(length(x))
} )
#######  Moda Horária por Utilizador ########
modaHoraria <- tapply(dataset$horaUnitaria, dataset$user, function(x) {
    tabled <- (table(x) / length(x))
    final <- var(tabled, na.rm = TRUE)
    return(final * 100)
})
####### Transaction Counter Per Day / Per User ########
transactionRepetition <- tapply(dataset$data, dataset$user, function(x) {
    if (length(x) < 20)
        return(-1)
    tabled <- sort(table(x))
    if (length(tabled) < 5)
        return(-1)
    return((max(table(tabled)) / length(unique(x))) * 100)
})
####### Transaction Counter Per Day / Per User / Variance ########
transactionRepetitionVariance <- tapply(dataset$data, dataset$user, function(x) {
    if (length(x) < 20)
        return(-1)
    tabled <- sort(table(x))
    if (length(tabled) < 5)
        return(-1)
    return(var(tabled))
})
####### % Days of Activity (Universe of the Query) ########
days <- length(unique(data))    
daysOfUser <- tapply(dataset$data, dataset$user, function(x) {
    return(daysOfUser <- (length(unique(x) ) / days) * 100)
})
data <- substr(result$Data, 1, nchar(result$Data - 11))
dataset$data <- as.Date(data)
####### Week-End Count (Universe of the Query) ########
weeknd <- tapply(dataset$data, dataset$user, function(x) { 
temp <- unique(x)
temp <- (weekdays(temp, abbr = TRUE))
temp <- temp[temp == "sßb" | temp == "dom"]
return(length(temp))
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
# (size - signatureSize ) / size   
report <- data.frame(signatureSize, (size - signatureSize) / size, varTransacional, modaHoraria, isBot, foraDeHoras, timeSignature, size, transactionRepetition, transactionRepetitionVariance, weeknd, monday, tuesday, wednesday, thursday, friday, daysOfUser)
report2 <- report[report$size > 100,]
end.time <- Sys.time()
time.taken <- end.time - start.time
print(time.taken)

report2 <- select(report2, - c(monday, weeknd, tuesday, wednesday, thursday, friday))
# Avoid log2(0)  
report2$timeSignature <- report2$timeSignature + 0.1
# Create new dataframe with synthetic features  
x <- data.frame(log2(report2$X.size...signatureSize..size), log2(report2$timeSignature))
a <- kmeans(x, centers = 3)
x <- cbind(x, a$cluster)
x <- cbind(x, row.names(report2))

# Remoção de variáveis secundárias / inutilizadas / abstratas
report2 <- select(report2, - c(signatureSize, X.size...signatureSize..size, varTransacional, transactionRepetitionVariance))
rm(report)
gc()
# Interactive Plot for Better Inspection 
ui <- fluidPage(
  # Título Gráfico 1
  fluidRow(
    column(width = 5, h4("Clustering de Utilizadores Resultante de Feature Engineering"))
    ),
  # Plot Gráfico 1 
  fluidRow(
    column(width = 5, plotOutput("plot1", height = 500, width = 1200, brush = brushOpts(id = "plot1_brush") ) )
    ),
  # Plot Brush
  fluidRow(
    column(width = 10, h4("Selecção de Pontos"), verbatimTextOutput("brush_info"))
    )
 )
server <- function(input, output) {
  output$plot1 <- renderPlot({
   ggplot(x, aes((log2.report2.timeSignature.), (log2.report2.X.size...signatureSize..size.), color = `a$cluster`) ) + geom_point() + geom_vline(xintercept = 7.0) + geom_hline(yintercept = -7.0)
  })

  output$brush_info <- renderPrint({
   brushedPoints(x, input$plot1_brush, xvar = "log2.report2.timeSignature.", yvar = "log2.report2.X.size...signatureSize..size.")
  })
 }
# Launch GUI via Browser 
shinyApp(ui, server)
}
