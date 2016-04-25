# Create Sales Data

numWeeks <- 500
numStores <- 4000 # 3e4
numRows <- numWeeks*numStores

# When on Promo, price is about 10 and volumeSales is about 50 (first half of weeks)
dataOnPromo <- data.frame( volumeSales=rnorm(n = numRows/2,mean = 50,sd = 3 ), 
                           price=rnorm(n = numRows/2,mean = 10,sd = 0.5 ), promo=1, 
                           storeId=rep(1:numStores,each=numWeeks/2),
                           weekId=rep(1:(numWeeks/2),times=numStores) )
# When off Promo, price is about 15 and volumeSales is about 10 (second half of weeks)
dataNonPromo <- data.frame( volumeSales=rnorm(n = numRows/2,mean = 10,sd = 2 ), 
                            price=rnorm(n = numRows/2,mean = 15,sd = 0.5 ), promo=0, 
                            storeId=rep(1:numStores,each=numWeeks/2),
                            weekId=rep((numWeeks/2 + 1):numWeeks,times=numStores))

dataTotal <- rbind(dataNonPromo, dataOnPromo)
dataTotal <- dataTotal[order(dataTotal$storeId,dataTotal$weekId),]


# Checking weeks of datasets
unique(dataOnPromo$weekId)
unique(dataNonPromo$weekId)

# List of stores
storesList <- unique(dataTotal$storeId)

# Calculate sum of VolumeSales by Store  (only for the first 100 stores)
system.time({
  sumVolume <- c()
  for (curStore in storesList[1:100]){
    curData <- dataTotal[dataTotal$storeId==curStore, ]
    sumVolume[curStore] <- 0
    for (i in 1:nrow(curData)){
      sumVolume[curStore] <- sumVolume[curStore] + curData$volumeSales[i]
    }
  }
})

# time about 200 secs 


# Same using aggregate
system.time( out1 <- aggregate(x = dataTotal$volumeSales, 
                               by = list(dataTotal$storeId), sum) )


# Same using data.table
library(data.table)
dataTotalDT <- as.data.table(dataTotal)
system.time( out2 <- dataTotalDT[,sum(volumeSales),by=list(storeId)] )


### Linear Regression (pooled version)
system.time( lm1 <- lm(volumeSales~price+promo,data = dataTotal) )
sumlm1 <- summary(lm1)
sumlm1




# Linear regression per store (for loop, subsetting using data.frame "dataTotal")
t1 <- system.time({
  coef <- c()
  for (curStore in storesList){
    curData <- dataTotal[dataTotal$storeId==curStore,]
    #curData <- dataTotalDT[storeId==curStore,]
    curLM <- lm(volumeSales~price+promo,data = curData)
    coef[curStore] <- coefficients(curLM)[3]  
  }
})
summary(coef)
hist(coef)


# Linear regression per store (for loop, subsetting using data.table "dataTotalDT")
t1b <- system.time({
  coef <- c()
  for (curStore in storesList){
    #curData <- dataTotal[dataTotal$storeId==curStore,]
    curData <- dataTotalDT[storeId==curStore,]
    curLM <- lm(volumeSales~price+promo,data = curData)
    coef[curStore] <- coefficients(curLM)[3]  
  }
})



# Linear regression per store (loop inside data.table "dataTotalDT" using function "lmfun")
lmfun <- function(curData){
  curLM <- lm(volumeSales~price+promo,data = curData)
  return(coefficients(curLM)[3])
}

t2 <- system.time( coefs <- dataTotalDT[,list( promoCoef=lmfun(.SD) ),by=storeId] )



# Linear regression per store (loop inside data.table "dataTotalDT" using function "lmfun", parallelized)
library(doParallel)
numCores <- 2
cl <- makeCluster(numCores)
registerDoParallel(cl)
numGroups <- 2
dataTotalDT[,groupId:=rep(1:numGroups,each=numRows/numGroups)]
setkey(dataTotalDT,groupId)


lmfun2 <- function(dataTotalDT,curGroupId){
  library(data.table)
  curData <- dataTotalDT[groupId==curGroupId,]
  coefs <- curData[,list( promoCoef=lmfun(.SD) ),by=storeId]
  return(coefs)
}
stopCluster(cl)

t3<-system.time(coefList <- foreach(curGroupId=1:numGroups) %dopar% { lmfun2(dataTotalDT,curGroupId) })
outTotal <- rbindlist(coefList)
cat("For:", t1[3],"For (DT):", t1b[3], " DT:", t2[3], " DT parallel:",t3[3],"\n")



# Create plot of actual and predicted values of linear models per store
lmfunPlot <- function(curData){
  curLM <- lm(volumeSales~price+promo,data = curData)
  curStore <- unique(curData$storeId)
  png(filename = paste0("LMimages/dataPlotStore",curStore,".png"))
  plot(curData$volumeSales,xlab = "weeks",ylab = "Volume Sales",type="l",col="blue")
  title(paste0("Linear Model of store ",curStore))
  lines(fitted(curLM),col="red")
  dev.off() 
  cat("\rCreated plot",curStore)
}

dir.create("LMimages")

# Create plots for the first 100 stores
for (curStore in 1:100){
  curData <- dataTotalDT[storeId==curStore,]
  lmfunPlot(curData)
}



# Same plot using dygraphs package
library(dygraphs)
library(knitr)
library(xts)


cols <- c("blue", "green", "red")

plotTitle <- paste0("Linear Model of store ",curStore)

dataSeries <- data.frame(Week=curData$weekId,ActualSales=curData$volumeSales,
                         PredictedSales=fitted(curLM))
dygraph(dataSeries, main = plotTitle)  %>% 
  dyAxis("y", label = "Sales")        %>%
  dyRangeSelector()                    %>%
  dyAxis("x", drawGrid = FALSE)        %>%
  dyOptions(colors = cols,drawPoints = TRUE, pointSize = 2) %>%
  dyLegend(width = 600)

# Including price in a secondary axis
dataSeries <- data.frame(Week=curData$weekId,Price=curData$price,ActualSales=curData$volumeSales,
                         PredictedSales=fitted(curLM))
dygraph(dataSeries, main = plotTitle)  %>% 
  dySeries("Price", axis = 'y2') %>% 
  dyAxis("y", label = "Sales")        %>%
  dyAxis("y2", label = "Price", independentTicks = TRUE, drawGrid = FALSE)  %>%
  dyRangeSelector()                    %>%
  dyAxis("x", drawGrid = FALSE)        %>%
  dyOptions(colors = cols,drawPoints = TRUE, pointSize = 2) %>%
  dyLegend(width = 600)
