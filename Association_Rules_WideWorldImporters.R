#######################################################################
## Author: Tomaz Kastrun
## Web: http://tomaztsql.wordpress.com
## Twitter: @tomaz_tsql
## Created: 14.10.2016; Ljubljana
## Executing Association Rules on WideWorldImportersDW database
## using sp_execute_external_script
#######################################################################

setwd("C:/DataTK")

library(RODBC)
library(arules)
library(arulesViz)


myconn <-odbcDriverConnect("driver={SQL Server};Server=SICN-KASTRUN;database=WideWorldImportersDW;trusted_connection=true")


cust.data <- sqlQuery(myconn, "
                      WITH PRODUCT
                        AS
                      (
                      SELECT
                      [Stock Item Key]
                      ,[WWI Stock Item ID]
                      ,[Stock Item] 
                      ,LEFT([Stock Item], 8) AS L8DESC 
                      ,ROW_NUMBER() OVER (PARTITION BY LEFT([Stock Item], 8) ORDER BY ([Stock Item])) AS RN_ID_PR
                      ,DENSE_RANK() OVER (ORDER BY (LEFT([Stock Item], 8))) AS PRODUCT_GROUP
                      FROM [Dimension].[Stock Item]
                      )
                      
                      SELECT
                      O.[WWI Order ID] AS OrderID
                      -- ,O.[Order Key]   AS OrderLineID
                      -- ,O.[Stock Item Key] AS ProductID
                      ,P.PRODUCT_GROUP AS ProductGroup
                      -- ,O.[Description] AS ProductDescription
                      ,LEFT([Stock Item],8) AS ProductDescription
                      
                      FROM [Fact].[Order] AS O
                      JOIN PRODUCT AS P
                      ON P.[Stock Item Key] = O.[Stock Item Key]
                      GROUP BY
                       O.[WWI Order ID]
                      ,P.PRODUCT_GROUP 
                      ,LEFT([Stock Item],8) 
                      ORDER BY 
                      O.[WWI Order ID] ")

close(myconn) 


# Check Data
str(cust.data)
# all int, Productdescription is factor; no worries


  cd_f <- data.frame(OrderID=as.factor(cust.data$OrderID),ProductGroup=as.factor(cust.data$ProductGroup))
str(cd_f)

# transform the data

# convert to transactions  ### Wrong approach!
##cd_f2_tran <- as(cd_f2, "transactions") 
##  231412 transactions (rows) and
##  73643 items (columns)


# convert to transactions ### correct approach!
cd_f2_tran  <- as(split(cd_f[,"ProductGroup"], cd_f[,"OrderID"]), "transactions")
#LIST(cd_f2_tran)


#find rules
rules <- apriori(cd_f2_tran, parameter=list(support=0.01, confidence=0.1))

# store in dataframe
df_r <- data.frame(inspect(rules))


# chart if needed
plot(rules, method="grouped", control=list(k=20));


# groups by support
PgroupSets <- eclat(cd_f2_tran, parameter = list(support = 0.05), control = list(verbose=FALSE));
normalizedGroups <- PgroupSets[size(items(PgroupSets)) == 1];
eachSupport <- quality(normalizedGroups)$support;
GroupName <- unlist(LIST(items(normalizedGroups), decode = FALSE));
df_groupsupport <- data.frame(GroupName, eachSupport)




