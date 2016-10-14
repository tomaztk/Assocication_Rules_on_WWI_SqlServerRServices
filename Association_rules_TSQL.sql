/*

** Author: Tomaz Kastrun
** Web: http://tomaztsql.wordpress.com
** Twitter: @tomaz_tsql
** Created: 14.10.2016; Ljubljana
** Executing Association Rules on WideWorldImportersDW database
** using sp_execute_external_script

*/



USE WideWorldIMportersDW;
GO


;WITH PRODUCT
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
--,O.[Order Key]
--,O.[Stock Item Key]
,P.PRODUCT_GROUP AS ProductGroup
--,O.[Description]
,LEFT([Stock Item],8) AS ProductDescription

FROM [Fact].[Order] AS O
JOIN PRODUCT AS P
	ON P.[Stock Item Key] = O.[Stock Item Key]
GROUP BY
	 O.[WWI Order ID]
	,P.PRODUCT_GROUP
	,LEFT([Stock Item],8)
ORDER BY 
	O.[WWI Order ID]
	


--- Products Clean
 
 SELECT 
 -- [Stock Item Key]
-- ,[WWI Stock Item ID]
-- ,[Stock Item] 
 LEFT([Stock Item], 8) AS L8DESC 
-- ,ROW_NUMBER() OVER (PARTITION BY LEFT([Stock Item], 8) ORDER BY ([Stock Item])) AS RN_ID_PR
 ,DENSE_RANK() OVER (ORDER BY (LEFT([Stock Item], 8))) AS PRODUCT_GROUP
 FROM [Dimension].[Stock Item]

GROUP BY  LEFT([Stock Item], 8)


------------------------------------------
------------------------------------------
--- USING  Association Rules 
--- With sp_execute_External_script
------------------------------------------
------------------------------------------

-- Getting Association Rules into T-SQL
DECLARE @TSQL AS NVARCHAR(MAX)
SET @TSQL = N'WITH PRODUCT
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
                      O.[WWI Order ID]'

DECLARE @RScript AS NVARCHAR(MAX)
SET @RScript = N'
				library(arules)
				cust.data <- InputDataSet
				cd_f <- data.frame(OrderID=as.factor(cust.data$OrderID),ProductGroup=as.factor(cust.data$ProductGroup))
				cd_f2_tran  <- as(split(cd_f[,"ProductGroup"], cd_f[,"OrderID"]), "transactions")
				rules <- apriori(cd_f2_tran, parameter=list(support=0.01, confidence=0.1))
				OutputDataSet <- data.frame(inspect(rules))'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RScript
		  ,@input_data_1 = @TSQL
		  
WITH RESULT SETS ((
     lhs NVARCHAR(500)
	,[Var.2] NVARCHAR(10)
	,rhs NVARCHAR(500)
	,support DECIMAL(18,3)
	,confidence DECIMAL(18,3)
	,lift DECIMAL(18,3)
				 )); 



				 
-- Getting Association Rules into T-SQL
-- ProductGroups with support information

DECLARE @TSQL AS NVARCHAR(MAX)
SET @TSQL = N'WITH PRODUCT
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
                      O.[WWI Order ID]'

DECLARE @RScript AS NVARCHAR(MAX)
SET @RScript = N'
				library(arules)
				cust.data <- InputDataSet
				cd_f <- data.frame(OrderID=as.factor(cust.data$OrderID),ProductGroup=as.factor(cust.data$ProductGroup))
				cd_f2_tran  <- as(split(cd_f[,"ProductGroup"], cd_f[,"OrderID"]), "transactions")
				PgroupSets <- eclat(cd_f2_tran, parameter = list(support = 0.05), control = list(verbose=FALSE))
				normalizedGroups <- PgroupSets[size(items(PgroupSets)) == 1]
				eachSupport <- quality(normalizedGroups)$support
				GroupName <- unlist(LIST(items(normalizedGroups), decode = FALSE))
				OutputDataSet <- data.frame(GroupName, eachSupport);'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RScript
		  ,@input_data_1 = @TSQL
		  
WITH RESULT SETS ((
     ProductGroup NVARCHAR(500)
	,support DECIMAL(18,3)
				 )); 