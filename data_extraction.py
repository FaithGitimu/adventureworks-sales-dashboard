import pandas as pd,pyodbc

conn=pyodbc.connect(
    "DRIVER={SQL Server};"
    "SERVER=Your-server-name;"
    "DATABASE=AdventureWorks2022;"
    "Trusted_Connection=yes;"

)

query = """
SELECT 
C.CustomerID,
S.OrderDate,
S.ShipDate,

COALESCE(P.FirstName + ' ' + P.LastName, St.Name) AS CustomerName,

PP.PhoneNumber,
PE.EmailAddress,

St.Name AS StoreName,
SM.Name AS ShipMethodName,

CR.Name AS CountryRegionName,
SP.Name AS StateProvinceName,
A.City,

PR.Name AS ProductName,
PC.Name AS ProductCategoryName,
PSC.Name AS ProductSubcategoryName,

Cc.CardType,

SD.OrderQty,
SD.LineTotal,
S.TotalDue AS TotalOrderAmount

FROM Sales.SalesOrderHeader S

JOIN Sales.Customer C 
ON S.CustomerID = C.CustomerID


LEFT JOIN Person.Person P 
ON C.PersonID = P.BusinessEntityID



LEFT JOIN (
    SELECT BusinessEntityID, PhoneNumber
    FROM (
        SELECT BusinessEntityID, PhoneNumber,
               ROW_NUMBER() OVER (PARTITION BY BusinessEntityID ORDER BY ModifiedDate DESC) rn
        FROM Person.PersonPhone
    ) x
    WHERE rn = 1
) PP
ON P.BusinessEntityID = PP.BusinessEntityID


 
LEFT JOIN (
    SELECT BusinessEntityID, EmailAddress
    FROM (
        SELECT BusinessEntityID, EmailAddress,
               ROW_NUMBER() OVER (PARTITION BY BusinessEntityID ORDER BY ModifiedDate DESC) rn
        FROM Person.EmailAddress
    ) x
    WHERE rn = 1
) PE
ON P.BusinessEntityID = PE.BusinessEntityID


 
LEFT JOIN Person.BusinessEntityAddress BEA
ON P.BusinessEntityID = BEA.BusinessEntityID

LEFT JOIN Person.Address A
ON BEA.AddressID = A.AddressID

LEFT JOIN Person.StateProvince SP
ON A.StateProvinceID = SP.StateProvinceID

LEFT JOIN Person.CountryRegion CR
ON SP.CountryRegionCode = CR.CountryRegionCode


LEFT JOIN Sales.Store St 
ON C.StoreID = St.BusinessEntityID


LEFT JOIN Purchasing.ShipMethod SM 
ON S.ShipMethodID = SM.ShipMethodID


JOIN Sales.SalesOrderDetail SD
ON S.SalesOrderID = SD.SalesOrderID


JOIN Production.Product PR 
ON SD.ProductID = PR.ProductID


LEFT JOIN Production.ProductSubcategory PSC 
ON PR.ProductSubcategoryID = PSC.ProductSubcategoryID


LEFT JOIN Production.ProductCategory PC
ON PSC.ProductCategoryID = PC.ProductCategoryID


LEFT JOIN Sales.CreditCard Cc 
ON S.CreditCardID = Cc.CreditCardID


WHERE S.OrderDate >= '2014-01-01' 
AND S.OrderDate < '2015-01-01'


ORDER BY S.OrderDate

"""

df = pd.read_sql(query,conn)
print(df.head())

df.to_csv('sales_dataset.csv',index=False)