--ETL entre tabelas:
INSERT INTO sales_calculated (invoiceno, stockcode,description, quantity,invoicedate,unitprice, customerid, country, total_sales) 
SELECT invoiceno, stockcode,description, quantity,invoicedate,unitprice, customerid, country, (quantity * unitprice) as total_sales 
FROM sales;
