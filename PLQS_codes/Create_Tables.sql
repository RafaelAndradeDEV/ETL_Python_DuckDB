-- Criação das tabelas
CREATE TABLE sales(
   invoiceno INT NOT NULL,
   stockcode VARCHAR(7) NOT NULL,
   description VARCHAR(40) NOT NULL,
   quantity INT NOT NULL,
   invoicedate VARCHAR(16) NOT NULL,
   unitprice DECIMAL(8, 2) NOT NULL,
   customerid INT NOT NULL,
   country VARCHAR(20)
);
--
CREATE TABLE sales_calculated(
   invoiceno INT NOT NULL,
   stockcode VARCHAR(7) NOT NULL,
   description VARCHAR(40) NOT NULL,
   quantity INT NOT NULL,
   invoicedate VARCHAR(16) NOT NULL,
   unitprice DECIMAL(8, 2) NOT NULL,
   customerid INT NOT NULL,
   country VARCHAR(20) NOT NULL,
   total_sales DECIMAL(10, 2) NOT NULL
);

