""" 
# His backEngine is write in C++
# There are 2 categories of databases:
- In-Process(In local machine, it runs) / Client-server(Needs a server to run queries)
- Transactional(OLTP) / Analytical(OLAP)
Ex: - SQLite: In-process and Transactional
- ClickHouse: Client-Server and Analytical
- PostgreSQL: Client-Server and Transactional

-- DuckDB fits in Analytical and In-process, it brings a lot use advantages 

# Can receive and work with a lot of types(CSV, JSON, Parquet, Iceberg), have it's own database(.duckdb) and connect libraries, such: Polars, pandas, numpy.
# Same for data sources(MySQL, PostgreSQL, SQLite) and destinations(http(s)://, s3://, gcs://, azure://)

# MotherDuck is a DB in Cloud(so simple: "conn = duckdb.connect('md:')")
# When connected to MD, we can use run all the queries, using the same clients, everything is ran in cloud

- to see databases in MD: "conn.sql("show databases")"





 """
