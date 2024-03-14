""" 
# His backEngine is write in C++
# There are 2 categories of databases:
- In-Process(In local machine, it runs) / Client-server(Needs a server to run queries)
- Transactional(OLTP) / Analytical(OLA P)
Ex: - SQLite: In-process and Transactional
- ClickHouse: Client-Server and Analytical
- PostgreSQL: Client-Server and Transactional

-- DuckDB fits in Analytical and In-process, it brings a lot use advantages 

# Can receive and work with a lot of types(CSV, JSON, Parquet, Iceberg), have it's own database(.duckdb) and connect libraries, such: Polars, pandas, numpy.
# Same for data sources(MySQL, PostgreSQL, SQLite) and destinations(http(s)://, s3://, gcs://, azure://)
 
# MotherDuck is a DB in Cloud(so simple: "conn = duckdb.connect('md:')")
# When connected to MD, we can use run all the queries, using the same clients, everything is ran in cloud

- to see databases in MD: "conn.sql("show databases")"


# Coding:
- Install pyenv-win
- Install poetry
  
# Execute:
"pyenv install 3.12.1 or pyenv local 3.12.1(if already installed)"
"poetry env use 3.12.1" or "poetry env use $(which python3.12.1)"
"poetry init"
"poetry shell"
(check first version python in env)"poetry add streamlit"


# O Streamlit é uma biblioteca que renderiza react, transformando código python em Javascript
- Uso do Site "Render", tipo AWS e Azure, não é necessário cartão de crédito 

# O que é o docker?
- Ele abstrai a máquina que está em uso, seja linux ou windows ou servidor, invés disso ela ira rodar no docker. A vantagem disso é que todos os lugares tem o mesmo docker
- Ele tem em seu código, todos os comandos que rodamos no terminal, mesma sequência de passos. Ele irá criar uma imagem de docker, como se fosse um grande zip, com todos os arquivos e pastas no workspace e envia para o servidor


-> Criação do "dockerfile" na pasta atual





 """
