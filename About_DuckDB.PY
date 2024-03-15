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
"poetry shell" - sempre que trocar o bash ou iniciar novamente
(check first version python in env)"poetry add streamlit"
"streamlit run app.py"
"poetry remove django" (To exclude some library)

using venv: python -m venv .venv
source .venv/Scripts/activate

# O Streamlit é uma biblioteca que renderiza react, transformando código python em Javascript
- Uso do Site "Render", tipo AWS e Azure, não é necessário cartão de crédito. É possível subir um banco postgree gratuitamente

# O que é o docker?
- Ele abstrai a máquina que está em uso, seja linux ou windows ou servidor, invés disso ela ira rodar no docker. A vantagem disso é que todos os lugares tem o mesmo docker
- Ele tem em seu código, todos os comandos que rodamos no terminal, mesma sequência de passos. Ele irá criar uma imagem de docker, como se fosse um grande zip, com todos os arquivos e pastas no workspace e envia para o servidor
- Existem 4 pilares ao redor do docker: 1° dockerfile / 2° Código: Todos os outros arquivos que não seja o dockerfile / 3° Imagem: Que é basicamente todo o workspace, tudo que é necessário para rodar, o container é uma instância da minha imagem / 4° Container: Os contêineres em execução usam uma visão imutável da imagem, permitindo vários recipientes para reutilizar a mesma imagem simultaneamente. Como as imagens são arquivos, elas podem ser gerenciadas por sistemas de controle de versão, melhorando a automação do contêiner e o provisionamento.


-> Criação do "dockerfile" na pasta atual

# Pip e PipX: O pip é estável o que é muito bom, mas tem muitas novidades que demoram a chegar nele. Já o Pipx, é possível criar ambientes virtuais por usuário, se houver mais de um usuário na máquina, não "sujara as dependências" do outro. Instalada pelo pip globalmente 
# O poetry segrega entre as dependências instaladas na máquina e as dependências no ambiente de uma maneira mais organizada

# Instalar bibliotecas: 
   - gdown(biblioteca para fazer download de google drive) e necessita de IAM instalado, para leitura de variáveis. Existe também a própria do google, tem que fazer um cadastro no GCP para usar. As vezes a gdown pode dar problema de request, por isso em projetos utilize a adequada(google-api-python)
   - duckdb(queries e DB)
   - streamlit(dashboard bonito)
   - psycopg2-binary(para trabalhar com banco postgree)
   - python-dotenv(Para leitura de variáveis) 
   - psycopg2(postgree)
   - sqlalchemy(para SQL)

# Para saber como ficaria utilizando pip invés de poetry:
"pip freeze >> requirements.txt" iria ficar muito bagunçado

 """

