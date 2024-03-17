import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from pandas import DataFrame
from duckdb import DuckDBPyRelation

class Pipeline:
   def __init__(self) -> None:
      load_dotenv()
      self.arquivos_csv = []
      self.arquivos_parquet = []
      self.arquivos_json = []

   def  baixar_os_arquivos_do_google_drive(self, url_pasta:str, diretorio_local:str):
      os.makedirs(diretorio_local, exist_ok=True)
      gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

   # Função para listar arquivos CSV no diretório especificado
   def listar_arquivos(self, diretorio:str):
      todos_arquivos = os.listdir(diretorio)
      todos_arquivos = list(filter(lambda x:os.path.isfile(f"{diretorio}\\"+x), todos_arquivos))

      for arquivo in todos_arquivos:
         arquivo = arquivo.lower()
         caminho_completo = os.path.join(diretorio, arquivo)
         if arquivo.endswith(".csv"):
            self.arquivos_csv.append(caminho_completo)
         elif arquivo.endswith('.parquet'):
            self.arquivos_parquet.append(caminho_completo)
         elif arquivo.endswith(".json"): 
            self.arquivos_json.append(caminho_completo)
      
   # Função para ler um arquivos(CSV, parquet, json) e retornar um DataFrame duckdb
   def ler_csv(self): #ler arquivos e juntar
      dataframe_duckdb_from_csv = duckdb.read_csv(self.arquivos_csv)
      
      return dataframe_duckdb_from_csv
   
   # Função para adicionar uma coluna de total de vendas
   def transformar(self, df:DuckDBPyRelation) -> DataFrame:
      #df_transformado = duckdb.sql("SELECT * FROM df limit 20").df()
      df_transformado = duckdb.sql("SELECT *, quantity * unitprice AS total_sales FROM df limit 20").df()


      return df_transformado

   def salvar_no_postgres(self, df_duckdb, tabela):
      DATABASE_URL = os.getenv("DATABASE_URL") # EX: 'postgresql://user:password@localhost:5432/data'
      engine = create_engine(DATABASE_URL)
      # Salvar o DataFrame no PostgreSQL
      df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)

if __name__ == "__main__":
   pipeline = Pipeline()
   url_pasta = 'https://drive.google.com/drive/folders/1OQK3oxAfJWLw4s1-7VT7XleHbcgt-Lop'
   diretorio_local = '.\\pasta_gdown'

   baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
   pipeline.listar_arquivos(diretorio_local)
   dataframe_duckdb_csv = pipeline.ler_csv()
   dataframe_csv = pipeline.transformar(dataframe_duckdb_csv)
   pipeline.salvar_no_postgres(dataframe_csv, "sales_calculated")
   
   
