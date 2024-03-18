import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

from pandas import DataFrame
from duckdb import DuckDBPyRelation

class Pipeline:
   def __init__(self) -> None:
      """Carregar das variáveis de ambiente e inicialização dos atributos do objeto"""
      load_dotenv()
      self.arquivos_e_tipos = []

   def conectar_banco(self):
      """Conecta ao banco de dados DuckDB; cria o banco se não existir."""
      return duckdb.connect(database='duckdb.db', read_only=False)
   
   def inicializar_tabela(self,con):
      """Cria a tabela se ela não existir."""
      con.execute("""
         CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP
         )
      """)
   
   def registrar_arquivo(self,con, nome_arquivo):
      """Registra um novo arquivo no banco de dados com o horário atual."""
      con.execute("""
         INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
         VALUES (?,?)
      """, (nome_arquivo, datetime.now()))

   def arquivos_processados(self,con):
      """Retorna um set com os nomes de todos os arquivos já processados."""
      return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())

   def baixar_os_arquivos_do_google_drive(self, url_pasta:str, diretorio_local:str):
      """Busca no link passado como parâmetro, baixa todos os arquivos e salva em um diretório especificado"""
      os.makedirs(diretorio_local, exist_ok=True)
      gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

   
   def listar_arquivos(self, diretorio:str):
      """Função para listar arquivos CSV no diretório especificado e guardar caminho e tipo"""
      todos_arquivos = os.listdir(diretorio)
      todos_arquivos = list(filter(lambda x:os.path.isfile(f"{diretorio}\\"+x), todos_arquivos))

      for arquivo in todos_arquivos:
         if arquivo.endswith(".csv") or arquivo.endswith(".json") or arquivo.endswith(".parquet"):
               caminho_completo = os.path.join(diretorio, arquivo)
               tipo = arquivo.split(".")[-1]
               self.arquivos_e_tipos.append((caminho_completo, tipo))
      return self.arquivos_e_tipos
         
   def ler_csv(self,caminho_do_arquivo, tipo): 
      """Função para ler um arquivos(CSV ou parquet ou json) e retornar um DataFrame duckdb"""

      if tipo == 'csv':
         return duckdb.read_csv(caminho_do_arquivo)
      elif tipo == 'json':
         return pd.read_json(caminho_do_arquivo)
      elif tipo == 'parquet':
         return pd.read_parquet(caminho_do_arquivo)
      else:
         raise ValueError(f"tipo de arquivo não suportado: {tipo}")
      

   
   
   def transformar(self, df:DuckDBPyRelation) -> DataFrame:
      """Função para adicionar a coluna de total de vendas"""
      df_transformado = duckdb.sql("SELECT *, quantity * unitprice AS total_sales FROM df limit 8").df()
      return df_transformado

   def salvar_no_postgres(self, df_duckdb, tabela):
      """Função para salvar no DB Postgres"""
      DATABASE_URL = os.getenv("DATABASE_URL") # EX: 'postgresql://user:password@localhost:5432/data'
      engine = create_engine(DATABASE_URL)
      # Salvar o DataFrame no PostgreSQL
      df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)

   def pipeline(self):
      """ MAIN, executa toda a pipeline """
      url_pasta = 'https://drive.google.com/drive/folders/1OQK3oxAfJWLw4s1-7VT7XleHbcgt-Lop'
      diretorio_local = '.\\pasta_gdown'

      self.baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
      self.listar_arquivos(diretorio_local)
      con = self.conectar_banco()
      self.inicializar_tabela(con)
      processados = self.arquivos_processados(con)
      logs = []
      for caminho_do_arquivo, tipo in self.arquivos_e_tipos:
         nome_arquivo = os.path.basename(caminho_do_arquivo)
         if nome_arquivo not in processados:
            df_duckdb = self.ler_csv(caminho_do_arquivo, tipo)
            df_transformado = self.transformar(df_duckdb)
            self.salvar_no_postgres(df_transformado, "sales_calculated")
            self.registrar_arquivo(con, nome_arquivo)
            logs.append(f"arquivo {nome_arquivo} processado e salvo")
         else: 
            logs.append(f"arquivo {nome_arquivo} Já foi processado e salvo anteriormente")
      for log in logs:
         print(log)
      return logs
      

if __name__ == "__main__":
   pipeline = Pipeline()
   pipeline.pipeline()
   
