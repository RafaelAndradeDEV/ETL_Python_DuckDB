# Projeto: ETL_Python_DuckDB

![Ilustração e anotações sobre ETL do projeto](/imgs/ETL_do_projeto.png)

## Processos:
1. **Baixar arquivos do Google Drive**
2. **Listar arquivos no diretório**
3. **Checar tipo do arquivo**
    - **CSV:** Ler arquivo CSV
    - **JSON:** Ler arquivo JSON
    - **Parquet:** Ler arquivo Parquet
4. **Transformar em DataFrame**
5. **Checar se o arquivo foi processado**
    - Se sim, Fim do processamento do arquivo
    - Se não:
        - Salvar no PostgreSQL
        - Registrar arquivo como processado
        - Fim do processamento do arquivo


![Processo_ETL](/imgs/processo_ETL.png)

## Etapas para execução do código:
Clonagem do repositório:

```
$ git clone https://github.com/RafaelAndradeDEV/ETL_Python_DuckDB.git 
$ cd ETL_Ptyhon_DuckDB
 ```

#### Instalação do Pyenv externamente e poetry:
- Tutorial para pyenv: https://k0nze.dev/posts/install-pyenv-venv-vscode/

```
$ pip install poetry
$ pyenv install 3.12.1
$ pyenv local 3.12.1
$ poetry init
$ poetry env use 3.12.1
$ poetry shell
```
#### Instalando dependências: 
```
$ poetry add $(cat requirements.txt)
```
#### Ajustar a variável de ambiente para conexão com o banco Postgre, seguindo o exemplo em [exemplo.env](exemplo.env)
#### Executar no banco os scripts de criação das tabelas [Create_Tables.sql](PLQS_codes/Create_Tables.sql)
#### Rodando aplicação:
```
$ streamlit run app.py
```


## To do  
- [ ] Utilizar biblioteca oficial do google para baixar os arquivos
- [ ] Normalizar tabela e ajuste de tipos
- [ ] Time series analysis
- [ ] Desenvolvimento de modelo para predição da série temporal
