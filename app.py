import streamlit as st
from pipeline import Pipeline

st.title('Procesador de dados')

def main():
   pipeline = Pipeline()
   if st.button('processar'):
      with st.spinner('Processando...'):
         logs = pipeline.pipeline()
         for log in logs:
            st.write(log)

if __name__ == "__main__":
   main()
   

   
