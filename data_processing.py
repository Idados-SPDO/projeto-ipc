import pandas as pd
import re
import datetime
import streamlit as st
from config import SHEET_NAMES

@st.cache_data
def read_excel_file(uploaded_file) -> pd.DataFrame:
    """Lê e processa o arquivo Excel principal com várias abas."""
    lista_dfs = []
    for sheet in SHEET_NAMES:
        df_sheet = pd.read_excel(
            uploaded_file,
            sheet_name=sheet,
            skiprows=6
        )
        df_sheet["Capital"] = sheet
        lista_dfs.append(df_sheet)
    
    df = pd.concat(lista_dfs, ignore_index=True)
    df = df.dropna(axis=1, how='all')
    df.rename(columns={'Capital': 'UF'}, inplace=True)
    df.columns = [re.sub(r'\s*\(Q.*\)', '', col) for col in df.columns]

    data_atual = datetime.datetime.now()
    cols_to_keep = []
    for col in df.columns:
        match = re.search(r'(\d{2}/\d{4})', col)
        if match:
            try:
                data_coluna = datetime.datetime.strptime(match.group(1), "%m/%Y")
                # Mantém colunas com ano a partir de 2024 e até a data atual
                if data_coluna.year >= 2024 and data_coluna <= data_atual:
                    cols_to_keep.append(col)
            except Exception:
                pass
        else:
            cols_to_keep.append(col)
    df = df[cols_to_keep]
    st.dataframe(df)
    return df

@st.cache_data
def read_excel_excess_file(upload_file) -> pd.DataFrame:
    """Lê e processa o arquivo Excel de excessões."""
    df_sheet = pd.read_excel(upload_file, sheet_name='itens com excessões')
    df_excecao = df_sheet[df_sheet["excessão"].notna()][["DESCRIÇÃO"]]
    return df_excecao

def atualizar_base_incremental(df_atual: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    colunas_datas_atual = df_atual.columns[3:]
    colunas_datas_new = new_df.columns[3:]
    
    novas_colunas = [col for col in colunas_datas_new if col not in colunas_datas_atual]
    
    if novas_colunas:
        chaves = ["UF", "Código", "Descrição"]
        df_atual = df_atual.set_index(chaves)
        new_data = new_df.set_index(chaves)[novas_colunas]
        
        df_atual = df_atual.join(new_data, how="left")
        df_atual = df_atual.reset_index()
    
    return df_atual
