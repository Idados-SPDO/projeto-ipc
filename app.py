import streamlit as st
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import datetime
import re

from config import *
from utils import to_excel, highlight_values, get_criticidade
from data_processing import read_excel_file, read_excel_excess_file
from visualizations import plot_bar_chart, plot_time_series
from data_update import atualizar_base_incremental


def load_database(df: pd.DataFrame, table_name: str, con):
    """Registra o DataFrame no DuckDB e cria a tabela."""
    con.register("df_excel", df)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_excel")

def create_legend():
    legend_markdown = """
    <style>
        .legend-table {
            width: 100%;
            border-collapse: collapse;
        }
        .legend-table th, .legend-table td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }
        .legend-table th {
            background-color: #f4f4f4;
        }
        .super-critico { background-color: #ff4d4d; color: white; }
        .critico { background-color: #ffa500; color: white; }
        .aceitavel { background-color: #FCDA51; color: black; }
        .suficiente { background-color: #66cc66; color: white; }
        .excessao  { background-color: gray; color: white; }
    </style>
    <table class="legend-table">
        <tr>
            <th>Categoria</th>
            <th>Descrição</th>
            <th>Cor</th>
        </tr>
        <tr>
            <td>SuperCrítico</td>
            <td>Qtd de cotações abaixo ou igual a 25</td>
            <td class="super-critico"></td>
        </tr>
        <tr>
            <td>Crítico</td>
            <td>Qtd de cotações entre 26 e 55</td>
            <td class="critico"></td>
        </tr>
        <tr>
            <td>Aceitável</td>
            <td>Qtd de cotações entre 56 e 100</td>
            <td class="aceitavel"></td>
        </tr>
        <tr>
            <td>Suficiente</td>
            <td>Qtd de cotações acima de 100</td>
            <td class="suficiente"></td>
        </tr>
        <tr>
            <td>Excessão</td>
            <td>Itens que não entram no cálculo do IPC</td>
            <td class="excessao"></td>
        </tr>
    </table>
    """
    st.sidebar.markdown(legend_markdown, unsafe_allow_html=True)

def main():
    st.title("Leitor de Controle de Cotações - IPC")
    st.text("Esta aplicação foi desenvolvida para facilitar a consulta do número de cotações por subitem e por UF, permitindo uma análise mais detalhada e eficiente das informações disponíveis.")
    st.text("Caso seja necessária uma atualização, carregue o arquivo em formato Excel contendo o controle de cotações e as exceções na barra lateral esquerda.")
    st.text("As abas abaixo facilitam a navegação, permitindo que o usuário acesse as informações de acordo com sua necessidade. A aba 'Visão Geral' exibe a quantidade de itens cotados para o IPC e identifica seu status, informando se é necessário abrir um pedido ou não.")
    st.text("A aba 'Visão Gráfica' apresenta duas perspectivas: 'Visão do Último Mês' e 'Série Histórica'. A 'Visão do Último Mês' exibe um gráfico de barras referente ao último mês, permitindo a seleção da UF e do item desejado para filtragem. Já a 'Série Histórica' possibilita a seleção da UF e do item, oferecendo uma visão mais ampla da evolução da quantidade de cotações a partir de 2024.")
    st.text("Por fim, a aba 'Pesquisa e Detalhamento' permite a aplicação de filtros específicos na base geral, possibilitando a filtragem por data, item, criticidade e UF.")
    uploaded_file = st.sidebar.file_uploader("Atualize sua Base de Cotações:", type=["xls", "xlsx"])
    uploaded_excess_file = st.sidebar.file_uploader("Atualize sua Base de Excessões:", type=["xls", "xlsx"])
    
    create_legend()
    
    tab1, tab2, tab3 = st.tabs(["Visão Geral", "Visão Gráfica", "Pesquisa e Detalhamento"])
    
    db_path = "ipc.db"
    table_name = "controle_cotacoes"
    excess_table_name = "excessoes"
    con = duckdb.connect(db_path)
    
    if uploaded_file is not None:
        df_novo = read_excel_file(uploaded_file)
        
        try:
            query = f"SELECT * FROM {table_name}"
            df_atual = con.execute(query).fetchdf()
        except Exception:
            #df_atual = pd.DataFrame()  
            pass

        if not df_atual.empty:
            df_atualizada = atualizar_base_incremental(df_atual, df_novo)
        else:
            df_atualizada = df_novo
        
        load_database(df_atualizada, table_name, con)

    
    if uploaded_excess_file is not None:
        df_excess_excel = read_excel_excess_file(uploaded_excess_file)
        con.register("df_excess_excel", df_excess_excel)
        con.execute(f"DROP TABLE IF EXISTS {excess_table_name}")
        con.execute(f"CREATE TABLE {excess_table_name} AS SELECT * FROM df_excess_excel")
    
    query = f"SELECT * FROM {table_name}"
    excess_query = f"SELECT * FROM {excess_table_name}"
    df = con.execute(query).fetchdf()
    df_excess = con.execute(excess_query).fetchdf()
    
    colunas_datas = df.columns[3:]
    df_br = df.groupby(["Código", "Descrição"], as_index=False)[colunas_datas].sum()
    df_br["UF"] = "BR"
    df_br = df_br[["UF", "Código", "Descrição"] + list(colunas_datas)]
    df = pd.concat([df, df_br], ignore_index=True)
    
    df_melted = df.melt(
        id_vars=["UF", "Código", "Descrição"],
        value_vars=colunas_datas,
        var_name="Data",
        value_name="Valor"
    )
    df_melted["Valor"] = pd.to_numeric(df_melted["Valor"], errors="coerce")
    df_melted["CodigoDescricao"] = df_melted["Código"].astype(str) + " - " + df_melted["Descrição"].astype(str)
    if not df_excess.empty:
        excess_set = set(df_excess["DESCRIÇÃO"].dropna())
        df_melted["Exceção"] = df_melted["Descrição"].apply(lambda x: x in excess_set)
    else:
        df_melted["Exceção"] = False

    df_totais = df_melted[~df_melted["Exceção"]].groupby(["UF", "Data"])["Valor"].count().rename("Total")
    df_super_critico = df_melted[(~df_melted["Exceção"]) & (df_melted["Valor"] <= 25)]\
        .groupby(["UF", "Data"])["Valor"].count().rename("SuperCrítico")
    df_critico = df_melted[(~df_melted["Exceção"]) & (df_melted["Valor"] >= 26) & (df_melted["Valor"] <= 55)]\
        .groupby(["UF", "Data"])["Valor"].count().rename("Crítico")
    df_aceitavel = df_melted[(~df_melted["Exceção"]) & (df_melted["Valor"] >= 56) & (df_melted["Valor"] <= 100)]\
        .groupby(["UF", "Data"])["Valor"].count().rename("Aceitável")
    df_suficiente = df_melted[(~df_melted["Exceção"]) & (df_melted["Valor"] > 100)]\
        .groupby(["UF", "Data"])["Valor"].count().rename("Suficiente")
    df_excessao = df_melted[df_melted["Exceção"]].groupby(["UF", "Data"])["Valor"].count().rename("Excessão")
    
    df_comparativo = pd.concat(
        [df_totais, df_super_critico, df_critico, df_aceitavel, df_suficiente, df_excessao], axis=1
    ).fillna(0).reset_index()
    
    df_comparativo["Data_dt"] = pd.to_datetime(df_comparativo["Data"], format="%m/%Y", errors="coerce")
    max_date = df_comparativo["Data_dt"].max()
    df_comparativo_recent = df_comparativo[df_comparativo["Data_dt"] == max_date]
    
    with tab1:
        st.write("### Status da quantidade de cotações (Mês atual):")
        st.dataframe(df_comparativo_recent[["UF", "Data", "Total", "SuperCrítico", "Crítico", "Aceitável", "Suficiente", "Excessão"]])
        st.download_button(
            label="📥 Download do Visão Geral",
            data=to_excel(df_comparativo_recent, "Visão Geral"),
            file_name="visao_geral.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    with tab2:
        subtab1, subtab2 = st.tabs(["Visão do Último Mês", "Série Histórica"])
        
        with subtab1:
            df_bar = df.melt(
                id_vars=["UF", "Código", "Descrição"],
                value_vars=colunas_datas,
                var_name="Data",
                value_name="Valor"
            )
            df_bar["CodigoDescricao"] = df_bar["Código"].astype(str) + " - " + df_bar["Descrição"].astype(str)
            df_bar["Data_clean_dt"] = pd.to_datetime(df_bar["Data"].str.extract(r'(\d{2}/\d{4})')[0], format="%m/%Y")
            df_bar = df_bar.sort_values("Data_clean_dt")
            
            last_date = df_bar["Data_clean_dt"].max()
            df_bar_filtered = df_bar[df_bar["Data_clean_dt"] == last_date]
            
            unique_ufs = sorted(df_bar_filtered["UF"].unique())
            unique_ufs_sem_br = [uf for uf in unique_ufs if uf != "BR"]
            selected_ufs = st.multiselect("Selecione a UF/BR:", unique_ufs, default=unique_ufs_sem_br)
            unique_products = sorted(df_bar_filtered["CodigoDescricao"].unique())
            selected_product = st.selectbox("Selecione o item:", unique_products)
            
            df_bar_filtered = df_bar_filtered[
                (df_bar_filtered["CodigoDescricao"] == selected_product) &
                (df_bar_filtered["UF"].isin(selected_ufs))
            ]
            
            if not df_bar_filtered.empty:
                df_bar_series = df_bar_filtered.pivot_table(
                    index="UF",
                    values="Valor",
                    aggfunc="mean"
                )
                plot_bar_chart(df_bar_series, last_date)
            else:
                st.warning("Não há dados para o produto e UF(s) selecionados no último período.")
        
        with subtab2:
            df_series = df.melt(
                id_vars=["UF", "Código", "Descrição"],
                value_vars=colunas_datas,
                var_name="Data",
                value_name="Valor"
            )
            df_series["Valor"] = pd.to_numeric(df_series["Valor"], errors="coerce")
            df_series["Data_clean"] = pd.to_datetime(
                df_series["Data"].str.extract(r'(\d{2}/\d{4})')[0], format="%m/%Y"
            )
            df_series["CodigoDescricao"] = df_series["Código"].astype(str) + " - " + df_series["Descrição"].astype(str)
            
            capitais_series = sorted(df["UF"].unique())
            selected_capitais = st.multiselect("Selecione a UF/BR:", capitais_series)
            unique_items = sorted(df_series["CodigoDescricao"].unique())
            selected_items = st.multiselect("Selecione o item:", unique_items)
            
            if not selected_capitais or not selected_items:
                st.error("Por favor, selecione ao menos uma região e um item para visualizar a série histórica.")
            else:
                df_series_filtered = df_series[
                    (df_series["UF"].isin(selected_capitais)) &
                    (df_series["CodigoDescricao"].isin(selected_items))
                ]
                if not df_series_filtered.empty:
                    df_pivot = df_series_filtered.pivot_table(
                        index="Data_clean",
                        columns=["UF", "CodigoDescricao"],
                        values="Valor",
                        aggfunc="mean"
                    )
                    plot_time_series(df_pivot)
                else:
                    st.warning("Selecione pelo menos uma região e/ou item para visualizar a série histórica.")
    
    with tab3:
        df_tab3 = df.copy()
        if not df_excess.empty:
            excess_set = set(df_excess["DESCRIÇÃO"].dropna())
            df_tab3["Exceção"] = df_tab3["Descrição"].apply(lambda x: x in excess_set)
        else:
            df_tab3["Exceção"] = False
        
        capitais = df_tab3["UF"].unique()
        col1, col2, col3 = st.columns(3)
        input_capital = col1.multiselect("Selecione a UF/BR:", sorted(capitais), key="search_capital_or_national")
        if input_capital:
            df_filtrado = df_tab3[df_tab3["UF"].isin(input_capital)]
        else:
            df_filtrado = df_tab3.copy()
        
        df_filtrado["CodigoDescricao"] = df_filtrado["Código"].astype(str) + " - " + df_filtrado["Descrição"].astype(str)
        colunas_reordenadas = ["UF", "CodigoDescricao", "Descrição", "Exceção"] + [col for col in df_filtrado.columns if col not in ["UF", "CodigoDescricao", "Descrição", "Exceção"]]
        df_filtrado = df_filtrado[colunas_reordenadas]
        
        selected_codigo_descricao = col2.multiselect("Selecione os items:", df_filtrado["CodigoDescricao"].unique())
        if selected_codigo_descricao:
            df_filtrado = df_filtrado[df_filtrado["CodigoDescricao"].isin(selected_codigo_descricao)]
        
        colunas_datas = df_filtrado.columns[4:]
        selected_datas = col3.multiselect("Selecione as datas:", options=list(colunas_datas))
        
        if selected_datas:
            df_filtrado = df_filtrado[["UF", "CodigoDescricao", "Descrição", "Exceção"] + selected_datas]
            subset_datas = selected_datas
        else:
            subset_datas = list(colunas_datas)
        
        subset_datas = [col for col in subset_datas if col != "Código"]
        
        selected_criticidade = st.multiselect("Selecione a criticidade:", ["SuperCrítico", "Crítico", "Aceitável", "Suficiente", "Exceção"])
        if selected_criticidade:
            df_filtrado = df_filtrado[df_filtrado.apply(
                lambda row: (row["Exceção"] and "Exceção" in selected_criticidade) or 
                            (not row["Exceção"] and any(get_criticidade(row[col]) in selected_criticidade 
                                                    for col in subset_datas if pd.notnull(row[col]))),
                axis=1
            )]
        
        def style_date_columns(row):
            styles = {}
            for col in row.index:
                if col in subset_datas:
                    if row["Exceção"]:
                        styles[col] = "background-color: gray; color: white"
                    else:
                        styles[col] = highlight_values(row[col])
                else:
                    styles[col] = ""
            return pd.Series(styles)
        
        styled_df = df_filtrado.style.apply(style_date_columns, axis=1)
        columns_to_display = [col for col in df_filtrado.columns if col not in ["Descrição", "Exceção"]]
        
        st.write("### Controle de Cotações:")
        st.dataframe(styled_df, column_order=columns_to_display)
        st.download_button(
            label="📥 Download do Controle de Cotações",
            data=to_excel(df_filtrado.drop(columns=["Descrição", "Exceção"]), "Controle_Cotacoes"),
            file_name="controle_cotacoes.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if __name__ == "__main__":
    main()
