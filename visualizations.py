import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

def plot_bar_chart(df_bar_series: pd.DataFrame, last_date) -> None:
    """Cria um gráfico de barras para o último mês."""
    fig, ax = plt.subplots(figsize=(8, 6))
    df_bar_series.plot(kind="bar", ax=ax)
    ax.set_xlabel("UF")
    ax.set_ylabel("Quantidade de cotações")
    ax.set_title(f"Última Data: {last_date.strftime('%m/%Y')}")
    st.pyplot(fig)

def plot_time_series(df_pivot: pd.DataFrame) -> None:
    """Cria um gráfico de série histórica para os itens selecionados."""
    fig, ax = plt.subplots(figsize=(10, 6))
    for col in df_pivot.columns:
        ax.plot(df_pivot.index, df_pivot[col], marker="o", label=f"{col[0]} - {col[1]}")
    ax.set_xlabel("Data")
    ax.set_ylabel("Quantidade de Cotações")
    ax.set_title("Série Histórica")
    ax.axhline(y=25, color='#ff4d4d', linestyle='--', label="Limite Crítico (<25)")
    ax.axhline(y=50, color='#ffa500', linestyle='--', label="Limite Mediano (<50)")
    ax.axhline(y=75, color='#FCDA51', linestyle='--', label="Limite Relevante (<75)")
    ax.axhline(y=100, color='#66cc66', linestyle='--', label="Limite Aceitável (<100)")
    ax.legend(title="Região - Produto", loc="best")
    st.pyplot(fig)
