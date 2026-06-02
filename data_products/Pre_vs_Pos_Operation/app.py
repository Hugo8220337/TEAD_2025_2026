import streamlit as st
import os, sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from helpers.data_loader import load_gold_data, get_filtered_data, get_kpis
from helpers.clinical_info import get_nome_exame, get_nome_departamento
from ui.visualization import plot_visao_global_lollipop, plot_antes_depois_dumbbell, plot_genero_siglas_hover

# Configuração e CSS Dark Mode
st.set_page_config(page_title="Impacto Cirúrgico", layout="wide", page_icon="🩺", initial_sidebar_state="collapsed")
st.markdown("<style>div[data-testid='metric-container'] { background-color: #161A23; border: 1px solid #2A2D35; padding: 20px; border-radius: 12px; border-left: 4px solid #00E5FF; }</style>", unsafe_allow_html=True)

# 1. Carregar Dados
df_gold = load_gold_data()
df_gold['department'] = df_gold['department'].apply(get_nome_departamento)

st.title("Painel Clínico: Avaliação de Impacto Cirúrgico")

# 2. Filtros no Topo
col_f1, col_f2 = st.columns(2)
dept_sel = col_f1.selectbox("Especialidade Médica:", ["Todos"] + sorted(df_gold['department'].unique().tolist()))
exame_sel = col_f2.selectbox("Indicador Clínico:", ["Todos"] + sorted(df_gold['lab_test'].unique().tolist()), 
                             format_func=lambda x: get_nome_exame(x) if x != "Todos" else "Visão Global (Heatmap)")

df_f = get_filtered_data(df_gold, dept_sel, exame_sel)

# 3. KPIs
kpis = get_kpis(df_f)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Pacientes", kpis['pacientes'])
c2.metric("Idade Média", f"{kpis['idade']} anos")
c3.metric("Estadia UCI", f"{kpis['uci']} dias")
c4.metric("Indicadores", kpis['exames'])

# 4. Visualizações Principais
if exame_sel == "Todos":
    st.markdown("### Análise Sistémica: Impacto Fisiológico Global")
    st.info("**Contexto Analítico:** Desvios para a esquerda (Rosa) sinalizam depleção ou perda induzida pelo stress cirúrgico.")
    
    # Chama a função do Lollipop em vez do heatmap
    st.plotly_chart(plot_visao_global_lollipop(df_f), use_container_width=True) 
else:
    st.markdown(f"###Variação Entrada vs Saída ({get_nome_exame(exame_sel)})")
    st.plotly_chart(plot_antes_depois_dumbbell(df_f, exame_sel), use_container_width=True)

# 5. Análise de Género
st.divider()
st.markdown("### Diferenciação de Resposta por Género")
st.plotly_chart(plot_genero_siglas_hover(df_f, exame_sel), use_container_width=True)