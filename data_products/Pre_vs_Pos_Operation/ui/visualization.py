import plotly.express as px
import plotly.graph_objects as go
from helpers.clinical_info import get_nome_exame

COLOR_PRE, COLOR_POS = "#00E5FF", "#FF3366" # Cyan e Rosa Néon

def apply_dark_theme(fig, height=500):
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)', 
        font=dict(color="#E2E8F0"), 
        height=height, 
        margin=dict(l=10, r=10, t=40, b=10)
    )
    return fig

def plot_visao_global_lollipop(df):
    """Gráfico Lollipop ordenado com siglas no eixo e explicação no hover."""
    df_avg = df.groupby('lab_test')['absolute_delta'].mean().reset_index()
    
    # Criar a sigla e a descrição
    df_avg['sigla'] = df_avg['lab_test'].str.upper()
    df_avg['descricao'] = df_avg['lab_test'].apply(get_nome_exame)
    
    # Ordenar do mais negativo para o mais positivo
    df_avg = df_avg.sort_values('absolute_delta', ascending=True)
    
    fig = go.Figure()
    
    # Hastes (linhas)
    for i, row in df_avg.iterrows():
        fig.add_shape(type="line", x0=0, y0=row['sigla'], x1=row['absolute_delta'], y1=row['sigla'], line=dict(color="#3A3F48", width=2))
        
    # Bolas e Hover customizado
    cores = [COLOR_POS if x < 0 else COLOR_PRE for x in df_avg['absolute_delta']]
    
    # Construir o texto que aparece quando passas o rato
    hover_text = df_avg['sigla'] + " - " + df_avg['descricao'] + "<br>Impacto Médio: <b>" + df_avg['absolute_delta'].round(2).astype(str) + "</b>"
    
    fig.add_trace(go.Scatter(
        x=df_avg['absolute_delta'],
        y=df_avg['sigla'], 
        mode='markers',
        marker=dict(size=12, color=cores, line=dict(width=1, color='#FFFFFF')),
        text=hover_text,
        hoverinfo='text',
        name=''
    ))

    fig = apply_dark_theme(fig, height=650)
    fig.update_layout(
        showlegend=False,
        xaxis=dict(title="← Depleção Fisiológica (Perda) | Acréscimo (Ganho) →", showgrid=True, gridcolor='#2D3139', zeroline=True, zerolinecolor='#64748B'),
        yaxis=dict(title="Indicador (Sigla)", showgrid=False)
    )
    return fig

def plot_genero_siglas_hover(df, exame):
    """Gráfico de barras de género com siglas no eixo e hover explicativo."""
    group_col = 'department' if exame != "Todos" else 'lab_test'
    df_sex = df.groupby([group_col, 'sex'])['absolute_delta'].mean().reset_index()
    
    if group_col == 'lab_test':
        df_sex['Eixo_X'] = df_sex['lab_test'].str.upper()
        df_sex['Descricao'] = df_sex['lab_test'].apply(get_nome_exame)
        
        fig = px.bar(
            df_sex, 
            x='Eixo_X', 
            y='absolute_delta', 
            color='sex', 
            barmode='group',
            color_discrete_map={'M': COLOR_PRE, 'F': '#B388FF'},
            hover_data={'Descricao': True} 
        )
        
        # O %{data.name} vai buscar automaticamente o nome da cor (Sexo: M ou F)
        # O %{customdata[0]} vai buscar a Descricao processada pelo hover_data
        fig.update_traces(
            hovertemplate="<b>%{x}</b> - %{customdata[0]}<br>Sexo: %{data.name}<br>Impacto Médio: <b>%{y:.2f}</b><extra></extra>"
        )
        
    else:
        df_sex['Eixo_X'] = df_sex['department']
        
        fig = px.bar(
            df_sex, 
            x='Eixo_X', 
            y='absolute_delta', 
            color='sex', 
            barmode='group',
            color_discrete_map={'M': COLOR_PRE, 'F': '#B388FF'}
        )
        
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>Sexo: %{data.name}<br>Impacto Médio: <b>%{y:.2f}</b><extra></extra>"
        )
        
    fig.update_layout(xaxis_title="Indicador (Sigla)" if exame == "Todos" else "Especialidade", yaxis_title="Variação Média")
    return apply_dark_theme(fig)

def plot_antes_depois_dumbbell(df, exame):
    """Gráfico Dumbbell para análise de variação de um único exame."""
    df_agg = df.groupby('department')[['pre_op_avg', 'pos_op_avg']].mean().reset_index().sort_values('pre_op_avg')
    fig = go.Figure()
    for _, row in df_agg.iterrows():
        fig.add_shape(type="line", x0=row['pre_op_avg'], y0=row['department'], x1=row['pos_op_avg'], y1=row['department'], line=dict(color="#3A3F48", width=3))
    fig.add_trace(go.Scatter(x=df_agg['pre_op_avg'], y=df_agg['department'], mode='markers', name='Entrada (Pré)', marker=dict(color=COLOR_PRE, size=14)))
    fig.add_trace(go.Scatter(x=df_agg['pos_op_avg'], y=df_agg['department'], mode='markers', name='Saída (Pós)', marker=dict(color=COLOR_POS, size=14)))
    fig.update_layout(xaxis_title=f"Concentração ({get_nome_exame(exame).split('(')[0].strip()})", yaxis_title="")
    return apply_dark_theme(fig)