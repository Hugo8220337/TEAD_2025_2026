# utils/clinical_info.py

DICIONARIO_EXAMES = {
    'hb': 'Hemoglobina (Transporte de Oxigénio/Perda de Sangue)',
    'plt': 'Plaquetas (Capacidade de Coagulação)',
    'na': 'Sódio (Equilíbrio de Líquidos e Pressão)',
    'k': 'Potássio (Função Muscular e Cardíaca)',
    'cl': 'Cloro (Equilíbrio Ácido-Base)',
    'gluc': 'Glicose (Nível de Açúcar/Stress Metabólico)',
    'alb': 'Albumina (Estado Nutricional e Proteico)',
    'creat': 'Creatinina (Função Renal/Rins)',
    'bun': 'Azoto Ureico (Função Renal/Rins)',
    'hct': 'Hematócrito (Concentração de Glóbulos Vermelhos)',
    'wbc': 'Glóbulos Brancos (Resposta do Sistema Imunitário)',
    'po2': 'Pressão Parcial de Oxigénio (Oxigenação no Sangue)',
    'alt': 'Alanina Aminotransferase (Função Hepática/Fígado)',
    'sao2': 'Saturação de Oxigénio (Eficácia Respiratória)',
    'ptsec': 'Tempo de Protrombina (Velocidade de Coagulação)',
    'hco3': 'Bicarbonato (Equilíbrio Ácido-Base)',
    'ptinr': 'Índice INR (Risco de Hemorragia)',
    'ica': 'Cálcio Ionizado (Função Cardíaca e Muscular)',
    'p': 'Fósforo (Energia Celular e Recuperação)',
    'tprot': 'Proteína Total (Estado Nutricional Global)',
    'ammo': 'Amónia (Toxicidade/Fadiga Hepática)',
    'ast': 'Aspartato Aminotransferase (Função Hepática/Fígado)',
    'gfr': 'Taxa de Filtração Glomerular (Eficácia Renal/Rins)',
    'tbil': 'Bilirrubina Total (Função Hepática/Vesícula)',
    'cr': 'Creatinina Sérica (Função Renal/Rins)',
    'esr': 'Velocidade de Sedimentação (Marcador de Inflamação)',
    'crp': 'Proteína C-Reativa (Marcador de Inflamação/Infeção)',
    'aptt': 'Tempo de Tromboplastina (Eficácia de Coagulação)',
    'pco2': 'Pressão Parcial de CO2 (Ventilação/Respiração)',
    'pt%': 'Atividade da Protrombina (Eficácia de Coagulação)',
    'fib': 'Fibrinogénio (Proteína de Coagulação)',
    'be': 'Excesso de Base (Equilíbrio Metabólico/Acidez)',
    'ccr': 'Clearance de Creatinina (Eficácia de Filtração Renal)',
    'lac': 'Lactato (Falta de Oxigénio nos Tecidos/Stress Celular)',
    'ph': 'pH (Nível de Acidez no Sangue)'
}

DICIONARIO_DEPARTAMENTOS = {
    'general surgery': 'Cirurgia Geral',
    'gynecology': 'Ginecologia',
    'thoracic surgery': 'Cirurgia Torácica',
    'urology': 'Urologia'
}

def get_nome_departamento(nome_ingles):
    if not isinstance(nome_ingles, str):
        return nome_ingles
    return DICIONARIO_DEPARTAMENTOS.get(nome_ingles.lower(), nome_ingles)

def get_nome_exame(sigla):
    return DICIONARIO_EXAMES.get(sigla.lower(), sigla.upper())