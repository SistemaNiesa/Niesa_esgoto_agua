import numpy as np
import pandas as pd 
import psycopg2
from conexao import conexao

conn = psycopg2.connect(**conexao)
query = "SELECT * FROM public.tratamento_agua_"

df = pd.read_sql_query(query, conn)
print(df.info())
df['CD_BAIRRO'] = df["CD_BAIRRO"].fillna("SEM INFORMAÇÕES")
df['CD_SETOR'] = df["CD_SETOR"].fillna("SEM INFORMAÇÕES")
df['CD_DIST'] = df["CD_DIST"].fillna("SEM INFORMAÇÕES")
df['SUB_S_AB'] = df["SUB_S_AB"].fillna("SEM INFORMAÇÕES")
df['S_ABASTE'] = df["S_ABASTE"].fillna("SEM INFORMAÇÕES")
df['S_ESGO'] = df["S_ESGO"].fillna("SEM INFORMAÇÕES")
df = df.rename(columns={'0_municipi': 'municipio',  
                        '0_ano_refe': 'data_referencia', 'CD_SETOR': 'cod_setor', 'CD_MUN': 'cod_muni', 'CD_DIST':'cod_distrito', 'S_ABASTE': 'sistema_abastec',
                        'SUB_S_AB': 'subsist_abastec', 'CD_BAIRRO': 'cod_bairro', '4_tipo_est': "tipo_estacao", "5_tipo_tra": "tipo_tratamento",
                        "6_coagulan": "coagulante", "7_tipo_des": "tipo_des", "8_polimero": "polimero", "9_utiliza_": "utiliza", "9_cal_util": "cal_util",
                        "10_fluoret": "fluoret", "13_vazao_a": "vazao_a", "S_ESGO": "sistema_esgoto", "situacao_e": "dsc_situacao", "problemas_":"dsc_problemas"})
df['data_referencia'] = df['data_referencia'].replace(0, np.nan)
df['data_referencia'] = '01/01/' + df['data_referencia'].astype('Int64').astype(str)
df['data_referencia'] = pd.to_datetime(df['data_referencia'], format='%d/%m/%Y', errors='coerce')
df['data_referencia'] = df['data_referencia'].dt.date

def limpar_e_maiusculo(texto):
    if isinstance(texto, str):
        if isinstance(texto, str) and texto.startswith('OUTRA. QUAL,'):
            texto = texto.replace('OUTRA. QUAL,', '').strip()
        if isinstance(texto, str) and texto.startswith('SIMPLIFICADO. QUAL?,'):
            texto = texto.replace('SIMPLIFICADO. QUAL,', '').strip()
        if isinstance(texto, str) and texto.startswith('SIMPLIFICADO, QUAL,'):
            texto = texto.replace('SIMPLIFICADO, QUAL,', '').strip()
        if isinstance(texto, str) and texto.startswith('OUTRO,'):
            texto = texto.replace('OUTRO,', '').strip()
    return texto.upper()


df['tipo_estacao'] = df['tipo_estacao'].fillna("SEM INFORMAÇÕES")
df['tipo_estacao'] = df['tipo_estacao'].apply(limpar_e_maiusculo)

df['tipo_tratamento'] = df['tipo_tratamento'].fillna("SEM INFORMAÇÕES")
df['tipo_tratamento'] = df['tipo_tratamento'].apply(limpar_e_maiusculo)

df['coagulante'] = df['coagulante'].fillna("SEM INFORMAÇÕES")
df['coagulante'] = df['coagulante'].apply(limpar_e_maiusculo)


df['tipo_des'] = df['tipo_des'].fillna("SEM INFORMAÇÕES")
df['polimero'] = df['polimero'].fillna("SEM INFORMAÇÕES")
df['utiliza'] = df['utiliza'].fillna("SEM INFORMAÇÕES")
df['cal_util'] = df['cal_util'].fillna("SEM INFORMAÇÕES")
df['fluoret'] = df['fluoret'].fillna("SEM INFORMAÇÕES")
# df['11_capacid'] = df['11_capacid'].fillna("SEM INFORMAÇÕES")
# df['12_capacid'] = df['12_capacid'].fillna("SEM INFORMAÇÕES")
# df['vazao_a'] = df['vazao_a'].fillna("SEM INFORMAÇÕES")
# df['14_capacid'] = df['14_capacid'].fillna("SEM INFORMAÇÕES")


print(df.head())
valores = df['vazao_a'].unique().tolist()
print(valores)

conn.close()