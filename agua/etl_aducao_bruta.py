import pandas as pd 
import psycopg2
from conexao import conexao

conn = psycopg2.connect(**conexao)
query = "SELECT * FROM public.aducao_ag_b_"

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

df = pd.read_sql_query(query, conn)
print(df.info())


df['CD_BAIRRO'] = df["CD_BAIRRO"].fillna("SEM INFORMAÇÕES")
df['CD_MUN'] = df["CD_MUN"].fillna("SEM INFORMAÇÕES")
df['CD_SETOR'] = df["CD_SETOR"].fillna("SEM INFORMAÇÕES")
df['CD_DIST'] = df["CD_DIST"].fillna("SEM INFORMAÇÕES")
df = df.rename(columns={'0_municipi': 'municipio',  
                        '0_ano_ref': 'data_referencia', 'CD_SETOR': 'cod_setor', 'CD_MUN': 'cod_muni', 'CD_DIST':'cod_distrito',
                        'S_ABASTE': 'sistema_abastec','SUB_S_AB': 'subsist_abastec', 'CD_BAIRRO': 'cod_bairro', 
                        '1_caracter': "caracteristica", "2_sistema": "sistema", "3_situacao": "situacao"})
df['data_referencia'] = pd.to_datetime('01/' + '01' + '/' + df['data_referencia'].astype(str), format='%d/%m/%Y')
df['data_referencia'] = df['data_referencia'].dt.date

df['municipio'] = df['municipio'].apply(limpar_e_maiusculo)

df['caracteristica'] = df['caracteristica'].fillna("SEM INFORMAÇÕES")
df['caracteristica'] = df['caracteristica'].apply(limpar_e_maiusculo)

df['sistema'] = df['sistema'].fillna("SEM INFORMAÇÕES")
df['sistema'] = df['sistema'].apply(limpar_e_maiusculo)
print(df.head())
valores = df['sistema'].unique().tolist()
print(valores)

conn.close()