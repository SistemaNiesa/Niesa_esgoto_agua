import pandas as pd 
import psycopg2
from conexao import conexao
import re

def limpar_e_maiusculo(texto):
    if isinstance(texto, str):
        # Remove padrões como: OUTRO, QUAL, / OUTRO. QUAL, / OUTRA. QUAL, etc.
        texto = re.sub(r'^\s*(OUTRO|OUTRA|SIMPLIFICADO)?[.,]?\s*QUAL[?,]?\s*,?', '', texto, flags=re.IGNORECASE).strip()
        return texto.upper()
    return texto

def limpar_outro(texto):
    if isinstance(texto, str):
        # Remove apenas "OUTRO," ou "OUTRO ," com ou sem espaço
        texto = re.sub(r'^\s*OUTRO[.,]?\s*,?\s*', '', texto, flags=re.IGNORECASE).strip()
        return texto.upper()
    return texto

conn = psycopg2.connect(**conexao)
query = "SELECT * FROM public.captacao_superficial_"

df = pd.read_sql_query(query, conn)
print(df.info())
df['CD_BAIRRO'] = df["CD_BAIRRO"].fillna("SEM INFORMAÇÕES")
df['S_ABASTE'] = df["S_ABASTE"].fillna("SEM INFORMAÇÕES")
df['SUB_S_AB'] = df["SUB_S_AB"].fillna("SEM INFORMAÇÕES")
df['S_ESGO'] = df["S_ESGO"].fillna("SEM INFORMAÇÕES")
df = df.rename(columns={'0_municipi': 'municipio', '1_denomina': 'denomina', "3_fonte": "fonte_origem", "4_existenc": "existenc", "6_tipo_cap": "tipo_cap",
                        '0_ano_refe': 'data_referencia', 'CD_SETOR': 'cod_setor', 'CD_MUN': 'cod_muni', 'CD_DIST':'cod_distrito', 'S_ABASTE': 'sistema_abastec',
                        'S_ESGO': 'sistema_esgoto', 'SUB_S_AB': 'subsist_abastec', 'CD_BAIRRO': 'cod_bairro', '7_tipo_bom': "tipo_bom",
                        "8_marca": "marca", "8_modelo": "modelo", "10_manuten": "manutencao", "11_bomba_r": "bomba_r", "12_captaca": "captacao",
                        "14_desinfe": "desinfe", "14_cloraca": "cloraca", '15_data_em': "data_em", "15_data_va": "data_va",
                        "15_observa": "observacao", "16_problem": "problemas"})
df['data_referencia'] = pd.to_datetime('01/' + '01' + '/' + df['data_referencia'].astype(str), format='%d/%m/%Y')
df['data_referencia'] = df['data_referencia'].dt.date

df['denomina'] = df['denomina'].fillna("SEM INFORMAÇÕES")
df['fonte_origem'] = df['fonte_origem'].fillna("SEM INFORMAÇÕES")
df['existenc'] = df['existenc'].fillna("SEM INFORMAÇÕES")
df['tipo_cap'] = df['tipo_cap'].fillna("SEM INFORMAÇÕES")
df['tipo_cap'] = df['tipo_cap'].apply(limpar_e_maiusculo)

df['tipo_bom'] = df['tipo_bom'].fillna("SEM INFORMAÇÕES")
df['marca'] = df['marca'].fillna("SEM INFORMAÇÕES")
df['modelo'] = df['modelo'].fillna("SEM INFORMAÇÕES")
df['manutencao'] = df['manutencao'].fillna("SEM INFORMAÇÕES")
df['manutencao'] = df['manutencao'].apply(limpar_outro)
df['bomba_r'] = df['bomba_r'].fillna("SEM INFORMAÇÕES")
df['captacao'] = df['captacao'].fillna("SEM INFORMAÇÕES")
df['desinfe'] = df['desinfe'].fillna("SEM INFORMAÇÕES")
df['cloraca'] = df['cloraca'].fillna("SEM INFORMAÇÕES")
df['observacao'] = df['observacao'].fillna("SEM OBSERVAÇÕES")
df['observacao'] = df['observacao'].apply(limpar_e_maiusculo)
df['problemas'] = df['problemas'].apply(limpar_e_maiusculo)
df['problemas'] = df['problemas'].fillna("SEM PROBLEMAS LISTADOS")

df['data_em'] = pd.to_datetime(df['data_em'], errors='coerce').dt.date
df['data_va'] = pd.to_datetime(df['data_va'], errors='coerce').dt.date

print(df.head())

valores = df['problemas'].unique().tolist()
print(valores)

df.to_excel("teste.xlsx")

conn.close()