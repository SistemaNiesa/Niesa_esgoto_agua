import re
import numpy as np
import pandas as pd 
import psycopg2
from conexao import conexao

conn = psycopg2.connect(**conexao)
query = "SELECT * FROM public.reservacao_agua_"

df = pd.read_sql_query(query, conn)
print(df.info())
df['CD_BAIRRO'] = df["CD_BAIRRO"].fillna("INFORMAÇÃO AUSENTE")
df['S_ABASTE'] = df["S_ABASTE"].fillna("INFORMAÇÃO AUSENTE")
df['SUB_S_AB'] = df["SUB_S_AB"].fillna("INFORMAÇÃO AUSENTE")
df['S_ESGO'] = df["S_ESGO"].fillna("INFORMAÇÃO AUSENTE")
df['S_ESGO'] = df["S_ESGO"].fillna("INFORMAÇÃO AUSENTE")
df = df.rename(columns={'0_municipi': 'municipio', '1_n_econom': 'numero_econom', 
                        '0_ano_refe': 'data_referencia', 'CD_SETOR': 'cod_setor', 'CD_MUN': 'cod_muni', 'CD_DIST':'cod_distrito', 'S_ABASTE': 'sistema_abastec',
                        'S_ESGO': 'sistema_esgoto', 'SUB_S_AB': 'subsist_abastec', 'CD_BAIRRO': 'cod_bairro', '3_tipo_res': 'tipo_reservatorio',
                        '4_material': 'material', '5_capacida': 'capacidade', '7_situacao': 'situacao', 'problemas_': 'dsc_problema', '8_bairros_': 'bairros'})

df['data_referencia'] = df['data_referencia'].replace(0, np.nan)
df['data_referencia'] = '01/01/' + df['data_referencia'].astype('Int64').astype(str)
df['data_referencia'] = pd.to_datetime(df['data_referencia'], format='%d/%m/%Y', errors='coerce')
df['data_referencia'] = df['data_referencia'].dt.date
df['situacao'] = df['situacao'].fillna("INFORMAÇÃO AUSENTE")

def limpar_e_maiusculo(texto):
    if isinstance(texto, str):
        if isinstance(texto, str) and texto.startswith('OUTRO. QUAL?,'):
            texto = texto.replace('OUTRO. QUAL?,', '').strip()
        if texto.upper().startswith('SEMI') and not texto.upper().startswith('SEMI-'):
            texto = re.sub(r'^SEMI[\s-]*', 'SEMI-', texto, flags=re.IGNORECASE)
    return texto.upper()

df['tipo_reservatorio'] = df['tipo_reservatorio'].fillna("Sem informações")
df['tipo_reservatorio'] = df['tipo_reservatorio'].apply(limpar_e_maiusculo)

df['material'] = df['material'].fillna("Sem informações")
df['material'] = df['material'].apply(limpar_e_maiusculo)

df['dsc_problema'] = df['dsc_problema'].fillna("Sem informações")
df['dsc_problema'] = df['dsc_problema'].apply(lambda x: x.upper() if isinstance(x, str) else x)

def limpar_localidade(texto):
    if not isinstance(texto, str):
        return texto
    texto = texto.upper()

    
    texto = re.sub(r'\s+', ' ', texto).strip()

    texto = re.sub(r'[.;,]+$', '', texto)

    if texto in [
        'CIDADE TODA', 'TODA A CIDADE', 'A CIDADE TODA', 'TODO MUNICÍPIO', 'TODA A SEDE URBANA',
        'TODA SEDE URBANA', 'TODA ÁREA URBANA', 'TODA A REDE URBANA', 'ABASTECE TODA A ÁREA URBANA OU O RESERVATÓRIO 06',
        'RESERVATÓRIO ABASTECE A REDE DE TODA A CIDADE', 'O RESERVATÓRIO ABASTECE A REDE TODA DA CIDADE',
        'O RESERVATÓRIO ABASTECE A REDE DE TODA A CIDADE', 'ABASTECE TODA A SEDE DO MUNICÍPIO'
    ]:
        return 'TODA A CIDADE'

    if any(palavra in texto for palavra in ['NÃO', 'SEM ABASTECIMENTO', 'NENHUM']):
        return 'SEM ABASTECIMENTO'

    if texto in ['SEM INFORMAÇÕES', 'NÃO INFORMADO']:
        return 'INFORMAÇÃO AUSENTE'

    return texto
df['bairros'] = df['bairros'].fillna("Sem informações")
df['bairros'] = df['bairros'].apply(limpar_localidade)
valores = df['bairros'].unique().tolist()
print(valores)
df.to_excel("teste.xlsx")

conn.close()