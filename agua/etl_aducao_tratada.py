import pandas as pd 
import psycopg2
from conexao import conexao

conn = psycopg2.connect(**conexao)
query = "SELECT * FROM public.aducao_ag_t_"

df = pd.read_sql_query(query, conn)
print(df.info())
df['CD_BAIRRO'] = df["CD_BAIRRO"].fillna("Sem infomações")
df['CD_SETOR'] = df["CD_SETOR"].fillna("Sem infomações")
df['CD_DIST'] = df["CD_DIST"].fillna("Sem infomações")
df = df.rename(columns={'0_municipi': 'municipio',  
                        '0_ano_ref': 'data_referencia', 'CD_SETOR': 'cod_setor', 'CD_MUN': 'cod_muni', 'CD_DIST':'cod_distrito', 'S_ABASTE': 'sistema_abastec',
                        'SUB_S_AB': 'subsist_abastec', 'CD_BAIRRO': 'cod_bairro'})
df['data_referencia'] = pd.to_datetime('01/' + '01' + '/' + df['data_referencia'].astype(str), format='%d/%m/%Y')
df['data_referencia'] = df['data_referencia'].dt.date
print(df.head())
# valores = df['cod_setor'].unique().tolist()
# print(valores)

conn.close()