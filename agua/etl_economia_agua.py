import pandas as pd 
import psycopg2
from conexao import conexao

conn = psycopg2.connect(**conexao)
query = "SELECT * FROM public.economia_agua_"

df = pd.read_sql_query(query, conn)
print(df.info())
df['CD_BAIRRO'] = df["CD_BAIRRO"].fillna("Sem infomações")
df['S_ABASTE'] = df["S_ABASTE"].fillna("Sem infomações")
df['SUB_S_AB'] = df["SUB_S_AB"].fillna("Sem infomações")
df['S_ESGO'] = df["S_ESGO"].fillna("Sem infomações")
df = df.rename(columns={'0_municipi': 'municipio', '1_n_econom': 'numero_econom', 
                        '0_ano_refe': 'data_referencia', 'CD_SETOR': 'cod_setor', 'CD_MUN': 'cod_muni', 'CD_DIST':'cod_distrito', 'S_ABASTE': 'sistema_abastec',
                        'S_ESGO': 'sistema_esgoto', 'SUB_S_AB': 'subsist_abastec', 'CD_BAIRRO': 'cod_bairro'})
df['data_referencia'] = pd.to_datetime('01/' + '01' + '/' + df['data_referencia'].astype(str), format='%d/%m/%Y')
df['data_referencia'] = df['data_referencia'].dt.date
df['numero_econom'] = df['numero_econom'].astype(int)
print(df.head())
# valores = df['cod_setor'].unique().tolist()
# print(valores)

conn.close()