import re
import numpy as np
import pandas as pd 
import psycopg2
from conexao import conexao

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
query = "SELECT * FROM public.captacao_subterranea_"

df = pd.read_sql_query(query, conn)
print(df.info())
df['CD_BAIRRO'] = df["CD_BAIRRO"].fillna("SEM INFORMAÇÕES")
df['S_ABASTE'] = df["S_ABASTE"].fillna("SEM INFORMAÇÕES")
df['SUB_S_AB'] = df["SUB_S_AB"].fillna("SEM INFORMAÇÕES")
df['S_ESGO'] = df["S_ESGO"].fillna("SEM INFORMAÇÕES")
df = df.rename(columns={
    '0_municipi': 'municipio',
    '1_n_econom': 'numero_econom',
    '0_ano_refe': 'data_referencia',
    'CD_SETOR': 'cod_setor',
    'CD_MUN': 'cod_muni',
    'CD_DIST': 'cod_distrito',
    'S_ABASTE': 'sistema_abastec',
    'S_ESGO': 'sistema_esgoto',
    'SUB_S_AB': 'subsist_abastec',
    'CD_BAIRRO': 'cod_bairro',
    '1_denomina': 'denominacao',
    '3_situacao': 'situacao',
    '4_vazao_no': 'vazao_normal',
    '5_profundi': 'profundidade',
    '6_nivel_di': 'nivel_dinamico',
    '7_nivel_es': 'nivel_estatico',
    '8_diametro': 'diametro_bomba',
    '9_diametro': 'diametro_tubo',
    '10_tempo_f': 'tempo_funcionamento',
    '10_tempo_1': 'tempo_funcionamento_1',
    '11_inicio_': 'inicio_funcionamento',
    '12_marca': 'marca_bomba',
    '12_modelo': 'modelo_bomba',
    '13_manuten': 'manutencao',
    '14_bomba_r': 'bomba_reserva',
    '15_abrigo_': 'abrigo_protecao',
    '16_macrome': 'macromedidor',
    '16_tubo_gu': 'tubo_guia',
    '16_laje_pr': 'laje_protecao',
    '16_valvula': 'valvula_reducao',
    '16_tomada_': 'tomada_agua',
    '16_registr': 'registro',
    '16_area_pr': 'area_protecao',
    '16_tampa_v': 'tampa_visita',
    '17_tipo_re': 'tipo_reservatorio',
    '18_possui_': 'possui_hidrometro',
    '19_abrigo_': 'abrigo_reservatorio',
    '20_situaca': 'situacao_desinfeccao',
    '20_desinfe': 'desinfeccao',
    '20_produto': 'produto_utilizado',
    '20_cloraca': 'cloracao',
    '21_bombeam': 'bombeamento',
    '22_numero_': 'numero_ligacoes',
    '22_data_em': 'data_emissao',
    '22_data_va': 'data_validade',
    '22_observa': 'observacoes',
    '23_bairros': 'bairros_atendidos',
    '23_bairr_1': 'bairros_1'
})
df['data_referencia'] = pd.to_datetime('01/' + '01' + '/' + df['data_referencia'].astype(str), format='%d/%m/%Y')
df['data_referencia'] = df['data_referencia'].dt.date

df['situacao'] = df["situacao"].fillna("SEM INFORMAÇÕES")
df['situacao'] = df["situacao"].apply(limpar_e_maiusculo)

df['tempo_funcionamento'] = df["tempo_funcionamento"].fillna("SEM INFORMAÇÕES")

df['tempo_funcionamento_1'] = df["tempo_funcionamento_1"].fillna("SEM INFORMAÇÕES")

df['marca_bomba'] = df["marca_bomba"].fillna("SEM INFORMAÇÕES")

df['modelo_bomba'] = df["modelo_bomba"].fillna("SEM INFORMAÇÕES")

df['inicio_funcionamento'] = df['inicio_funcionamento'].replace(0, np.nan)
df['inicio_funcionamento'] = '01/01/' + df['inicio_funcionamento'].astype('Int64').astype(str)
df['inicio_funcionamento'] = pd.to_datetime(df['inicio_funcionamento'], format='%d/%m/%Y', errors='coerce')
df['inicio_funcionamento'] = df['inicio_funcionamento'].dt.date

df['manutencao'] = df["manutencao"].fillna("SEM INFORMAÇÕES")

df['manutencao'] = df["manutencao"].apply(limpar_outro)

df['bomba_reserva'] = df["bomba_reserva"].fillna("SEM INFORMAÇÕES")

df['abrigo_protecao'] = df["abrigo_protecao"].fillna("SEM INFORMAÇÕES")

df['macromedidor'] = df["macromedidor"].fillna("SEM INFORMAÇÕES")

df['tubo_guia'] = df["tubo_guia"].fillna("SEM INFORMAÇÕES")

df['laje_protecao'] = df["laje_protecao"].fillna("SEM INFORMAÇÕES")

df['valvula_reducao'] = df["valvula_reducao"].fillna("SEM INFORMAÇÕES")

df['tomada_agua'] = df["tomada_agua"].fillna("SEM INFORMAÇÕES")

df['registro'] = df["registro"].fillna("SEM INFORMAÇÕES")

df['area_protecao'] = df["area_protecao"].fillna("SEM INFORMAÇÕES")

df['tampa_visita'] = df["tampa_visita"].fillna("SEM INFORMAÇÕES")

df['tipo_reservatorio'] = df["tipo_reservatorio"].fillna("SEM INFORMAÇÕES")
df['tipo_reservatorio'] = df['tipo_reservatorio'].apply(lambda x: 'NÃO INFORMADO' if isinstance(x, str) and x.strip().upper() == 'NÃO INFOMADO' else x)

df['possui_hidrometro'] = df["possui_hidrometro"].fillna("SEM INFORMAÇÕES")

df['abrigo_reservatorio'] = df["abrigo_reservatorio"].fillna("SEM INFORMAÇÕES")

df['situacao_desinfeccao'] = df["situacao_desinfeccao"].fillna("SEM INFORMAÇÕES")
df['situacao_desinfeccao'] = df["situacao_desinfeccao"].apply(limpar_e_maiusculo)
df['desinfeccao'] = df["desinfeccao"].fillna("SEM INFORMAÇÕES")

df['produto_utilizado'] = df["produto_utilizado"].fillna("SEM INFORMAÇÕES")

df['cloracao'] = df["cloracao"].fillna("SEM INFORMAÇÕES")
df['cloracao'] = df["cloracao"].apply(limpar_outro)

df['bombeamento'] = df["bombeamento"].fillna("SEM INFORMAÇÕES")

df['data_emissao'] = df['data_emissao'].replace(0, np.nan)
# df['data_emissao'] = '01/01/' + df['data_emissao'].astype('Int64').astype(str)
df['data_emissao'] = pd.to_datetime(df['data_emissao'], format='%d/%m/%Y', errors='coerce')
df['data_emissao'] = df['data_emissao'].dt.date

df['data_validade'] = df['data_validade'].replace(0, np.nan)
# df['data_validade'] = '01/01/' + df['data_validade'].astype('Int64').astype(str)
df['data_validade'] = pd.to_datetime(df['data_validade'], format='%d/%m/%Y', errors='coerce')
df['data_validade'] = df['data_validade'].dt.date

df['observacoes'] = df["observacoes"].fillna("SEM OBSERVAÇÕES")
df['observacoes'] = df["observacoes"].apply(limpar_e_maiusculo)

df['bairros_atendidos'] = df["bairros_atendidos"].fillna("SEM INFORMAÇÕES")
df['bairros_atendidos'] = df['bairros_atendidos'].str.replace('\n', ' ').str.strip()
df['bairros_atendidos'] = df["bairros_atendidos"].apply(limpar_e_maiusculo)

df['bairros_1'] = df["bairros_1"].fillna("SEM INFORMAÇÕES")
df['bairros_1'] = df["bairros_1"].apply(limpar_e_maiusculo)
print(df.head())


# valores = df['bairros_atendidos'].unique().tolist()
# print(valores)

conn.close()