from re import match

from sqlalchemy import create_engine
import psycopg2
import configparser
import pandas as pd
import numpy as np

#pd.set_option('display.max_columns', None)

# Read config file
config = configparser.ConfigParser()

config.read('config.ini')
host = config.get('login', 'host')
port = config.get('login', 'port')
db = config.get('login', 'database')
user = config.get('login', 'user')
pw = config.get('login', 'password')

# Connect to PostgreSQL database
engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}')
connection = engine.raw_connection()
cursor = connection.cursor()

# Check all tables available on schema
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type='BASE TABLE';
""")
print('\nTables available: ', cursor.fetchall())

# Read .xlsx file
raw_data = pd.read_excel('dados_importacao.xlsx')

# Check original columns names
original_columns = raw_data.columns.values
print('\nOriginal columns names: ', original_columns)

# Create dataframe to be transformed
df = raw_data

# Modify columns names to match the names on the schema

# Since there are no information about the source of the Excel file,
# it`s unknown if the positions or the names of the columns are something fixed.
# If the positions are fixed, it`s possible to map using the columns indexes.
# If the names are fixed, it`s possible to map using the columns names.
# If neither are fixed, it`s needed to remap the columns for every source file.

df.rename(columns={
    'Nome/Razão Social': 'nome_razao_social',
    'Nome Fantasia': 'nome_fantasia',
    'CPF/CNPJ': 'cpf_cnpj',
    'Data Nasc.': 'data_nascimento',
    'Data Cadastro cliente': 'data_cadastro',
    'Celulares': 'Celular',
    'Telefones': 'Telefone',
    'Emails': 'E-mail',
    'Endereço': 'endereco_logradouro',
    'Número': 'endereco_numero',
    'Complemento': 'endereco_complemento',
    'Bairro': 'endereco_bairro',
    'CEP': 'endereco_cep',
    'Cidade': 'endereco_cidade',
    'UF': 'endereco_uf',
    'Plano': 'descricao',
    'Plano Valor': 'valor',
    'Vencimento': 'dia_vencimento',
    'Status': 'status',
    'Isento': 'isento'
}, inplace=True)

# Validating dataframe

# Creating column for the exclusion reasons
df = df.assign(motivo_exclusao = '')

# Check for null values
# check if nome_razao_social missing
column_to_be_checked = 'nome_razao_social'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '

#check if cpf_cnpj missing
column_to_be_checked = 'cpf_cnpj'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '
#check if cpf_cnpj has the correct length
df[column_to_be_checked] = df[column_to_be_checked].astype(str).str.replace(r'[\D]', '', regex=True)
cpf_cnpj_len = df[column_to_be_checked].str.len()
df.loc[~cpf_cnpj_len.isin([11, 14]), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' errado '
#convert to correct format
df.loc[cpf_cnpj_len == 11, column_to_be_checked] = df.loc[cpf_cnpj_len == 11, column_to_be_checked].apply(lambda x: f"{x[0:3]}.{x[3:6]}.{x[6:9]}-{x[9:11]}")
df.loc[cpf_cnpj_len == 14, column_to_be_checked] = df.loc[cpf_cnpj_len == 14, column_to_be_checked].apply(lambda x: f"{x[0:2]}.{x[2:5]}.{x[5:8]}/{x[8:12]}-{x[12:14]}")

#check if endereco_logradouro missing
column_to_be_checked = 'endereco_logradouro'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '

#check if endereco_bairro missing
column_to_be_checked = 'endereco_bairro'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '

#check if endereco_cidade missing
column_to_be_checked = 'endereco_cidade'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '

#check if endereco_cep missing
column_to_be_checked = 'endereco_cep'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '
#check if endereco_cep has the correct length
df[column_to_be_checked] = df[column_to_be_checked].astype(str).str.replace(r'[\D]', '', regex=True)
cpf_cnpj_len = df[column_to_be_checked].str.len()
df.loc[~cpf_cnpj_len == 8, 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' errado '
#convert to correct format
df.loc[cpf_cnpj_len == 8, column_to_be_checked] = df.loc[cpf_cnpj_len == 8, column_to_be_checked].apply(lambda x: f"{x[0:5]}-{x[5:8]}")

#convert to UF
column_to_be_checked = 'endereco_uf'
def ufConverter(state_name):
    match state_name:
        case 'Acre':
            return 'AC'
        case 'Alagoas':
            return 'AL'
        case 'Amapá':
            return 'AP'
        case 'Amazonas':
            return 'AM'
        case 'Bahia':
            return 'BA'
        case 'Ceará':
            return 'CE'
        case 'Distrito Federal':
            return 'DF'
        case 'Espírito Santo':
            return 'ES'
        case 'Goiás':
            return 'GO'
        case 'Maranhão':
            return 'MA'
        case 'Mato Grosso':
            return 'MT'
        case 'Mato Grosso do Sul':
            return 'MS'
        case 'Minas Gerais':
            return 'MG'
        case 'Pará':
            return 'PA'
        case 'Paraíba':
            return 'PB'
        case 'Paraná':
            return 'PR'
        case 'Pernambuco':
            return 'PE'
        case 'Piauí':
            return 'PI'
        case 'Rio de Janeiro':
            return 'RJ'
        case 'Rio Grande do Norte':
            return 'RN'
        case 'Rio Grande do Sul':
            return 'RS'
        case 'Rondônia':
            return 'RO'
        case 'Roraima':
            return 'RR'
        case 'Santa Catarina':
            return 'SC'
        case 'São Paulo':
            return 'SP'
        case 'Sergipe':
            return 'SE'
        case 'Tocantins':
            return 'TO'
        case _:
            return 'Error'
df['endereco_uf'] = df['endereco_uf'].apply(lambda x: ufConverter(x))
#check if endereco_uf is correct
df.loc[df['endereco_uf'] == "Error", 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' errado '

#check if dia_vencimento missing
column_to_be_checked = 'dia_vencimento'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '

#check if status missing
column_to_be_checked = 'status'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '

#check if descricao missing
column_to_be_checked = 'descricao'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '

#check if valor missing
column_to_be_checked = 'valor'
df.loc[df[column_to_be_checked].isna(), 'motivo_exclusao'] += '/ ' + column_to_be_checked + ' faltante '

# Replace values for isento column
df['isento'] = ~df['isento'].isna()

# Change celular and telefone datatype
df['Celular'] = df['Celular'].astype(str)
df['Telefone'] = df['Telefone'].astype(str)

# Separate valid rows
df_validated = df.loc[df['motivo_exclusao'] == '']
df_excluded = df.loc[df['motivo_exclusao'] != '']

# tbl_clientes transformation

# Separate tbl_clientes
tbl_clientes = df_validated[['nome_razao_social', 'nome_fantasia', 'cpf_cnpj', 'data_nascimento', 'data_cadastro']]
# Remove duplicates on cpf_cnpj
tbl_clientes = tbl_clientes.groupby('cpf_cnpj').agg({
    'nome_razao_social': 'first',       # keeping first non-null value
    'nome_fantasia': 'first',           # keeping first non-null value
    'data_nascimento': 'first',         # keeping first non-null value
    'data_cadastro': 'min'              # keeping the earliest value
}).reset_index()
# Add clientes_index
tbl_clientes['id'] = tbl_clientes.reset_index(drop=True).index + 1
# tbl_clientes ready

# Separate tbl_cliente_contatos
tbl_cliente_contatos = df_validated[['cpf_cnpj', 'Celular', 'Telefone', 'E-mail']].melt(id_vars=['cpf_cnpj'], var_name='tipo_contato', value_name='contato')
tbl_cliente_contatos = tbl_cliente_contatos.replace('nan', pd.NA).dropna()
tbl_cliente_contatos['id'] = tbl_cliente_contatos.reset_index(drop=True).index + 1

# Separate tbl_tipos_contato
tbl_tipos_contato = tbl_cliente_contatos[['tipo_contato']].drop_duplicates()
tbl_tipos_contato['id'] = tbl_tipos_contato.reset_index(drop=True).index + 1
# tbl_tipos_contato ready

# Obtain tipo_contato_id for tbl_cliente_contatos
tbl_cliente_contatos = tbl_cliente_contatos.merge(tbl_tipos_contato, how='left', on='tipo_contato', suffixes=('', "_right")).rename(columns={'id_right': 'tipo_contato_id'}).drop(columns=["tipo_contato"])

# Obtain cliente_id for tbl_cliente_contatos
tbl_cliente_contatos = tbl_cliente_contatos.merge(tbl_clientes, how='left', on='cpf_cnpj', suffixes=('', "_right")).rename(columns={'id_right': 'cliente_id'}).drop(columns=["cpf_cnpj", 'nome_razao_social', 'nome_fantasia', 'data_nascimento', 'data_cadastro'])
# tbl_cliente_contatos ready

# Separate tbl_status_contrato
tbl_status_contrato = df_validated[['status']].drop_duplicates()
tbl_status_contrato['id'] = tbl_status_contrato.reset_index(drop=True).index + 1
# tbl_status_contrato ready

# Separate tbl_planos
tbl_planos = df_validated[['descricao', 'valor']].drop_duplicates()
tbl_planos['id'] = tbl_planos.reset_index(drop=True).index + 1
# tbl_planos ready

# Separate tbl_cliente_contratos
tbl_cliente_contratos = df_validated[
    ['cpf_cnpj', 'descricao', 'dia_vencimento', 'isento', 'endereco_logradouro', 'endereco_numero', 'endereco_bairro',
     'endereco_cidade', 'endereco_complemento', 'endereco_cep', 'endereco_uf', 'status']].drop_duplicates()
tbl_cliente_contratos['id'] = tbl_cliente_contratos.reset_index(drop=True).index + 1
# Obtain cliente id
tbl_cliente_contratos = tbl_cliente_contratos.merge(tbl_clientes, how='left', on='cpf_cnpj', suffixes=('', "_right")).rename(columns={'id_right': 'cliente_id'}).drop(columns=["cpf_cnpj", 'nome_razao_social', 'nome_fantasia', 'data_nascimento', 'data_cadastro'])
# Obtain status id
tbl_cliente_contratos = tbl_cliente_contratos.merge(tbl_status_contrato, how='left', on='status', suffixes=('', "_right")).rename(columns={'id_right': 'status_id'}).drop(columns=["status"])
# Obtain plano id
tbl_cliente_contratos = tbl_cliente_contratos.merge(tbl_planos, how='left', on='descricao', suffixes=('', "_right")).rename(columns={'id_right': 'plano_id'}).drop(columns=["descricao", 'valor'])

# Upload data to PsotgreSQL

tbl_planos.set_index('id', inplace=True)
tbl_planos.to_sql(name='tbl_planos', con=engine, index_label='id', schema='public', if_exists='append')
print(f"\ntbl_planos upload succesful. {tbl_planos.shape[0]} record(s) were added to this table.")

tbl_status_contrato.set_index('id', inplace=True)
tbl_status_contrato.to_sql(name='tbl_status_contrato', con=engine, index_label='id', schema='public', if_exists='append')
print(f"tbl_status_contrato upload succesful. {tbl_status_contrato.shape[0]} record(s) were added to this table.")

tbl_clientes.set_index('id', inplace=True)
tbl_clientes.to_sql(name='tbl_clientes', con=engine, index_label='id', schema='public', if_exists='append')
print(f"tbl_clientes upload succesful. {tbl_clientes.shape[0]} record(s) were added to this table.")

tbl_tipos_contato.set_index('id', inplace=True)
tbl_tipos_contato.to_sql(name='tbl_tipos_contato', con=engine, index_label='id', schema='public', if_exists='append')
print(f"tbl_tipos_contato upload succesful. {tbl_tipos_contato.shape[0]} record(s) were added to this table.")

tbl_cliente_contatos.set_index('id', inplace=True)
tbl_cliente_contatos.to_sql(name='tbl_cliente_contatos', con=engine, index_label='id', schema='public', if_exists='append')
print(f"tbl_cliente_contatos upload succesful. {tbl_cliente_contatos.shape[0]} record(s) were added to this table.")

tbl_cliente_contratos.set_index('id', inplace=True)
tbl_cliente_contratos.to_sql(name='tbl_cliente_contratos', con=engine, index_label='id', schema='public', if_exists='append')
print(f"tbl_cliente_contratos upload succesful. {tbl_cliente_contratos.shape[0]} record(s) were added to this table.")

print(f"\n{df_excluded.shape[0]} record(s) were not added to the PostgreSQL database.\n")
print(df_excluded)