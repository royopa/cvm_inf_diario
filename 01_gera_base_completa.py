# %%
import dask.dataframe as dd
import dask.multiprocessing
import dask.threaded
from dask.diagnostics import ProgressBar
ProgressBar().register()

# %%
# Arquivos atuais
df_2005_2021 = dd.read_csv(['csv_2005_2020/inf_diario_fi_20*.csv'], sep=";", dtype={'TP_FUNDO': 'object'}) #44645554
df_2005_2021 = df_2005_2021.compute()
# cria a coluna vazia
df_2005_2021['TP_FUNDO'] = None
df_2005_2021 = dd.from_pandas(df_2005_2021, npartitions=4)

df_atual = dd.read_csv(['csv/inf_diario_fi_20*.csv'], sep=";", dtype={'TP_FUNDO': 'object'}) #17925211

df = dd.concat([df_2005_2021, df_atual]) 

print(df.columns)
print(f'O dataframe tem {len(df)} registros')

# %%
# transforma o campo DT_COMPTC
df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.replace('.', '')
df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.replace('/', '')
df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.replace('-', '')
df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].str.zfill(14)

# cria o campo CO_PRD
df['CO_PRD'] = df['CNPJ_FUNDO']

# cria e formata o campo DT_REF, com a data de referência
print('Formatando campo DT_REF')
df.assign(DT_COMPTC=dd.to_datetime(df['DT_COMPTC'], format='%Y-%m-%d', errors='coerce'))
df['DT_REF'] = df['DT_COMPTC']

# %%
# faz o sort do df
df = df.sort_values(by=['CO_PRD', 'DT_REF'])

# Convert to a Pandas DataFrame because dask was being slow with the select logic below
print('Converte dataframe dask para pandas')
df = df.compute()

# cria uma nova coluna com o percentual de resgate para o dia
print('Calculando campo PC_RESG')
df['PC_RESG'] = df['RESG_DIA'] / df.groupby(['CO_PRD'])['VL_PATRIM_LIQ'].shift(1)

# Convert back to a Dask dataframe because we want that juicy parallelism
print('Converte dataframe pandas para dask')
df = dd.from_pandas(df, npartitions=4)

df['CNPJ_FUNDO'] = df['CNPJ_FUNDO'].astype(str).str.zfill(14)
df['CO_PRD'] = df['CO_PRD'].astype(str).str.zfill(14)

# removendo itens duplicados
print('Removendo itens duplicados')
df = df.drop_duplicates(subset=['CO_PRD', 'DT_REF'], keep='last')

df.tail()

# %%
print('Salvando base completa')
df.to_csv(filename='base_completa.csv', single_file=True, index=False)

# %%
#df = df.compute()
df_fundo = df[df['CO_PRD'] == '68623479000156']
df_fundo = df_fundo.sort_values(by=['DT_REF'])
df_fundo.head()

# %%
df_fundo.tail()

# %%
df_fundo = df[df['CO_PRD'] == '03737223000124']
df_fundo = df_fundo.sort_values(by=['DT_REF'])
df_fundo.head()

# %%
df_fundo.tail()


