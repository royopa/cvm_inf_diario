# %%
from requests_html import HTMLSession
from concurrent.futures import ThreadPoolExecutor
import requests
import os
import shutil
import glob
import pandas as pd
import dask.dataframe as dd
import dask.multiprocessing
import dask.threaded
from dask.diagnostics import ProgressBar
ProgressBar().register()

# %%
def download_file(url):
    response = requests.get(url)
    if "content-disposition" in response.headers:
        content_disposition = response.headers["content-disposition"]
        filename = content_disposition.split("filename=")[1]
    else:
        filename = url.split("/")[-1]
        with open(filename, mode="wb") as file:
            file.write(response.content)
        print(f"Downloaded file {filename}")

# %%
# Arquivos a partir de 2021 - ainda passam pelo processo de atualização CSV
session = HTMLSession()
links = []
base_url = 'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'
r = session.get(base_url)
print(r)

for link in r.html.links:
    if not link.endswith('.zip'):
        continue
    url = base_url+link
    links.append(url)

# %%
for link in links:
    print('Baixando', link)
    download_file(link)

# %%
print('Movendo arquivos zip para a pasta ./zip')
for file_name in os.listdir():
    if not file_name.endswith('.zip'):
        continue
    if not os.path.exists(os.path.join('.', 'zip')):
        os.mkdir('zip')
    print(file_name)
    shutil.unpack_archive(file_name)
    shutil.move(
        os.path.join(file_name),
        os.path.join('zip', file_name)
    )
print('ok')

# %%
print('Movendo arquivos csv para a pasta ./csv')
for file_name in os.listdir():
    if not file_name.endswith('.csv'):
        continue
    if not os.path.exists(os.path.join('.', 'csv')):
        os.mkdir('csv')
    print(file_name)
    shutil.move(
        os.path.join(file_name),
        os.path.join('csv', file_name)
    )
print('ok')


