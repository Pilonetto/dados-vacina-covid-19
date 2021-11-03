# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 09:59:33 2021

@author: Marlon
"""

import requests
import json
import pandas as pd


##ATENÇÃO, A BASE DE DADOS TOTAL TEM 275 MILHÕES DE REGISTROS E TOTALIZA QUASE 135 GB DE DADOS

url = "https://imunizacao-es.saude.gov.br/_search?scroll=1m"

payload = json.dumps({
  "size": 10000
})
headers = {
  'Authorization': 'Basic aW11bml6YWNhb19wdWJsaWM6cWx0bzV0JjdyX0ArI1Rsc3RpZ2k=',
  'Content-Type': 'application/json',
  'Cookie': 'ELASTIC-PROD=1635943791.031.5321.716134'
}

_cols = ["estabelecimento_valor",
        "vacina_fabricante_referencia",
        "estabelecimento_razaoSocial",
        "estabelecimento_municipio_codigo",
        "vacina_descricao_dose",
        "status",
        "dt_deleted",
        "id_sistema_origem",
        "vacina_categoria_nome",
        "estalecimento_noFantasia",
        "paciente_dataNascimento",
        "estabelecimento_uf",
        "paciente_endereco_coIbgeMunicipio",
        "paciente_endereco_uf",
        "paciente_endereco_nmMunicipio",
        "paciente_racaCor_codigo",
        "estabelecimento_municipio_nome",
        "vacina_lote",
        "vacina_dataAplicacao",
        "paciente_idade",
        "paciente_racaCor_valor",
        "paciente_endereco_coPais",
        "sistema_origem",
        "vacina_codigo",
        "vacina_grupoAtendimento_nome",
        "@timestamp",
        "paciente_nacionalidade_enumNacionalidade",
        "vacina_categoria_codigo",
        "vacina_grupoAtendimento_codigo",
        "@version",
        "paciente_endereco_nmPais",
        "paciente_endereco_cep",
        "data_importacao_rnds",
        "paciente_id",
        "vacina_fabricante_nome",
        "paciente_enumSexoBiologico",
        "vacina_nome",
        "document_id"]    
print('Starting data extraction')

df = pd.DataFrame( columns=_cols)
pg = 0
while True:    
    pg +=1        
    response = requests.request("POST", url, headers=headers, data=payload)
    _id = response.json()['_scroll_id']
    _els = response.json()['hits']['hits']
    _list = []
    for el in _els:
       _src = el['_source']
       _list.append(_src)
    
    dfp = pd.DataFrame(_list, columns=_cols)
    df = pd.concat([df, dfp]) 
    
    if (len(_els) == 0):
        print("no new data")
        break
    
    url = "https://imunizacao-es.saude.gov.br/_search/scroll"
    payload = json.dumps({
            "scroll_id": _id,
            "scroll": "1m"
    })
    
    print('''extracting page %d, total records %d ''' % (pg, len(df)))

df.to_csv('data.csv',header=True)    
print('end of extraction')    
    


