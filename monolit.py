# -*- coding: utf-8 -*-
"""Untitled1.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1gsQPN528N1SZjT72tOpDMZWLEUeXsXL6
"""

# !pip install gspread requests pandas
import gspread
import requests
import pandas as pd
import json
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configurar o acesso ao Notion
NOTION_URL = "https://api.notion.com/v1/pages/"
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

gc = None

def parse_brl_to_float(value):
    value = str(handle_div_zero(value))
    value = value.replace("R$", "").strip()
    value = value.replace(".", "").replace(",", ".")
    return float(value)

def parse_int(value):
    value = value.replace(".", "").replace(",", "").strip()
    try:
        return int(value)
    except ValueError:
        return 0

def parse_percent(value):
    # value = str(handle_div_zero(value))
    value = value.replace('%', '').strip()
    try:
        return float(value) / 100
    except ValueError:
        return 0

def handle_div_zero(value):
    if value == "#DIV/0!":
        return 0  # or None, depending on the use case
    else:
        try:
            return float(value)
        except ValueError:
            return value

def open_sheet(sheet_name, sheet_page='PAINEL OUTUBRO/2024'):
    try:
        sheet = gc.open(sheet_name).worksheet(sheet_page)
    except:
        sheet = gc.open_by_url(sheet_name).worksheet(sheet_page)
    data = sheet.get_all_values()
    df = pd.DataFrame.from_records(data)
    return df

def print_sheet_data(sheet_name, sheet_page='PAINEL OUTUBRO/2024'):
    df = open_sheet(sheet_name, sheet_page)
    print('nome dashboard: ' + df[1][1])
    print("# leads:" + df[3][8])
    print("invest. ads: " + df[1][8])
    print("taxa conversao agendamento: " + df[1][11])
    print("# agendamentos: " + df[3][11])
    print("taxa conversao comparecimento: " + df[1][14])
    print("# realizado: " + df[3][14])
    print("TAXA DE CONVERSÃO P/ VENDA : " + df[1][17])
    print("# vendas: " + df[3][17])
    print("tkm: " + df[1][20])
    print("faturamento: " + df[3][20])
    print("roas: " + df[5][20])

def df_to_metricas(df):
    metricas = {
        "nome_dashboard": df[1][1],
        "Nº DE LEADS 👥": parse_int(df[3][8]),
        "INVESTIMENTO EM ADS 💸": parse_brl_to_float(df[1][8]),
        "N° DE AGENDAMENTOS": parse_int(df[3][11]),
        "TAXA DE CONVERSÃO P/ AGENDAMENTO": parse_percent(df[1][11]),
        "N° DE REALIZADO":  parse_int(df[3][14]),
        "TAXA DE CONVERSÃO P/ COMPARECIMENTO":  parse_percent(df[1][14]),
        "N° DE VENDAS": parse_int(df[3][17]),
        "TAXA DE CONVERSÃO P/ VENDA":  parse_percent(df[1][17]),
        "FATURAMENTO": parse_brl_to_float(df[3][20]),
        "TKM": parse_brl_to_float(df[1][20]),
        "ROAS": parse_brl_to_float(df[5][20])
    }
    return metricas

def notion_client_id(client_name) -> str:
    # Verificar existencia do cliente na database:
    check_cliente_url = f'https://api.notion.com/v1/databases/{DATABASE_ID}/query'
    query_payload = {
        "filter": {
            "property": "Cliente",
            "rich_text": {
                "equals": client_name
            }
        }
    }
    response = requests.post(check_cliente_url, json=query_payload, headers=HEADERS)

    # print('verificando existencia do cliente: Status: ' + str(response.status_code))
    if response.status_code != 200:
        raise Exception(f"Erro ao verificar existência do cliente: {response.status_code} - {response.text}")
        #"Nao foi possível contactar ao notion, erro: " + response.text

    res = response.json()
    if len(res['results']) > 0:
        results = res['results']
        cliente_page_id = results[0]['id']
        return cliente_page_id
    return None

def notion_create_post_payload(cliente_nome, metricas):
    def create_nome(cliente_nome):
        return {
            "text": {
                "content": cliente_nome
            }
        }
    payload = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {}
    }

    payload['properties'] = metricas
    payload['properties']['title'] = [create_nome(cliente_nome)]
    # del(payload['properties']['nome_dashboard'])
    return payload

def notion_create_patch_payload(metricas):
    patch_payload = { 'properties': metricas }
    # del(patch_payload['properties']['nome_dashboard'])
    return patch_payload

def extract_client_name(nome_dashboard):
    return nome_dashboard[len("DASHBOARD -"):]

def get_argv_value(*arg_names, default=None):
    for arg_name in arg_names:
        if arg_name in sys.argv:
            return sys.argv[sys.argv.index(arg_name)+1]
    return default

def id_to_url(id):
    return f'https://docs.google.com/spreadsheets/d/{id}/edit?usp=sharing'

MESES = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO']

def main(**kwargs):
    global gc

    mes = kwargs.get('mes')
    ano = kwargs.get('ano')
    file_id = kwargs.get('file_id')
    prevent_post = kwargs.get('prevent_post')

    mes = MESES[mes-1].upper()
    print(f"Atualizando: Mes: {mes}, Ano: {ano}, File ID: {file_id}")

    if len(ano) != 4:
        print("Ano inválido")
        sys.exit(1)

    if mes not in MESES:
        print("Mês inválido")
        sys.exit(1)

    with open('credentials.json', 'r', encoding='utf-8') as f:
        credentials = json.load(f)

    # Configurar credenciais do Google Sheets
    gc = gspread.service_account_from_dict(credentials)

    is_print = kwargs.get('is_print', False)
    if is_print:
        print_sheet_data(id_to_url(file_id), f'PAINEL {mes}/{ano}')
    metricas = df_to_metricas(open_sheet(id_to_url(file_id), f'PAINEL {mes}/{ano}'))

    cliente_nome = extract_client_name(metricas['nome_dashboard'])
    del(metricas['nome_dashboard'])

    cliente_id = notion_client_id(cliente_nome)

    if prevent_post:
        print('Aborting post by --no-post flag')
        print(json.dumps({
            'cliente_id': cliente_id,
            'cliente_nome': cliente_nome,
            'metricas': metricas
        }, indent=2))
        return

    if cliente_id:
        payload = notion_create_patch_payload(metricas)
        response = requests.patch(NOTION_URL + cliente_id, json=payload, headers=HEADERS)
    else:
        payload = notion_create_post_payload(cliente_nome, metricas)
        response = requests.post(NOTION_URL, json=payload, headers=HEADERS)

    if response.status_code == 200:
        print("Dados enviados com sucesso para o Notion!")
    else:
        response_text = json.loads(response.text)
        print(f"Erro ao enviar dados para o Notion: {response.status_code} - {response_text}")

class Monolit:
    def update(self, file):
        kwargs = {}
        kwargs['mes'] = datetime.today().month
        kwargs['ano'] = str(datetime.today().year)
        kwargs['file_id'] = file["file_info"]["id"]
        kwargs['prevent_post'] = False
        kwargs['is_print'] = False
        main(**kwargs)


if __name__ == "__main__":
    kwargs = {}

    kwargs['mes'] = int(get_argv_value('--mes', '-m', default=datetime.today().month))
    kwargs['ano'] = get_argv_value('--ano', '-a', default=str(datetime.today().year))
    kwargs['file_id'] = get_argv_value('--file-id', '-f', default='1FhZ5ktfehaN8V9FtpbLrC8Jn-pst4514hnZ76YiY6hk')
    kwargs['prevent_post'] = '-n' in sys.argv or '--no-post' in sys.argv
    kwargs['is_print'] = '-p' in sys.argv or '--print' in sys.argv
    main(**kwargs)
