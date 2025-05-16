import pyodbc
import pandas as pd
from pathlib import Path
from pandas_gbq import read_gbq
import requests
import time
import os

def get_project_root():
    return Path(__file__).resolve().parent

def registrar_id_na_planilha(id_integracao: str):
    nome_arquivo = 'Omie-nao-encontrado.xlsx'
    caminho_completo =  get_project_root() / nome_arquivo
    coluna = 'codigo integracao'

    if os.path.exists(caminho_completo):
        df = pd.read_excel(caminho_completo)
    else:
        df = pd.DataFrame(columns=[coluna])

    if coluna not in df.columns:
        df[coluna] = None

    if id_integracao not in df[coluna].astype(str).values:
        novo_dado = pd.DataFrame({coluna: [id_integracao]})
        df = pd.concat([df, novo_dado], ignore_index=True)

    df.to_excel(caminho_completo, index=False)

def extrair_nsu(nome_arquivo):
    try:
        caminho_completo = get_project_root() / nome_arquivo
        df = pd.read_excel(caminho_completo)

        if 'NSU' not in df.columns or 'Servico' not in df.columns:
            raise ValueError("As colunas 'NSU' e/ou 'Servico' não foram encontradas na planilha.")

        servicos_excluir = [
            'Licenciamento Online - SEFAZ/SP',
            'Liberação de Veículo Apreendido, Revistoria, Rebocamento - Liberação Veículo Apreendido'
        ]

        df_filtrado = df[~df['Servico'].isin(servicos_excluir)]

        nsu_lista = df_filtrado['NSU'].dropna().tolist()
        return nsu_lista

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return []
    
def exportar_nsu_nao_encontrados(nsu_lista, caminho_arquivo):
    try:
        caminho = get_project_root() / caminho_arquivo

        df = pd.read_excel(caminho)
        df.columns = df.columns.str.strip()

        if 'NSU' not in df.columns:
            raise ValueError("Coluna 'NSU' não encontrada na planilha.")

        nsus_planilha = df['NSU'].dropna().astype(str).str.strip().tolist()
        nsus_nao_encontrados = [nsu for nsu in nsu_lista if str(nsu).strip() not in nsus_planilha]

        if nsus_nao_encontrados:
            df_nao_encontrados = pd.DataFrame({'NSU': nsus_nao_encontrados})
            df_nao_encontrados.to_excel('nao-encontrados.xlsx', index=False)
            print("Arquivo 'nao-encontrados.xlsx' criado com sucesso.")
        else:
            print("Todos os NSUs foram encontrados na planilha.")

    except Exception as e:
        print(f"Erro ao verificar NSUs: {e}")

def atualizar_valores_omie(caminho_arquivo):
    try:
        caminho = get_project_root() / caminho_arquivo

        df = pd.read_excel(caminho)
        df.columns = df.columns.str.strip()

        if 'identificadorMottu' not in df.columns:
            raise ValueError("Coluna 'identificadorMottu' não encontrada.")

        if 'amount' not in df.columns:
            df['amount'] = None
        if 'receiptReference' not in df.columns:
            df['receiptReference'] = None
        if 'type' not in df.columns:
            df['type'] = None
        if 'accountCode' not in df.columns:
            df['accountCode'] = None
        if 'receiptNote' not in df.columns:
            df['receiptNote'] = None
            

        url = "https://app.omie.com.br/api/v1/financas/pesquisartitulos/"
        headers = {"Content-Type": "application/json"}

        for index, row in df.iterrows():
            idmottu = str(row['identificadorMottu']).strip()
            cod_int = f"MULTA-{idmottu}-L"

            df.at[index, 'receiptReference'] = cod_int
            df.at[index, 'type'] = "Multa"
            df.at[index, 'accountCode'] = '4328825911'
            df.at[index, 'receiptNote'] = 'ND'

            payload = {
                "call": "PesquisarLancamentos",
                "param": [{
                    "nPagina": 1,
                    "nRegPorPagina": 500,
                    "cCodIntTitulo": cod_int
                }],
                "app_key": "886526446806",
                "app_secret": "cbf1506ead40d28a8653429a7e6aa0ed"
            }

            concluded = False
        
            while not concluded:
                response = requests.post(url, json=payload, headers=headers)
                print(f'Buscando dados na Omie, linha {index}')
                if response.status_code == 200:
                    data = response.json()
                    titulos = data.get("titulosEncontrados", [])
                    if titulos:
                        valor = titulos[0].get("cabecTitulo", {}).get("nValorTitulo", None)
                        df.at[index, 'amount'] = valor
                        concluded = True
                elif response.status_code == 429:
                    print("Too many requests, tentando novamente em 1 minuto")
                    time.sleep(60)
                    continue
                else:
                    error = response.json()
                    
                    if error.get("faultstring") == "ERROR: Código do Título informado na tag [cCodIntTitulo] não cadastrado!":
                        print(f"Multa {idmottu} não cadastrada na Omie")
                        registrar_id_na_planilha(idmottu)
                        df.drop(index, inplace=True)
                        concluded = True
                        continue
                    else:
                        print(f"Erro na requisição para {idmottu}: {response.status_code}")

        df.to_excel(caminho, index=False)
        print("Planilha atualizada com sucesso.")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        
def renomear_colunas_recebimento(caminho_arquivo):
    try:
        caminho = get_project_root() / caminho_arquivo
        
        df = pd.read_excel(caminho)
        df.columns = df.columns.str.strip() 

        colunas_renomear = {
            "Valor": "receiptReferenceAmount",
            "ReciboPagamentoDataAutenticacao": "receiptDate",
            "identificadorMottu": "multaId"
        }

        colunas_existentes = df.columns
        renomear = {k: v for k, v in colunas_renomear.items() if k in colunas_existentes}

        if not renomear:
            print("Nenhuma das colunas para renomear foi encontrada.")
            return

        df.rename(columns=renomear, inplace=True)
        df.to_excel(caminho, index=False)
        print("Colunas renomeadas e arquivo salvo com sucesso.")

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        
def formatar_receipt_date(caminho_arquivo):
    try:
        caminho = get_project_root() / caminho_arquivo

        df = pd.read_excel(caminho)

        if 'receiptDate' not in df.columns:
            raise ValueError("A coluna 'receiptDate' não foi encontrada na planilha.")

        def parse_data(data):
            try:
                return pd.to_datetime(data).strftime("%d/%m/%Y")
            except Exception:
                return data 

        df['receiptDate'] = df['receiptDate'].apply(parse_data)

        df.to_excel(caminho_arquivo, index=False)
        print(f"Datas formatadas com sucesso no arquivo: {caminho_arquivo}")

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")

def getBoletoId(date):
    
    nsu_list = extrair_nsu("teste.xlsx")
    
    nsu_values = ', '.join(f"'{item}'" for item in nsu_list)

    dsn = 'BigQuery'
    connection_string = f'DSN={dsn};'
    connection = pyodbc.connect(connection_string, autocommit=True)

    sql_query = f"""
        SELECT
            identificadorMottu,
            ReciboPagamentoDataAutenticacao,
            Valor,
            NSU
        FROM `z_ren_homologacao.extrato_rendimento_conciliacao`
        WHERE NSU IN ({nsu_values})
        AND ReciboPagamentoDataAutenticacao = '{date}' 
    """
        
    df = pd.read_sql(sql_query, connection)

    df.to_excel('resultado.xlsx', index=False)

    connection.close()
    
    exportar_nsu_nao_encontrados(nsu_list, 'resultado.xlsx')
    atualizar_valores_omie('resultado.xlsx')
    renomear_colunas_recebimento('resultado.xlsx')
    return df

# getBoletoId("2025-05-02")

# atualizar_valores_omie('resultado.xlsx')
# renomear_colunas_recebimento('resultado.xlsx')
