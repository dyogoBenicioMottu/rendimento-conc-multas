import pandas as pd
import requests
import time
import numpy as np
from anexar import anexar_nf
from getBoletoId import getBoletoId

APP_KEY = 886526446806
APP_SECRET = "cbf1506ead40d28a8653429a7e6aa0ed"
FA_API_URL = 'https://financial-accounting.mottu.cloud/api/account-payable'
OMIE_API_URL = 'https://app.omie.com.br/api/v1/financas'
BEARER_TOKEN =  "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJSLWdWNE1uOUNicll3cHBJY0dNUzc5STRjUHpSaU1iSkQ2NnFwaGJtSlJjIn0.eyJleHAiOjE3NDczMzk1NTQsImlhdCI6MTc0NzMxMDc1NCwianRpIjoiODgxMTRkNGUtYzE2OC00ZjhlLWJjZWItM2NkOWZiMThiODc2IiwiaXNzIjoiaHR0cHM6Ly9zc28ubW90dHUuY2xvdWQvcmVhbG1zL0ludGVybmFsIiwiYXVkIjpbIm1vdHR1LWFkbWluIiwiYWNjb3VudCJdLCJzdWIiOiIzNmQ5MTdkYy0yYWUzLTRlOGMtOTcyOS1hN2VjMWEyZTQ1NTUiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJtb3R0dS1hZG1pbiIsInNpZCI6IjczYTgxNmFkLWE3ODMtNGM4Yi05Mzk3LTQ0ZWU2NjczMWU2YiIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiKiJdLCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIG9wZW5pZCBtb3R0dS1zZXJ2aWNlIiwibmFtZSI6IkR5b2dvIEJyaXRvIiwibGRhcElkIjoiNjI1ZGRlMTctOWUyMi00NjgxLWI4OTEtMTVjZDFjMTBiZWUxIiwiaWQiOjI1MzIwNDcsInByZWZlcnJlZF91c2VybmFtZSI6ImR5b2dvLmJyaXRvIiwiZ2l2ZW5fbmFtZSI6IkR5b2dvIiwiZmFtaWx5X25hbWUiOiJCcml0byIsImVtYWlsIjoiZHlvZ28uYnJpdG9AbW90dHUuY29tLmJyIn0.cFNWVe4QWrHnoNHfB9zXMYlDCBBuevzefIdIPpO1MxfIs8GhyY3vVx_clJFSmQxq6-qfty6c6uMpnp6Z-oNRUtdj_QAbC8munwgTiiwoLUONywRtwInMyAlpmt6pYhkwXORStNWBJWEqdSIQcVe48aWVC6ZX-gLJMsmTjlbw_eRyE5w7dQzupp-ndZVvMYrGWyRc2c9fZGGnwZGf-YMnN8sC5CzptjZgjKHQnwpdk9S10x2nvTPcjFRa1wrRDIb5pKD9JMXlXxNuFvWZxtosTHUloYcQHBpN3BKtMnXKZuNQ29OsjPiR7q_Wxzd04v_MXoUxejcKaekJ66K_OhHWPw"

def delay(seconds=1):
    time.sleep(seconds)

def format_date(date):
    return date.strftime('%Y-%m-%dT00:00:00')

def safe_float(value):
    return float(value) if value is not None else 0.0

def upsertCA_FA(row):
    headers = {'Authorization': f'Bearer {BEARER_TOKEN}', 'X-Tenant': 'rental', 'Content-Type': 'application/json'}
    if row['type']== "Multa":
        data = {
            "documentTitle": row['documentTitle'],
            "documentType": row['documentType'],
            "externalReference": row['externalReference'],
            "dueDate": format_date(row['dueDate']),
            "expectedPaymentDate": format_date(row['expectedPaymentDate']),
            "issueDate": format_date(row['issueDate']),
            "registerDate": format_date(row['registerDate']),
            "categoryCode": row['categoryCode'],
            "accountCode": str(row['accountCode']),
            "clientExternalReference": row['clientExternalReference'],
            "amount": round(row['amount'],2),
            "note": row['note'],
            "taxDocumentNumber": row['externalReference']
        }
    else:
        data = {
            "documentTitle": row['documentTitle'],
            "documentType": row['documentType'],
            "externalReference": row['externalReference'],
            "dueDate": format_date(row['dueDate']),
            "expectedPaymentDate": format_date(row['expectedPaymentDate']),
            "issueDate": format_date(row['issueDate']),
            "registerDate": format_date(row['registerDate']),
            "categoryCode": row['categoryCode'],
            "accountCode": str(row['accountCode']),
            "clientExternalReference": row['clientExternalReference'],
            "amount": round(row['amount'],2),
            "note": row['note'],
            "taxDocumentNumber": row['documentTitle']
        }

    print(f"\nDados recebidos: {data}")

    response = requests.post(FA_API_URL, headers=headers, json=data)
    print(response.content)
    return response.ok

def realizarBaixa_FA(row):
   
    headers = {'Authorization': f'Bearer {BEARER_TOKEN}', 'X-Tenant': 'rental', 'Content-Type': 'application/json'}
    url = f'{FA_API_URL}/{row["receiptReference"]}/payment'
    
    print(url)

    if row['type']== "Multa":
        if row['receiptReferenceAmount'] < row['amount']:
            print('Tem desconto')
            data = {
                    "externalReference": f'MULTA-{row["multaId"]}-B1',
                    "accountCode": str(row['accountCode']),
                    "amount": round(row['receiptReferenceAmount'],2),
                    "date": format_date(row['receiptDate']),
                    "note": row['receiptNote'],
                }
            
            print(data)
            response = requests.post(url, headers=headers, json=data)
            print(response.json())

            additional_data = {
                "externalReference": f'MULTA-{row["multaId"]}-B2',
                "accountCode": '4403816966',
                "amount": round(row['amount'] - row['receiptReferenceAmount'],2),
                "date": format_date(row['receiptDate']),
                "note": row['receiptNote'] , 
            }
            
            print(additional_data)
            #retornov2 = update_v2_pagamentodata(row)
            additional_response = requests.post(url, headers=headers, json=additional_data)
            print(additional_response.json())

            # add update


            return additional_response.ok and response.ok

        elif row['receiptReferenceAmount'] >= row['amount']:
            print('Não tem desconto')
            # Interesse por Atraso
            overdue_data = {
                "externalReference": f'MULTA-{row["multaId"]}-B1',
                "accountCode": str(row['accountCode']),
                "amount": round(row['receiptReferenceAmount'],2),
                "interestOverduePaymentAmount": round(row['receiptReferenceAmount'] - row['amount'],2),
                "date": format_date(row['receiptDate']),
                "note": row['receiptNote'] 
            }
            #retornov2 =  update_v2_pagamentodata(row)
            overdue_response = requests.post(url, headers=headers, json=overdue_data)
            print(overdue_data)
            if overdue_response.content:
                try:
                    print(overdue_response.json())
                except ValueError as e:
                    print("Resposta não é um JSON válido:", overdue_response.content)
            else:
                print("Resposta vazia:", overdue_response.status_code)

            return overdue_response.ok
    
    else:
        # return_response = anexar_nf(row)

        data = {
            "externalReference": row["receiptReference"],
            "accountCode": str(row['accountCode']),
                "amount": round(row['receiptReferenceAmount'],2),
                "date": format_date(row['receiptDate']),
                "note": row['receiptNote'] if row['receiptNote'] else 'ND', 
                "interestOverduePaymentAmount": row['interestOverduePaymentAmount'],
                "mulctAmount": row['mulctAmount'],
                "discountAmount": row['discountAmount'],
            }
        #retornov2 = update_v2_pagamentodata(row)
        response = requests.post(url, headers=headers, json=data)

        return response.ok

if __name__ == '__main__':

    data_types = {
        'type': str,
        'multaId': int,
        'externalReference': str,
        'receiptReference': str,
        'receiptDate': 'datetime64[ns]',
        'receiptReferenceAmount': float,
        'receiptNote': str,
        'accountCode': str,
        'documentTitle': str,
        'documentType': str,
        'dueDate': 'datetime64[ns]',
        'expectedPaymentDate': 'datetime64[ns]',
        'issueDate': 'datetime64[ns]',
        'registerDate': 'datetime64[ns]',
        'categoryCode': str,
        'clientExternalReference': str,
        'amount': float,
        'note': str,    
        'interestOverduePaymentAmount': float,
        'mulctAmount': float,
        'discountAmount': float
    }
    
    getBoletoId("2025-05-02")
    
    df = pd.read_excel(r"resultado.xlsx",dtype=data_types)
    
    # Criar arquivo de log
    log_file = open('processamento_log.txt', 'w', encoding='utf-8')
    
    for index, row in df.iterrows():
        try:
            print(f'Processando linha {index+1}')
            log_file.write(f"\n\n=== Processando linha {index+1} ===\n")
            log_file.write(f"External Reference: {row['receiptReference']}\n")
            
            # upsertCA_FA(row)
            baixa_response = realizarBaixa_FA(row)
            
            # # Verificar se a baixa foi bem sucedida
            if baixa_response:
                log_file.write(f"Baixa realizada com sucesso\n")
            else:
                log_file.write(f"Falha ao realizar a baixa\n")
            
            time.sleep(0.5)
        except Exception as e:
            error_msg = f'Erro ao processar linha {index+1}: {str(e)}'
            print(error_msg)
            log_file.write(f"✗ {error_msg}\n")
    
    log_file.close()