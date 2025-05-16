import requests
import re
import base64
import hashlib
import io
import zipfile
import os

import requests

def consultar_conta_pagar(app_key: str, app_secret: str, codigo_lancamento_integracao: str) -> int:

    url = 'https://app.omie.com.br/api/v1/financas/contapagar/'
    headers = {
        'Content-type': 'application/json',
    }
    payload = {
        'call': 'ConsultarContaPagar',
        'param': [
            {
                'codigo_lancamento_omie': 0,
                'codigo_lancamento_integracao': codigo_lancamento_integracao,
            },
        ],
        'app_key': app_key,
        'app_secret': app_secret,
    }

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code != 200:
        raise Exception(f'Erro na requisição: {response.status_code} - {response.text}')
    
    data = response.json()
    
    if 'codigo_lancamento_omie' not in data:
        raise Exception('Campo "codigo_lancamento_omie" não encontrado na resposta.')
    
    codigo_lancamento_omie = data['codigo_lancamento_omie']
    
    return codigo_lancamento_omie

def anexar_nf(card_data):
    print("\n=== Iniciando anexação de NF ===")
    print(f"Dados recebidos: {card_data}")
    
    app_key = 886526446806
    app_secret = "cbf1506ead40d28a8653429a7e6aa0ed"
    print("Chaves de API configuradas")

    print(f"\n1. Consultando conta a pagar para external reference: {card_data['externalReference']}")
    codigo_omie = consultar_conta_pagar(app_key, app_secret, card_data['externalReference'])
    print(f"Código Omie obtido: {codigo_omie}")

    nome_do_arquivo = f"{card_data['receiptNote']}.pdf"
    print(f"\n2. Nome do arquivo gerado: {nome_do_arquivo}")

    pdf_path = os.path.join('pdfs', nome_do_arquivo)
    print(f"Caminho completo do PDF: {pdf_path}")

    print("\n3. Tentando abrir o arquivo PDF...")
    try:
        with open(pdf_path, 'rb') as pdf_file:
            data = pdf_file.read()
        print(f"PDF lido com sucesso. Tamanho: {len(data)} bytes")
    except Exception as e:
        print(f"ERRO ao ler PDF: {str(e)}")
        raise

    print("\n4. Criando arquivo ZIP...")
    zip_buffer = io.BytesIO()
    try:
        with zipfile.ZipFile(zip_buffer, "a") as zip_file:
            zip_file.writestr(nome_do_arquivo, data)
        print("ZIP criado com sucesso")
    except Exception as e:
        print(f"ERRO ao criar ZIP: {str(e)}")
        raise

    print("\n5. Codificando conteúdo em base64...")
    try:
        content = base64.b64encode(zip_buffer.getvalue())
        print(f"Conteúdo codificado. Tamanho: {len(content)} bytes")
    except Exception as e:
        print(f"ERRO na codificação base64: {str(e)}")
        raise

    print("\n6. Gerando hash MD5...")
    try:
        hash_md5 = hashlib.md5(f"{content.decode('utf-8')}".encode("utf-8")).hexdigest()
        print(f"Hash MD5 gerado: {hash_md5}")
    except Exception as e:
        print(f"ERRO ao gerar hash MD5: {str(e)}")
        raise

    print("\n7. Preparando payload para envio...")
    payload = {
        "call": "IncluirAnexo",
        "app_key": f"{app_key}",
        "app_secret": f"{app_secret}",
        "param": [
            {
                "cCodIntAnexo": "",
                "cTabela": "conta-pagar",
                "nId": codigo_omie,
                "cNomeArquivo": nome_do_arquivo,
                "cTipoArquivo": "",
                "cArquivo": f"{content.decode('utf-8')}",
                "cMd5": f"{hash_md5}",
            }
        ],
    }
    print("Payload preparado")

    print("\n8. Enviando requisição para API Omie...")
    try:
        response = requests.post(
            url="https://app.omie.com.br/api/v1/geral/anexo/",
            headers={'Content-type': 'application/json'},
            json=payload,
        )
        print(f"Status code: {response.status_code}")
        print(f"Resposta: {response.text}")

        if (
            response.status_code == 200
            and response.json()["cDesStatus"] == "Anexo adicionado com sucesso!"
        ):
            print("\n✓ Anexo adicionado com sucesso!")
            return None

    except Exception as e:
        print(f"\nERRO na requisição: {str(e)}")
        error_json = {
            "card": card_data["id"],
            "error": "Não foi possível anexar a NF",
            "Detalhes": f"Mensagem: {e}",
        }
        return error_json

    print("\n✗ Falha na anexação")
    return {
        "card": card_data.get("id", "N/A"),
        "error": "Não foi possível anexar a NF",
        "Detalhes": f"Status: {response.status_code}, Resposta: {response.text}"
    }