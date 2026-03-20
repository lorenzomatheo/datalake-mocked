import json

import requests

# DEV
# API_TOKEN = "seu-token"
# CODIGO_LOJA = "EAE521"
# USERNAME = "balconista"
# BASE_URL = "http://copilot.localhost"
# URL = f"{BASE_URL}/api/ativacao/lojas/{CODIGO_LOJA}/atendentes/{USERNAME}/atualizar"

# STAGING
API_TOKEN = "seu-token"
CODIGO_LOJA = "LKXYH4"
USERNAME = "gilberto.caray"
BASE_URL = "https://staging.api.maggu.ai"
URL = f"{BASE_URL}/ativacao/lojas/{CODIGO_LOJA}/atendentes/{USERNAME}/atualizar"


def atualizar_atendente():
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    # Dados para atualizar o atendente
    dados_atualizacao = {
        "nome": "Gilberto Caray",
        "cargo": "BALCONISTA",
        "cpf": "44526931802",
        "telefone": "11999998888",  # Sem formatação, máximo 11 caracteres
        "email": "teste.atualizado@maggu.com.br",
    }

    try:
        print(f"🔄 Atualizando atendente: {USERNAME}")
        print(f"🏪 Loja: {CODIGO_LOJA}")
        print(f"📡 URL: {URL}")
        print(
            f"📝 Dados: {json.dumps(dados_atualizacao, indent=2, ensure_ascii=False)}"
        )
        print("-" * 70)

        response = requests.put(URL, headers=headers, json=dados_atualizacao)

        print(f"📊 Status: {response.status_code}")

        if response.status_code == 200:
            atendente_atualizado = response.json()

            print("✅ Sucesso! Atendente atualizado:")
            print("-" * 50)
            print(f"ID: {atendente_atualizado['id']}")
            print(f"Nome: {atendente_atualizado['nome']}")
            print(f"Username: {atendente_atualizado['username']}")
            print(f"Cargo: {atendente_atualizado['cargo']}")
            print(f"Código Externo: {atendente_atualizado.get('codigoExterno', 'N/A')}")
            print(f"CPF: {atendente_atualizado.get('cpf', 'N/A')}")
            print(f"Telefone: {atendente_atualizado.get('telefone', 'N/A')}")
            print(f"Email: {atendente_atualizado.get('email', 'N/A')}")

        elif response.status_code == 404:
            error_data = response.text
            print(f"❌ Erro 404: {error_data}")

        elif response.status_code == 400:
            error_data = response.json()
            print("❌ Erro 400: Dados inválidos")
            print(f"Detalhes: {json.dumps(error_data, indent=2, ensure_ascii=False)}")

        elif response.status_code == 401:
            print("❌ Erro 401: Token de autenticação inválido")

        else:
            print(f"❌ Erro {response.status_code}")
            error_data = response.json()
            print(f"Detalhes: {json.dumps(error_data, indent=2, ensure_ascii=False)}")

    except requests.exceptions.ConnectionError as e:
        print(f"❌ Erro de conexão: {e}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na requisição: {e}")


def listar_atendentes_antes_depois():
    """Lista os atendentes antes e depois da atualização para verificar a mudança"""
    print("\n" + "=" * 70)
    print("📋 LISTANDO ATENDENTES ANTES DA ATUALIZAÇÃO")
    print("=" * 70)

    url_listar = f"{BASE_URL}/api/ativacao/lojas/{CODIGO_LOJA}/listar-atendentes"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    response = requests.get(url_listar, headers=headers)
    if response.status_code == 200:
        atendentes = response.json()
        for atendente in atendentes:
            if atendente['username'] == USERNAME:
                print(f"➡️  {atendente['nome']} (@{atendente['username']})")
                print(f"    Cargo: {atendente['cargo']}")
                print(f"    Email: {atendente.get('email', 'N/A')}")
                break


if __name__ == "__main__":
    print("🚀 Testador da API - Atualizar Atendente")
    print("=" * 70)

    listar_atendentes_antes_depois()

    print("\n" + "=" * 70)
    print("🔄 EXECUTANDO ATUALIZAÇÃO")
    print("=" * 70)
    atualizar_atendente()

    print("\n" + "=" * 70)
    print("📋 LISTANDO ATENDENTES APÓS A ATUALIZAÇÃO")
    print("=" * 70)
    listar_atendentes_antes_depois()
