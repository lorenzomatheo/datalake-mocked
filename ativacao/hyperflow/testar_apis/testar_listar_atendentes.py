import json

import requests

# DEV
# API_TOKEN = "seu-token"
# CODIGO_LOJA = "EAE521" #"W2R4TC"
# BASE_URL = "http://copilot.localhost"
# URL = f"{BASE_URL}/api/ativacao/lojas/{CODIGO_LOJA}/listar-atendentes"

# STAGING
API_TOKEN = "seu-token"
CODIGO_LOJA = "LKXYH4"
BASE_URL = "https://staging.api.maggu.ai/ativacao"
URL = f"{BASE_URL}/lojas/{CODIGO_LOJA}/listar-atendentes"


def listar_atendentes():
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    try:
        print(f"🔍 Listando atendentes da loja: {CODIGO_LOJA}")
        print(f"📡 URL: {URL}")
        print("-" * 50)

        response = requests.get(URL, headers=headers)

        print(f"📊 Status: {response.status_code}")

        if response.status_code == 200:
            atendentes = response.json()

            print(f"✅ Sucesso! Encontrados {len(atendentes)} atendentes:")
            with open("atendentes.json", "w", encoding="utf-8") as f:
                f.write(json.dumps(atendentes, indent=2, ensure_ascii=False))
            print("-" * 50)

            for i, atendente in enumerate(atendentes, 1):
                print(f"{i}. {atendente['nome']} (@{atendente['username']})")
                print(f"   ID: {atendente['id']}")
                print(f"   Cargo: {atendente['cargo']}")
                print(f"   Código Externo: {atendente['codigoExterno']}")
                print(f"   CPF: {atendente['cpf']}")
                print(f"   Telefone: {atendente['telefone']}")
                print(f"   Email: {atendente['email']}")
                print()

        elif response.status_code == 404:
            print(f"❌ Erro 404: {response.text}")

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


if __name__ == "__main__":
    print("🚀 Testador da API - Listar Atendentes")
    print("=" * 50)
    listar_atendentes()
