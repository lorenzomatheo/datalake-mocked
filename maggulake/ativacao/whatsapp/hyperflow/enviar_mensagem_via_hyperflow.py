import requests

# TODO: permitir envio em batches para economizar chamadas do HF
# TODO: deixar privado o endpoint no "API gateway" do HF


def enviar_mensagem_whatsapp_via_hyperflow(
    endpoint: str, client_id: str, payload: dict
) -> dict:
    headers = {"client_id": client_id}
    response = requests.post(endpoint, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    return response


def enviar_mensagem_whatsapp_via_hyperflow_regua(client_id: str, payload: dict) -> dict:
    url = "https://runtime.hyperflowapis.global/api/ummqdEpYx/regua"
    headers = {"client_id": client_id}
    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    return response
