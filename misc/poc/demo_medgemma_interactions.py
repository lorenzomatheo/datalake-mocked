# Databricks notebook source
# MAGIC %md
# MAGIC # MedGemma Vertex AI Endpoint - Demo de Interações
# MAGIC
# MAGIC Este notebook demonstra diferentes formas de interagir com o endpoint MedGemma no Vertex AI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configurar Credenciais

# COMMAND ----------

import json
import os

import requests

# COMMAND ----------

# Configurar credenciais do Databricks
GOOGLE_APPLICATION_CREDENTIALS = json.loads(
    dbutils.secrets.get(scope="databricks", key="GOOGLE_VERTEX_CREDENTIALS")
)

credentials_path = "/dbfs/tmp/gcp_credentials.json"
with open(credentials_path, "w") as f:
    json.dump(GOOGLE_APPLICATION_CREDENTIALS, f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
print(f"✓ Credenciais configuradas em: {credentials_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importar Funções do Módulo

# COMMAND ----------

from misc.poc.poc_medgemma_vertex_endpoint import (
    ask_medgemma,
    call_medgemma_endpoint,
)

print("✓ Módulo importado com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exemplo 1: Pergunta Simples
# MAGIC
# MAGIC Forma mais básica de usar o endpoint - apenas uma pergunta direta.

# COMMAND ----------

question = "Qual a posologia do Doril?"
print(f"Pergunta: {question}\n")

response = ask_medgemma(question, max_tokens=300)
print(f"Resposta:\n{response}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exemplo 2: Conversa Multi-turno
# MAGIC
# MAGIC Mantém o contexto da conversa através de múltiplas interações.

# COMMAND ----------

# Primeira pergunta
conversation = [
    {"role": "user", "content": "O que é paracetamol?"},
]

print("Turno 1:")
print(f"Usuário: {conversation[0]['content']}\n")

response_1 = call_medgemma_endpoint(conversation, max_tokens=200)
print(f"Modelo: {response_1}\n")

# Adicionar resposta do modelo à conversa
conversation.append({"role": "model", "content": response_1})

# Segunda pergunta (com contexto)
conversation.append(
    {"role": "user", "content": "Qual a dose máxima diária para adultos?"}
)

print("=" * 80)
print("Turno 2:")
print(f"Usuário: {conversation[2]['content']}\n")

response_2 = call_medgemma_endpoint(conversation, max_tokens=200)
print(f"Modelo: {response_2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exemplo 3: Múltiplas Perguntas Médicas
# MAGIC
# MAGIC Processar várias perguntas diferentes sobre medicamentos.

# COMMAND ----------

questions = [
    "Quais são as contraindicações do ibuprofeno?",
    "Como deve ser armazenado o medicamento dipirona?",
    "Quais os efeitos colaterais comuns da amoxicilina?",
    "O que é hipertensão arterial?",
]

for i, question in enumerate(questions, 1):
    print(f"\n{'=' * 80}")
    print(f"Pergunta {i}: {question}")
    print("-" * 80)

    response = ask_medgemma(question, max_tokens=150)
    print(f"Resposta: {response}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exemplo 4: Pergunta com Contexto
# MAGIC
# MAGIC Fazer perguntas complexas com cenários específicos de pacientes.

# COMMAND ----------

messages = [
    {
        "role": "user",
        "content": "Tenho um paciente de 45 anos com diabetes tipo 2. Quais cuidados devo ter ao prescrever anti-inflamatórios?",
    },
]

print(f"Contexto e Pergunta:\n{messages[0]['content']}\n")
print("=" * 80)

response = call_medgemma_endpoint(messages, max_tokens=400)
print(f"Resposta:\n{response}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exemplo 5: Processamento em Lote
# MAGIC
# MAGIC Consultar informações de múltiplos medicamentos de forma sistemática.

# COMMAND ----------

drug_list = ["Paracetamol", "Dipirona", "Ibuprofeno", "Aspirina", "Amoxicilina"]

print("Consultando informações de posologia para múltiplos medicamentos...\n")
print("=" * 80)

results = {}
for drug in drug_list:
    question = f"Qual a posologia usual de {drug} para adultos?"
    print(f"\nConsultando: {drug}...", end=" ")

    try:
        response = ask_medgemma(question, max_tokens=150)
        results[drug] = response
        print("✓")
    except requests.exceptions.RequestException as e:
        results[drug] = f"Erro: {e}"
        print("✗")

print("\n" + "=" * 80)
print("RESULTADOS:")
print("=" * 80)

for drug, info in results.items():
    print(f"\n{drug}:")
    print("-" * 80)
    print(info)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exemplo 6: Teste Personalizado
# MAGIC
# MAGIC Use esta célula para testar suas próprias perguntas.

# COMMAND ----------

# Modifique a pergunta abaixo para testar
my_question = "Qual a diferença entre paracetamol e dipirona?"

print(f"Sua Pergunta: {my_question}\n")
print("=" * 80)

my_response = ask_medgemma(my_question, max_tokens=300)
print(f"Resposta:\n{my_response}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exemplo 7: Conversa Interativa Completa
# MAGIC
# MAGIC Simula uma consulta completa sobre um medicamento.

# COMMAND ----------

# Iniciar conversa sobre um medicamento específico
medication = "Losartana"
chat_history = []

# Pergunta 1: Indicação
q1 = f"Para que serve o medicamento {medication}?"
chat_history.append({"role": "user", "content": q1})

print("Pergunta 1:", q1)
r1 = call_medgemma_endpoint(chat_history, max_tokens=200)
print("Resposta:", r1)
chat_history.append({"role": "model", "content": r1})

print("\n" + "=" * 80 + "\n")

# Pergunta 2: Posologia (com contexto)
q2 = "Qual a dose recomendada?"
chat_history.append({"role": "user", "content": q2})

print("Pergunta 2:", q2)
r2 = call_medgemma_endpoint(chat_history, max_tokens=200)
print("Resposta:", r2)
chat_history.append({"role": "model", "content": r2})

print("\n" + "=" * 80 + "\n")

# Pergunta 3: Efeitos colaterais (com contexto)
q3 = "Quais são os principais efeitos colaterais?"
chat_history.append({"role": "user", "content": q3})

print("Pergunta 3:", q3)
r3 = call_medgemma_endpoint(chat_history, max_tokens=200)
print("Resposta:", r3)
chat_history.append({"role": "model", "content": r3})

print("\n" + "=" * 80)
print(f"Conversa completa sobre {medication} finalizada!")
print(f"Total de turnos: {len([m for m in chat_history if m['role'] == 'user'])}")
