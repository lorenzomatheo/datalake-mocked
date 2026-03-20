# Databricks notebook source
# MAGIC %md
# MAGIC # Avaliação de Produtos Intercambiáveis
# MAGIC
# MAGIC Este notebook utiliza um agente Agno com Gemini para avaliar se produtos são intercambiáveis
# MAGIC de acordo com as regras da Anvisa, analisando especialmente dosagens e princípios ativos.

# COMMAND ----------

# MAGIC %pip install agno google-genai langchain-google-vertexai langchain_google_genai vertexai pydantic langsmith
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import time
from enum import Enum
from textwrap import dedent
from typing import Any

import gspread
import pyspark.sql.functions as F
import requests
from agno.agent import Agent
from agno.models.google import Gemini
from google.oauth2.service_account import Credentials
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import SparkSession

from maggulake.llm.agno_tracer import (
    TracedAgent,
)
from maggulake.llm.models import (
    get_model_name,
    setup_vertex_ai_credentials,
)
from maggulake.llm.vertex import setup_langsmith

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modelo Pydantic para Estrutura de Resposta

# COMMAND ----------


class ResultadoAvaliacao(str, Enum):
    CORRETO = "CORRETO"
    INCORRETO = "INCORRETO"


class AvaliacaoIntercambiabilidade(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    avaliacao: ResultadoAvaliacao = Field(
        description="""Resultado da avaliação de intercambiabilidade entre os dois produtos.

        Use CORRETO quando:
        - Mesmo princípio ativo e mesma dosagem (para medicamentos de prescrição)
        - Mesma forma farmacêutica e via de administração
        - Produtos OTC/MIP similares com pequenas variações aceitáveis

        Use INCORRETO quando:
        - Princípios ativos diferentes (exceto OTC/MIP que podem conter princípios ativos ligeiramente diferentes)
        - Dosagens diferentes (exceto OTC/MIP que podem ter concentrações diferentes)
        - Formas farmacêuticas incompatíveis (ex: comprimido vs cápsula)
        - Vias de administração diferentes (ex: oral vs tópico)
        """
    )
    motivo: str = Field(
        description="""Explicação técnica e detalhada justificando a avaliação.

        Bons motivos devem incluir:
        - Comparação específica dos princípios ativos encontrados
        - Análise das dosagens/concentrações presentes nos nomes
        - Verificação da compatibilidade de forma farmacêutica e via de administração
        - Citação dos critérios da Anvisa que foram atendidos ou violados

        Exemplos de bons motivos:
        - "CORRETO: Ambos contêm metildopa 500mg, forma farmacêutica comprimido revestido, via oral.
          Atendem todos os critérios de intercambiabilidade da Anvisa (mesmo princípio ativo, mesma dosagem,
          mesma forma farmacêutica e via de administração). A diferença na quantidade de comprimidos por caixa
          não afeta a intercambiabilidade."
        - "INCORRETO: Produtos não são compatíveis"
        """
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Spark

# COMMAND ----------

spark = SparkSession.builder.appName("avalia_intercambiaveis").getOrCreate()
spark.conf.set("maggu.app.name", "avalia_intercambiaveis")

setup_langsmith(dbutils, spark, stage="prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração das Credenciais Google

# COMMAND ----------

GOOGLE_APPLICATION_CREDENTIALS = json.loads(
    dbutils.secrets.get(scope="databricks", key="GOOGLE_VERTEX_CREDENTIALS")
)

credentials_path = "/dbfs/tmp/gcp_credentials.json"
with open(credentials_path, "w") as f:
    json.dump(GOOGLE_APPLICATION_CREDENTIALS, f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

json_txt = dbutils.secrets.get("databricks", "GOOGLE_SHEETS_PBI")
sheets_creds = Credentials.from_service_account_info(
    json.loads(json_txt),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ],
)

gc = gspread.authorize(sheets_creds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Agente Gemini

# COMMAND ----------

project_id, location = setup_vertex_ai_credentials(spark)
# TODO: trocar para SMALL quando os créditos do google acabarem
GEMINI_MODEL = get_model_name(provider="gemini_vertex", size="LARGE")

agente_intercambiaveis = TracedAgent(
    Agent(
        model=Gemini(
            id=GEMINI_MODEL,
            vertexai=True,
            project_id=project_id,
            location=location,
            include_thoughts=False,
            search=False,
        ),
        output_schema=AvaliacaoIntercambiabilidade,
        markdown=False,
        reasoning=False,
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração da Planilha Google

# COMMAND ----------

SHEET_ID = "1oI8hHKXvNNwV2PtAUMCf8V2Jwd0H-LduJ3mVRTFxc7U"
ABA = "produtos_intercambiaveis"

worksheet = gc.open_by_key(SHEET_ID).worksheet(ABA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Busca de informações adicionais dos produtos

# COMMAND ----------


def extrair_eans_e_dados_planilha() -> tuple[list[str], list[dict[str, Any]]]:
    all_records = worksheet.get_all_records()

    eans_unicos = set()
    for record in all_records:
        ean_ref = str(record.get('ean_referencia', '')).strip()
        ean_sim = str(record.get('ean_similar', '')).strip()

        if ean_ref:
            eans_unicos.add(ean_ref)
        if ean_sim:
            eans_unicos.add(ean_sim)

    eans_lista = list(eans_unicos)
    return eans_lista, all_records


def carregar_info_produtos(eans_necessarios: list = None):
    print("Carregando base de dados com informações adicionais dos produtos")
    return (
        spark.read.table("production.refined.produtos_refined")
        .filter(F.col("ean").isin(eans_necessarios))
        .select(
            "ean",
            "principio_ativo",
            "forma_farmaceutica",
            "via_administracao",
            "tipo_medicamento",
            "eh_otc",
        )
        .cache()
    )


def buscar_info_produto(produtos_df, ean: str) -> dict:
    produto_info = produtos_df.filter(F.col("ean") == ean).collect()

    if produto_info:
        row = produto_info[0]
        info = {}

        if row.principio_ativo:
            info["principio_ativo"] = row.principio_ativo
        if row.forma_farmaceutica:
            info["forma_farmaceutica"] = row.forma_farmaceutica
        if row.via_administracao:
            info["via_administracao"] = row.via_administracao
        if row.tipo_medicamento:
            info["tipo_medicamento"] = row.tipo_medicamento
        if row.eh_otc is not None:
            info["eh_otc"] = row.eh_otc

        return info
    else:
        print(f"⚠️  Produto com EAN {ean} não encontrado")
        return {}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do Prompt para Avaliação

# COMMAND ----------


def formatar_info_produto(nome: str, ean: str, info: dict) -> str:
    linhas = [f"- Nome: {nome}", f"- EAN: {ean}"]

    if "principio_ativo" in info:
        linhas.append(f"- Princípio Ativo: {info['principio_ativo']}")
    if "forma_farmaceutica" in info:
        linhas.append(f"- Forma Farmacêutica: {info['forma_farmaceutica']}")
    if "via_administracao" in info:
        linhas.append(f"- Via de Administração: {info['via_administracao']}")
    if "eh_otc" in info:
        linhas.append(f"- É OTC: {info['eh_otc']}")

    return "\n".join(linhas)


def criar_prompt_avaliacao(
    ean_referencia: str,
    nome_referencia: str,
    ean_similar: str,
    nome_similar: str,
    info_produto1: dict,
    info_produto2: dict,
) -> str:
    produto1_info = formatar_info_produto(
        nome_referencia, ean_referencia, info_produto1
    )
    produto2_info = formatar_info_produto(nome_similar, ean_similar, info_produto2)

    return dedent(f"""
        Você é um especialista em regulamentação farmacêutica da Anvisa
        (Agência Nacional de Vigilância Sanitária) do Brasil.

        Sua tarefa é avaliar se dois medicamentos são INTERCAMBIÁVEIS de acordo com as regras da Anvisa.

        INFORMAÇÕES DOS PRODUTOS:

        PRODUTO 1:
        {produto1_info}

        PRODUTO 2:
        {produto2_info}

        CRITÉRIOS DA ANVISA PARA INTERCAMBIALIDADE:
        1. Mesmo princípio ativo (exceto para produtos over-the-counter "OTCs" ou medicamentos isentos
           de prescrição "MIPs", que podem ter princípios ativos ligeiramente diferentes)
        2. Mesma concentração/dosagem (exceto para produtos OTCs / MIPs que podem ter concentrações
           diferentes)
        3. Mesma forma farmacêutica
        4. Mesma via de administração

        IMPORTANTE - DIFERENÇA ENTRE DOSAGEM E QUANTIDADE:
        - DOSAGEM/CONCENTRAÇÃO: é a quantidade de princípio ativo por unidade (ex: 500mg, 250mg, 10mg/ml)
          → Exemplo: "Dipirona 500mg" vs "Dipirona 1g" têm dosagens diferentes
        - QUANTIDADE/VOLUME: é o número de unidades na embalagem (ex: caixa com 30, 60 ou 100 comprimidos)
          → Exemplo: "Caixa com 30 comprimidos" vs "Caixa com 60 comprimidos"
          → A quantidade NÃO afeta a intercambiabilidade

        IMPORTANTE - FORMA FARMACÊUTICA:
        A forma farmacêutica é a apresentação física do medicamento.

        Exemplos de formas farmacêuticas:
        - Comprimido / Comprimido revestido / Comprimido mastigável
        - Cápsula / Cápsula gelatinosa
        - Solução oral / Suspensão oral / Xarope
        - Pomada / Creme / Gel
        - Gotas / Spray

        NÃO são intercambiáveis formas diferentes:
        → "Comprimido" vs "Cápsula" = INCORRETO (formas diferentes)
        → "Solução oral" vs "Suspensão" = INCORRETO (formas diferentes)
        → "Pomada" vs "Creme" = INCORRETO (formas diferentes)

        SÃO intercambiáveis formas iguais:
        → "Comprimido" vs "Comprimido" = pode ser CORRETO (mesma forma)
        → "Comprimido revestido" vs "Comprimido revestido" = pode ser CORRETO (mesma forma)

        INSTRUÇÕES:
        - Compare os princípios ativos dos dois produtos (se disponíveis)
        - Verifique se as formas farmacêuticas são EXATAMENTE IGUAIS (se disponíveis)
        - Analise se as vias de administração são IGUAIS (se disponíveis)
        - Considere as DOSAGENS (mg, g, ml, etc.) presentes nos nomes dos produtos
        - Para produtos não-medicamentos, avalie se são do mesmo tipo/categoria
        - Se informações estruturadas estão faltando, baseie-se no nome do produto
        - NÃO considere a QUANTIDADE de unidades na embalagem como fator determinante

        RESPOSTA OBRIGATÓRIA:
        Você deve responder com uma estrutura JSON contendo:
        - avaliacao: "CORRETO" se são intercambiáveis ou "INCORRETO" se não são intercambiáveis
        - motivo: Explicação detalhada do porquê da avaliação, considerando os critérios da Anvisa

        Seja específico no motivo, mencionando quais critérios foram atendidos ou não
        (princípio ativo, dosagem, forma farmacêutica, via de administração).
    """).strip()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Função para Avaliar um Par de Produtos

# COMMAND ----------


def avaliar_intercambiabilidade(
    produtos_df,
    ean_referencia: str,
    nome_referencia: str,
    ean_similar: str,
    nome_similar: str,
) -> tuple[str, str]:
    info_produto1 = buscar_info_produto(produtos_df, ean_referencia)
    info_produto2 = buscar_info_produto(produtos_df, ean_similar)

    prompt = criar_prompt_avaliacao(
        ean_referencia,
        nome_referencia,
        ean_similar,
        nome_similar,
        info_produto1,
        info_produto2,
    )

    run_output = agente_intercambiaveis.run(prompt)

    if hasattr(run_output, 'content') and hasattr(run_output.content, 'avaliacao'):
        avaliacao = run_output.content.avaliacao
        motivo = run_output.content.motivo.strip()
        return avaliacao, motivo
    else:
        print(f"Formato de resposta inesperado: {run_output}")
        return 'ERRO', "Erro no formato da resposta"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento Principal

# COMMAND ----------


def processar_avaliacoes():
    eans_necessarios, all_records = extrair_eans_e_dados_planilha()

    if not eans_necessarios:
        dbutils.notebook.exit("Nenhum EAN encontrado na planilha")

    registros_para_avaliar = []
    for i, record in enumerate(all_records, start=2):  # start=2 porque linha 1 é header
        avaliacao_atual = record.get('avaliacao', '').strip()
        if not avaliacao_atual:
            registros_para_avaliar.append(
                {
                    'linha': i,
                    'ean_referencia': record.get('ean_referencia', ''),
                    'nome_referencia': record.get('nome_referencia', ''),
                    'ean_similar': record.get('ean_similar', ''),
                    'nome_similar': record.get('nome_similar', ''),
                }
            )

    print(f"Registros para avaliar: {len(registros_para_avaliar)}")

    if not registros_para_avaliar:
        dbutils.notebook.exit("Todos os registros já foram avaliados")

    produtos_df = carregar_info_produtos(eans_necessarios)

    total_processados = 0
    total_erros = 0

    for registro in registros_para_avaliar:
        try:
            print(f"Processando linha {registro['linha']}...")

            avaliacao, motivo = avaliar_intercambiabilidade(
                produtos_df,
                registro['ean_referencia'],
                registro['nome_referencia'],
                registro['ean_similar'],
                registro['nome_similar'],
            )

            if avaliacao != 'ERRO':
                # Atualizar a planilha - colunas E (avaliação) e F (motivo)
                linha = registro['linha']
                range_update = f"E{linha}:F{linha}"
                values_update = [[avaliacao, motivo]]

                worksheet.update(range_name=range_update, values=values_update)
                print(f"✓ Linha {linha}: {avaliacao}")
                print(f"  Motivo: {motivo[:100]}...")
                total_processados += 1

                # Pausa para evitar rate limiting
                time.sleep(1)
            else:
                print(f"✗ Erro na linha {registro['linha']}: {motivo}")
                total_erros += 1

        except requests.RequestException as e:
            print(f"✗ Erro ao processar linha {registro['linha']}: {str(e)}")
            total_erros += 1
            continue

    print("\n=== RESUMO ===")
    print(f"Total processados com sucesso: {total_processados}")
    print(f"Total com erros: {total_erros}")
    print("Processamento concluído!")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste do Agente

# COMMAND ----------

eans_teste = ['7895858005924', '7898470686199']
produtos_df_teste = carregar_info_produtos(eans_teste)

avaliacao_teste, motivo_teste = avaliar_intercambiabilidade(
    produtos_df_teste,
    "7895858005924",
    "Aldomet 500mg, Caixa Com 30 Comprimidos Revestidos",
    "7898470686199",
    "Tensioval 500mg, Caixa Com 490 Comprimidos Revestidos",
)

print(f"Avaliação: {avaliacao_teste}")
print(f"Motivo: {motivo_teste}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar Processamento

# COMMAND ----------

processar_avaliacoes()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação Final

# COMMAND ----------

all_records_final = worksheet.get_all_records()
registros_pendentes = sum(
    1 for record in all_records_final if not record.get('avaliacao', '').strip()
)

print(f"Registros ainda pendentes de avaliação: {registros_pendentes}")
