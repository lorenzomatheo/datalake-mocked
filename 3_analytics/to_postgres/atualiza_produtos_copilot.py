# Databricks notebook source
# MAGIC %pip install ftfy
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import random
import time
import uuid
from datetime import datetime

import boto3
import pyspark.sql.functions as F
import requests
from botocore.exceptions import BotoCoreError
from ftfy import fix_text
from tqdm.notebook import tqdm

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.utils.iters import batched

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "atualiza_produtos_copilot",
    dbutils,
    widgets={"full_refresh": ["false", "true"]},
)

# COMMAND ----------

full_refresh = dbutils.widgets.get("full_refresh") == "true"
checkpoint_key = "checkpoints/ultimo_update_copilot.txt"

temp_uuid = str(uuid.uuid4())
temp_path = f"/tmp/maggu/produtos_copilot_{temp_uuid}"

# COMMAND ----------

url_by_stage = {
    "staging": "https://staging.copilot.maggu.ai",
    "production": "https://copilot.maggu.ai",
}
maggu_copilot_url = url_by_stage[env.settings.catalog]

secrets_scope_by_stage = {
    "staging": "staging.copilot.maggu.ai",
    "production": "copilot.maggu.ai",
}
secrets_scope = secrets_scope_by_stage[env.settings.catalog]
copilot_token = dbutils.secrets.get(scope=secrets_scope, key="COPILOT_MAGGU_API_TOKEN")

# COMMAND ----------

s3_client = boto3.client("s3")


def save_last_updated_at(timestamp):
    raw = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f").encode()
    s3_client.put_object(Body=raw, Bucket=env.settings.bucket, Key=checkpoint_key)


def get_last_updated_at():
    try:
        response = s3_client.get_object(Bucket=env.settings.bucket, Key=checkpoint_key)
        raw = response["Body"].read()
        return datetime.strptime(raw.decode(), "%Y-%m-%d %H:%M:%S.%f")
    except (BotoCoreError, ValueError):
        return None


# COMMAND ----------

now = datetime.now()

last_updated_at = (
    get_last_updated_at() or datetime.fromtimestamp(0)
    if not full_refresh
    else datetime.fromtimestamp(0)
)

last_updated_at

# COMMAND ----------

produtos_refined = (
    env.table(Table.produtos_refined)
    .filter(f"atualizado_em > '{last_updated_at}'")
    .filter("status = 'ativo'")
    .dropDuplicates(['ean'])
    .drop('id', 'bula')
    .cache()
)

if not produtos_refined.take(1):
    raise RuntimeError("Nenhum produto ativo encontrado, algo de errado aconteceu!")

# COMMAND ----------

# TODO: Fazer essa validacao mais cedo no pipeline
# Ou apagar os 60 produtos no copilot que estao com nome vazio (alias, como eles foram parar la com os validadores?)

produtos_refined = produtos_refined.filter(
    "nome != '' AND ean IS NOT NULL AND ean != ''"
).withColumn("nomes_alternativos", F.expr("filter(nomes_alternativos, x -> x != '')"))

# COMMAND ----------

# Converte strings vazias para null
text_fields_to_null = [
    "contraindicacoes",
    "efeitos_colaterais",
    "interacoes_medicamentosas",
    "instrucoes_de_uso",
    "advertencias_e_precaucoes",
    "condicoes_de_armazenamento",
    "validade_apos_abertura",
    "descricao",
    "indicacao",
    "dosagem",
    "principio_ativo",
    "imagem_url",
]

for field in text_fields_to_null:
    produtos_refined = produtos_refined.withColumn(
        field, F.when(F.trim(F.col(field)) == "", F.lit(None)).otherwise(F.col(field))
    )

# COMMAND ----------

produtos = (
    produtos_refined.withColumn("enriquecido", F.lit(True))
    .withColumn("exclusivo_da_loja", F.lit(None).cast("string"))
    .withColumnRenamed('gerado_em', 'gerado_no_datalake_em')
    .withColumnRenamed('atualizado_em', 'atualizado_no_datalake_em')
    .withColumnRenamed('tipo_receita', 'receita')
)

# COMMAND ----------

# Salva local para evitar reescrita

produtos.write.mode("overwrite").parquet(temp_path)

produtos = env.spark.read.parquet(temp_path)

# COMMAND ----------

# ignoreNullFields=false garante que campos NULL sejam serializados como "campo": null no JSON.
# Sem isso, o Spark omite campos NULL e a API do copilot interpreta a ausência como
# "não alterar", preservando valores inválidos antigos (ex: "N/i") no banco.
produtos_json = produtos.selectExpr(
    "to_json(struct(*), map('ignoreNullFields', 'false')) as json"
)


# COMMAND ----------


def fix_unicode_tags(payload):
    tag_columns = [
        'tags_complementares',
        'tags_substitutos',
        'tags_potencializam_uso',
        'tags_atenuam_efeitos',
        'tags_agregadas',
    ]

    for product in payload['produtos']:
        for column in tag_columns:
            if column in product and isinstance(product[column], list):
                product[column] = [fix_text(tag) for tag in product[column]]

    return payload


# COMMAND ----------


def executar_com_retry(funcao, max_tentativas=5, intervalo_base=2):
    for tentativa in range(1, max_tentativas + 1):
        try:
            return funcao()

        except (OSError, BotoCoreError) as e:
            erro_str = str(e)
            if (
                "AccessDenied" in erro_str
                and "s3" in erro_str
                and tentativa < max_tentativas
            ):
                espera = intervalo_base**tentativa + random.uniform(0, 1)
                print(
                    f"Erro de acesso ao S3 (tentativa {tentativa}/{max_tentativas}). "
                    f"Nova tentativa em {espera:.2f} segundos..."
                )
                time.sleep(espera)
            else:
                raise

    raise RuntimeError(
        f"Erro de acesso ao S3 persistente após {max_tentativas} tentativas."
    )


# COMMAND ----------

headers = {"Authorization": f"Bearer {copilot_token}"}
total_products = len(list(produtos_json.toLocalIterator()))
errors = []
error_codes = {}  # Rastreia códigos de erro HTTP
processed = 0
max_retries_503 = 5
backoff_base = 2


def send_batch_with_retry(batch, max_retries=max_retries_503):
    """Envia batch com retry exponencial para erros 503."""
    payload = {"produtos": [json.loads(p["json"]) for p in batch]}
    fixed_payload = fix_unicode_tags(payload)

    for tentativa in range(max_retries):
        try:
            r = requests.put(
                f"{maggu_copilot_url}/data-import/produtos-import",
                headers=headers,
                json=fixed_payload,
                timeout=30,
            )

            # Se obtivemos resposta 503, fazemos retry
            if r.status_code == 503 and tentativa < max_retries - 1:
                espera = (backoff_base**tentativa) + random.uniform(0, 1)
                tqdm.write(
                    f"⚠️  Erro 503 (tentativa {tentativa + 1}/{max_retries}). "
                    f"Aguardando {espera:.2f}s antes de tentar novamente..."
                )
                time.sleep(espera)
                continue

            # Rastreia código de erro
            if r.status_code not in [200, 204]:
                error_codes[r.status_code] = error_codes.get(r.status_code, 0) + 1

            return r

        except requests.exceptions.RequestException as e:
            if tentativa < max_retries - 1:
                espera = (backoff_base**tentativa) + random.uniform(0, 1)
                tqdm.write(
                    f"⚠️  Erro de conexão (tentativa {tentativa + 1}/{max_retries}): {e}"
                )
                tqdm.write(f"Aguardando {espera:.2f}s antes de tentar novamente...")
                time.sleep(espera)
            else:
                tqdm.write(f"❌ Request error após {max_retries} tentativas: {e}")
                error_codes["connection_error"] = (
                    error_codes.get("connection_error", 0) + 1
                )
                return type("Response", (), {"status_code": 500, "text": str(e)})()

    # Se chegou aqui, todas as tentativas falharam
    return r


def send_batch(batch):
    """Wrapper para manter compatibilidade com find_error."""
    return send_batch_with_retry(batch)


def find_error(batch):
    if len(batch) == 1:
        r = send_batch(batch)
        if r.status_code != 204:
            product = json.loads(batch[0]["json"])
            return f"Erro no EAN {product.get('ean', 'nenhum')} - Detalhe: {r.text}"
        return None

    mid = len(batch) // 2
    left, right = batch[:mid], batch[mid:]

    r_left = send_batch(left)
    if r_left.status_code != 204:
        return find_error(left)

    r_right = send_batch(right)
    if r_right.status_code != 204:
        return find_error(right)

    return None


# COMMAND ----------

iterator = executar_com_retry(lambda: produtos_json.toLocalIterator())

with tqdm(total=total_products, desc="Processando produtos", unit="produto") as pbar:
    for b in batched(iterator, 100):
        try:
            r = send_batch_with_retry(b)
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            error_ean = find_error(b)
            if error_ean:
                errors.append(error_ean)
                tqdm.write(f"🔎 Erro identificado no produto EAN: {error_ean}")
        except requests.exceptions.RequestException as req_err:
            tqdm.write("\nErro de conexão ou requisição!")
            tqdm.write(f"📢 Detalhes: {req_err}\n")
            errors.append(f"Erro de conexão: {req_err}")
        finally:
            processed += len(b)
            pbar.update(len(b))

# COMMAND ----------

print(f"\n✅ Produtos processados: {processed}")
print(f"❌ Total de erros: {len(errors)}")

if error_codes:
    print("\n📈 Códigos de erro HTTP encontrados:")
    for code, count in sorted(error_codes.items(), key=lambda x: x[1], reverse=True):
        print(f"  - Código {code}: {count} ocorrências")

if errors:
    print("\n\n❌ Exportação concluída com erros!")
    print("\n🔴 Lista detalhada de erros (primeiros 20):")
    for err in errors[:20]:
        print(f"  - {err}")
    if len(errors) > 20:
        print(f"\n... e mais {len(errors) - 20} erros.")
    print("\n" + "=" * 80)
    raise RuntimeError(
        f"A exportação falhou com {len(errors)} erro(s). Verifique os detalhes acima."
    )
else:
    print("\n✅ Exportação concluída com sucesso! Todos os produtos foram enviados.")
    print("=" * 80)

# COMMAND ----------

dbutils.fs.rm(temp_path, True)
print(f"🧹 Pasta temporária apagada: {temp_path}")

save_last_updated_at(now)
