# TODO: arquivo marcado como deprecated mas ainda tem 250+ linhas ativas. Remover ou documentar uso atual.
# Deprecated. Usar o batch_gpt_runnable
# construído com a versão mais recente do openai==1.35.7
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Tuple

import pyspark.sql.types as T
import pytz
from openai import OpenAI
from openai.types.batch import Batch
from openai.types.file_object import FileObject
from pyspark.sql.session import SparkSession

SAO_PAULO_TZ = pytz.timezone("America/Sao_Paulo")


@dataclass
class BatchGpt:
    spark: SparkSession
    openai_client: OpenAI
    catalog: str = 'staging'
    prefix: str = ''
    gpt_model: str = 'gpt-4o-mini'
    max_tokens: int = 1000
    temperature: float = 0.1

    def get_all_results(self, messages: Iterable[str]):
        print("GPT responde batch")
        requests = (
            self.make_request(self.message_to_conversation(m)) for m in messages
        )

        i = 1
        for b in self.make_batches(requests):
            print(f"Fazendo batch {i}")
            yield from self.get_batch_results(b)
            i += 1

    def get_batch_results(self, batch):
        ids = [r['custom_id'] for r in batch]
        print("Fazendo upload do batch")
        batch_input_file = self.upload_batch(batch)
        print("batch_job")
        batch_job = self.create_batch_job(batch_input_file.id)
        self.save_batch_job(batch_job, batch_input_file)

        returned_batch, status = self.check_batch_status(batch_job.id)

        if status not in ["completed"]:
            raise RuntimeError(
                "Batch não foi concluído com sucesso. Verifique os logs."
            )

        results = self.parse_batch_job_result(returned_batch.id)

        return [results[i] for i in ids]

    def make_request(self, conversation):
        return {
            "custom_id": self.prefix + str(uuid.uuid4()),
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": self.gpt_model,
                "messages": conversation,
                "max_tokens": self.max_tokens,
                "temperature": self.temperature,
            },
        }

    @staticmethod
    def message_to_conversation(message):
        return [
            {
                "role": "system",
                "content": "Seu nome é Maggu (feminino) e você é uma médica especialista em assuntos relacionados a farmácias e medicamentos.",
            },
            {"role": "user", "content": message},
        ]

    # Limites: 50k requests ou 100MB
    def make_batches(self, requests, max_requests=50000, max_size=100000000):
        batch = []
        batch_size = 0

        for r in requests:
            new_batch = batch + [r]
            new_request_size = len(self.to_file_bytes([r]))

            if (
                len(new_batch) >= max_requests
                or batch_size + new_request_size >= max_size
            ):
                yield batch
                batch = [r]
                batch_size = new_request_size
                continue

            batch = new_batch
            batch_size += new_request_size

        if len(batch) > 0:
            yield batch

    @staticmethod
    def to_file_bytes(requests):
        return ''.join(json.dumps(r) + '\n' for r in requests).encode()

    def upload_batch(self, batch) -> FileObject:
        tmp_folder = '/dbfs/tmp/maggu/batch_gpt'
        os.makedirs(tmp_folder, exist_ok=True)

        filename = f"{self.prefix}_{str(uuid.uuid4())}.jsonl"
        file_bytes = self.to_file_bytes(batch)

        with open(os.path.join(tmp_folder, filename), 'wb') as f:
            f.write(file_bytes)

        with open(os.path.join(tmp_folder, filename), 'rb') as f:
            response = self.openai_client.files.create(file=f, purpose="batch")
            print(
                f"Arquivo de Batch foi enviado para a openAI e possui ID: {response.id}"
            )
            return response

    def create_batch_job(self, batch_file_id: str) -> Batch:
        batch_job = self.openai_client.batches.create(
            input_file_id=batch_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        print(f"Batch job foi criado e possui ID: {batch_job.id}")
        return batch_job

    def save_batch_job(self, batch_job, batch_input_file):
        batch_job_df = self.spark.createDataFrame(
            [
                {
                    "id": batch_job.id,
                    "object": batch_job.object,
                    "bytes": batch_input_file.bytes,
                    "endpoint": batch_job.endpoint,
                    "filename": batch_input_file.filename,
                    "input_file_id": batch_input_file.id,
                    "completion_window": batch_job.completion_window,
                    "status": batch_job.status,
                    "output_file_id": batch_job.output_file_id,
                    "error_file_id": batch_job.error_file_id,
                    "created_at": self.convert_timestamp_to_datetime(
                        batch_job.created_at
                    ),
                    "in_progress_at": batch_job.in_progress_at,
                    "expires_at": self.convert_timestamp_to_datetime(
                        batch_job.expires_at
                    ),
                    "completed_at": batch_job.completed_at,
                    "failed_at": batch_job.failed_at,
                    "expired_at": batch_job.expired_at,
                    "metadata": (
                        json.dumps(batch_job.metadata) if batch_job.metadata else None
                    ),
                    "updated_at": datetime.now(SAO_PAULO_TZ),
                }
            ],
            schema=BATCH_FILE_SCHEMA,
        )

        batch_job_df.write.mode("append").saveAsTable(
            f"{self.catalog}.utils.openai_batch_jobs"
        )

    @staticmethod
    def convert_timestamp_to_datetime(
        timestamp, timezone: str = "America/Sao_Paulo"
    ) -> datetime:
        # o openai retorna um timestamp em segundos, representa tempo desde 1970
        tz = pytz.timezone(timezone)
        dt = datetime.fromtimestamp(timestamp, tz)
        return dt

    def check_batch_status(
        self,
        batch_job_id: str,
        max_time: float = 24 * 60 * 60,
    ) -> Tuple[Batch, str]:
        # https://platform.openai.com/docs/guides/batch/4-checking-the-status-of-a-batch
        attempt = 0
        start_time = time.time()
        print(f"Começando a checar o status do batch {batch_job_id}")
        while True:
            batch_obj = self.openai_client.batches.retrieve(batch_job_id)
            status = batch_obj.status
            current_time = datetime.now(SAO_PAULO_TZ)

            print(f"Current batch status: {status} ({current_time})")
            if status in ["completed"]:
                print("Status is now 'completed'")
                break
            if status in ["expired", "failed"]:
                print(
                    "There's a problem with this batch. Please check it. Status: ",
                    status,
                )
                break
            if time.time() - start_time > max_time:
                print(f"Timeout reached. Max time allowed was {max_time} seconds")
                break

            # TODO: tempo minimo pode ser menor em batches menores
            sleep_seconds = 120 if attempt == 0 else min(30, 2**attempt)
            time.sleep(sleep_seconds)
            attempt += 1

        return batch_obj, status

    def parse_batch_job_result(self, batch_id):
        output_file_id = self.openai_client.batches.retrieve(batch_id).output_file_id
        raw_content = self.openai_client.files.content(output_file_id)
        content = [json.loads(line) for line in raw_content.text.splitlines()]

        return {
            item["custom_id"]: item["response"]["body"]["choices"][0]["message"][
                "content"
            ]
            for item in content
        }


def lista_todos_os_batches_abertos_no_openai(client: OpenAI) -> list:
    batch_list = client.batches.list()

    # Mantém uma lista com status que mostrem que o batch ainda está vivo
    open_batches = []
    for batch in batch_list.data:
        batch_id = batch.id
        batch_status = batch.status
        if batch_status in ["in_progress", "finalizing", "validating"]:
            open_batches.append(batch_id)

    print(f"Foram encontrados {len(open_batches)} batches abertos no OpenAI.")

    return open_batches


BATCH_FILE_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), True),
        T.StructField("object", T.StringType(), True),
        T.StructField("bytes", T.IntegerType(), True),
        T.StructField("endpoint", T.StringType(), True),
        T.StructField("filename", T.StringType(), True),
        # T.StructField('errors', T.StringType(), True),
        T.StructField("input_file_id", T.StringType(), True),
        T.StructField("completion_window", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("output_file_id", T.StringType(), True),
        T.StructField("error_file_id", T.StringType(), True),
        T.StructField("created_at", T.DateType(), True),
        T.StructField("in_progress_at", T.DateType(), True),
        T.StructField("expires_at", T.DateType(), True),
        T.StructField("completed_at", T.DateType(), True),
        T.StructField("failed_at", T.DateType(), True),
        T.StructField("expired_at", T.DateType(), True),
        # T.StructField('request_counts', T.MapType(T.StringType(), T.IntegerType()), True),
        T.StructField("metadata", T.StringType(), True),
        T.StructField(
            "updated_at", T.DateType(), True
        ),  # TODO: verificar se to salvando data mesmo
    ]
)
