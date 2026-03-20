import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import pytz
from langchain_core.prompt_values import StringPromptValue
from langchain_core.runnables import Runnable
from openai import OpenAI
from openai.types.batch import Batch as OpenAIBatch
from openai.types.file_object import FileObject

SAO_PAULO_TZ = pytz.timezone("America/Sao_Paulo")
SLEEP_SECONDS = 30


@dataclass
class Batch:
    ids: list[int]
    input_file_id: str
    batch_job: OpenAIBatch


@dataclass
class BatchChatRunnable(Runnable):
    openai_client: OpenAI
    gpt_model: str = 'gpt-4o-mini'
    system_message: str = None
    max_tokens: int = 1000
    temperature: float = 0.1

    # TODO: invoke chama batch()[0] sem verificar lista vazia — pode dar IndexError.
    # TODO: adicionar type hints em input, config e retorno.
    def invoke(self, input, config=None, **kwargs):  # pylint: disable=unused-argument
        return self.batch(input)[0]

    # TODO: adicionar type hints em batch, make_batches e batch_requests.
    def batch(self, inputs, config=None, *, return_exceptions=False, **kwargs):  # pylint: disable=unused-argument
        batches = list(self.make_batches(inputs))

        self.wait_for_batches(batches)

        return list(self.get_responses(batches))

    def make_batches(
        self, messages: Iterable[str | StringPromptValue]
    ) -> Iterable[Batch]:
        print("Criando batches:")

        requests = (
            self.make_request(self.message_to_conversation(m)) for m in messages
        )

        i = 1
        for ids, file_bytes in self.batch_requests(requests):
            print(f"Uploading batch {i}")
            input_file = self.upload_batch(file_bytes)
            batch_job = self.create_batch_job(input_file.id)
            yield Batch(ids, input_file.id, batch_job)

            i += 1

    def wait_for_batches(self, batches: list[Batch]):
        for b in batches:
            b.batch_job = self.wait_for_batch_job(b.batch_job.id)

            if b.batch_job.status != 'completed':
                print(b.batch_job)
                raise RuntimeError("Batch terminou com erros")

    def get_responses(self, batches: list[Batch]) -> Iterable[str]:
        for b in batches:
            response = self.parse_batch_job_result(b.batch_job.id)
            yield from [response[i] for i in b.ids]

    def make_request(self, conversation):
        return {
            "custom_id": str(uuid.uuid4()),
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": self.gpt_model,
                "messages": conversation,
                "max_tokens": self.max_tokens,
                "temperature": self.temperature,
            },
        }

    def message_to_conversation(self, message: str | StringPromptValue):
        if not isinstance(message, str):
            message = message.text

        return (
            [{"role": "system", "content": self.system_message}]
            if self.system_message
            else []
        ) + [{"role": "user", "content": message}]

    # Limites: 50k requests ou 100MB
    def batch_requests(self, requests, max_requests=50000, max_size=100000000):
        batch = []
        batch_size = 0
        ids = []

        for r in requests:
            new_batch = batch + [r]
            new_request_size = len(self.to_file_bytes([r]))

            if (
                len(new_batch) >= max_requests
                or batch_size + new_request_size >= max_size
            ):
                yield (ids, self.to_file_bytes(batch))
                batch = [r]
                batch_size = new_request_size
                ids = [r['custom_id']]
                continue

            batch = new_batch
            batch_size += new_request_size
            ids.append(r['custom_id'])

        if len(batch) > 0:
            yield (ids, self.to_file_bytes(batch))

    @staticmethod
    def to_file_bytes(requests):
        return ''.join(json.dumps(r) + '\n' for r in requests).encode()

    # TODO: arquivos em /tmp/ não são apagados em caso de erro. Usar try/finally ou context manager.
    def upload_batch(self, file_bytes) -> FileObject:
        tmp_folder = '/tmp/maggu/batch_gpt/'
        os.makedirs(tmp_folder, exist_ok=True)

        filename = f"{str(uuid.uuid4())}.jsonl"

        with open(os.path.join(tmp_folder, filename), 'wb') as f:
            f.write(file_bytes)

        with open(os.path.join(tmp_folder, filename), 'rb') as f:
            response = self.openai_client.files.create(file=f, purpose="batch")
            print(
                f"Arquivo de Batch foi enviado para a openAI e possui ID: {response.id}"
            )

        os.remove(os.path.join(tmp_folder, filename))

        return response

    def create_batch_job(self, batch_file_id: str) -> OpenAIBatch:
        batch_job = self.openai_client.batches.create(
            input_file_id=batch_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        print(f"Batch job foi criado e possui ID: {batch_job.id}")
        return batch_job

    @staticmethod
    def convert_timestamp_to_datetime(
        timestamp, timezone: str = "America/Sao_Paulo"
    ) -> datetime:
        # o openai retorna um timestamp em segundos, representa tempo desde 1970
        tz = pytz.timezone(timezone)
        dt = datetime.fromtimestamp(timestamp, tz)
        return dt

    def wait_for_batch_job(
        self,
        batch_job_id: str,
        max_time: float = 24 * 60 * 60,
    ) -> OpenAIBatch:
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

            time.sleep(SLEEP_SECONDS)
            attempt += 1

        return batch_obj

    def parse_batch_job_result(self, batch_job_id):
        output_file_id = self.openai_client.batches.retrieve(
            batch_job_id
        ).output_file_id
        raw_content = self.openai_client.files.content(output_file_id)
        content = [json.loads(line) for line in raw_content.text.splitlines()]

        return {
            item["custom_id"]: item["response"]["body"]["choices"][0]["message"][
                "content"
            ]
            for item in content
        }
