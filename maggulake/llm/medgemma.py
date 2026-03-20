import json
import types
from enum import Enum
from textwrap import dedent
from typing import Type, Union, get_args, get_origin

import google.auth
import requests
from google.auth.transport.requests import Request as GoogleRequest
from langsmith import get_current_run_tree, traceable
from pydantic import BaseModel
from pydantic.fields import FieldInfo

MEDGEMMA_PROJECT_ID = "1092371128279"
MEDGEMMA_LOCATION = "europe-west4"
MEDGEMMA_ENDPOINT_ID = "mg-endpoint-5a8ef138-6050-44fd-a86f-df1114f6fc79"
MEDGEMMA_API_ENDPOINT = (
    f"https://{MEDGEMMA_ENDPOINT_ID}.{MEDGEMMA_LOCATION}"
    "-49106214730.prediction.vertexai.goog"
)


def get_medgemma_access_token() -> str:
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    if not credentials.valid:
        credentials.refresh(GoogleRequest())
    return credentials.token


@traceable(run_type="llm", name="MedGemma")
def call_medgemma(messages: list[dict], max_tokens: int = 8192) -> str:
    payload = {
        "instances": [
            {
                "@requestFormat": "chatCompletions",
                "messages": messages,
                "max_tokens": max_tokens,
            }
        ]
    }

    headers = {
        "Authorization": f"Bearer {get_medgemma_access_token()}",
        "Content-Type": "application/json",
    }

    host = MEDGEMMA_API_ENDPOINT.replace("https://", "")
    url = (
        f"https://{host}/v1/projects/{MEDGEMMA_PROJECT_ID}"
        f"/locations/{MEDGEMMA_LOCATION}/endpoints/{MEDGEMMA_ENDPOINT_ID}:predict"
    )

    response = requests.post(url, json=payload, headers=headers, timeout=300)

    if response.status_code != 200:
        raise RuntimeError(f"MedGemma error: {response.status_code} - {response.text}")

    predictions = response.json().get("predictions", [])
    if not predictions:
        return ""

    prediction = predictions[0] if isinstance(predictions, list) else predictions

    usage = prediction.get("usage", {})
    if usage:
        run = get_current_run_tree()
        if run is not None:
            run.metadata.update(
                {
                    "usage_metadata": {
                        "input_tokens": usage.get("prompt_tokens", 0),
                        "output_tokens": usage.get("completion_tokens", 0),
                        "total_tokens": usage.get("total_tokens", 0),
                    }
                }
            )
            run.patch()

    if "choices" in prediction and prediction["choices"]:
        return prediction["choices"][0]["message"]["content"]
    return prediction.get("content", str(prediction))


def get_enum_values(annotation) -> list[str] | None:
    origin = get_origin(annotation)
    if origin is Union:
        for arg in get_args(annotation):
            if isinstance(arg, type) and issubclass(arg, Enum):
                return [e.value for e in arg]
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return [e.value for e in annotation]
    return None


def get_schema_description(output_schema: Union[dict, Type[BaseModel]]) -> str:
    if not (isinstance(output_schema, type) and issubclass(output_schema, BaseModel)):
        return str(output_schema)

    lines = []
    for name, field_info in output_schema.model_fields.items():
        field_info: FieldInfo
        annotation = output_schema.__annotations__.get(name)

        type_str = "string"
        if annotation:
            origin = get_origin(annotation)
            if origin is Union:
                args = [a for a in get_args(annotation) if a is not types.NoneType]
                type_str = str(args[0].__name__) if args else "string"
            elif hasattr(annotation, "__name__"):
                type_str = annotation.__name__

        line = f'\n## Campo: "{name}" (tipo: {type_str})'

        enum_values = get_enum_values(annotation) if annotation else None
        if enum_values:
            line += f"\nValores permitidos: {enum_values}"

        if field_info.description:
            line += f"\n{field_info.description.strip()}"

        lines.append(line)

    return "\n".join(lines)


def build_medgemma_prompt(
    prompt_template: str,
    output_schema: Union[dict, Type[BaseModel]],
    message: Union[str, dict],
) -> str:
    json_instruction = dedent(f"""
        ---
        INSTRUÇÕES DE RESPOSTA:

        Você DEVE responder APENAS com um JSON válido.

        {get_schema_description(output_schema)}

        ---
        FORMATO DA RESPOSTA: JSON puro, sem explicações, sem markdown, sem texto adicional.
        """)

    if isinstance(message, dict):
        prompt = prompt_template
        for key, value in message.items():
            prompt = prompt.replace("{" + key + "}", str(value))
        return prompt + json_instruction
    return prompt_template.replace("{input}", str(message)) + json_instruction


def parse_medgemma_response(
    response: str, output_schema: Union[dict, Type[BaseModel]]
) -> dict:
    response = response.strip()
    if response.startswith("```json"):
        response = response[7:]
    elif response.startswith("```"):
        response = response[3:]
    if response.endswith("```"):
        response = response[:-3]
    response = response.strip()

    try:
        return json.loads(response)
    except json.JSONDecodeError:
        start, end = response.find("{"), response.rfind("}") + 1
        if start != -1 and end > start:
            try:
                return json.loads(response[start:end])
            except json.JSONDecodeError:
                pass

        if isinstance(output_schema, type) and issubclass(output_schema, BaseModel):
            schema = output_schema.model_json_schema()
            return {f: None for f in schema.get("properties", {}).keys()}
        return {}
