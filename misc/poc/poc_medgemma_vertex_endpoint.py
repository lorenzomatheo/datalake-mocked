"""
Simple wrapper to call the MedGemma Vertex AI endpoint.
No agent dependencies - just direct HTTP calls.
"""

import json
import os
from typing import Any, Dict, List

import google.auth
import requests
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2 import service_account

# Configuration
PROJECT_ID = "1092371128279"
LOCATION = "europe-west4"
ENDPOINT_ID = "mg-endpoint-5a8ef138-6050-44fd-a86f-df1114f6fc79"
API_ENDPOINT = f"https://{ENDPOINT_ID}.{LOCATION}-49106214730.prediction.vertexai.goog"


class VertexAIError(Exception):
    """Custom exception for Vertex AI endpoint errors."""


def setup_credentials_from_databricks(dbutils):
    """
    Set up credentials from Databricks secrets.

    Args:
        dbutils: Databricks utils object
    """
    credentials_dict = json.loads(
        dbutils.secrets.get(scope="databricks", key="GOOGLE_VERTEX_CREDENTIALS")
    )

    credentials_path = "/dbfs/tmp/gcp_credentials.json"
    with open(credentials_path, "w") as f:
        json.dump(credentials_dict, f)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    print(f"Credentials set up at: {credentials_path}")


def get_access_token() -> str:
    """
    Get Google Cloud access token for authentication using default credentials.
    Uses GOOGLE_APPLICATION_CREDENTIALS environment variable.

    Returns:
        Access token string
    """
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    if not credentials.valid:
        credentials.refresh(GoogleRequest())
    return credentials.token


def get_access_token_from_dict(credentials_dict: Dict[str, Any]) -> str:
    """
    Get Google Cloud access token directly from service account credentials dictionary.
    This method doesn't require writing to a file or setting environment variables.

    Args:
        credentials_dict: Service account credentials as a dictionary
                         (the parsed JSON from your service account key file)

    Returns:
        Access token string

    Example:
        >>> creds = json.loads(dbutils.secrets.get(scope="databricks", key="GOOGLE_VERTEX_CREDENTIALS"))
        >>> token = get_access_token_from_dict(creds)
    """
    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    if not credentials.valid:
        credentials.refresh(GoogleRequest())
    return credentials.token


def call_medgemma_endpoint(
    messages: List[Dict[str, str]],
    max_tokens: int = 200,
    project_id: str = PROJECT_ID,
    location: str = LOCATION,
    endpoint_id: str = ENDPOINT_ID,
    api_endpoint: str = API_ENDPOINT,
) -> str:
    """
    Call the MedGemma Vertex AI endpoint.

    Args:
        messages: List of message dicts with 'role' and 'content' keys
                  Role should be 'user' or 'model' (not 'assistant')
        max_tokens: Maximum tokens in response
        project_id: GCP project ID
        location: GCP region
        endpoint_id: Vertex AI endpoint ID
        api_endpoint: Full API endpoint URL

    Returns:
        Response content as string

    Example:
        >>> messages = [{"role": "user", "content": "Qual a posologia do Doril?"}]
        >>> response = call_medgemma_endpoint(messages)
        >>> print(response)
    """
    payload = {
        "instances": [
            {
                "@requestFormat": "chatCompletions",
                "messages": messages,
                "max_tokens": max_tokens,
            }
        ]
    }

    # Get authentication token
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Build the URL
    host = api_endpoint.replace("https://", "").replace("http://", "")
    url = f"https://{host}/v1/projects/{project_id}/locations/{location}/endpoints/{endpoint_id}:predict"

    # Make the request
    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        raise VertexAIError(
            f"Error calling Vertex AI: {response.status_code} - {response.text}"
        )

    # Parse the response
    result = response.json()
    predictions = result.get("predictions")

    if isinstance(predictions, list) and len(predictions) > 0:
        prediction = predictions[0]
    elif isinstance(predictions, dict):
        prediction = predictions
    else:
        return ""

    # Extract content from prediction
    if "choices" in prediction and len(prediction["choices"]) > 0:
        return prediction["choices"][0]["message"]["content"]
    if "content" in prediction:
        return prediction["content"]

    return str(prediction)


def ask_medgemma(question: str, max_tokens: int = 200) -> str:
    """
    Simple helper to ask a single question to MedGemma.

    Args:
        question: The question to ask
        max_tokens: Maximum tokens in response

    Returns:
        Response content as string

    Example:
        >>> response = ask_medgemma("Qual a posologia do Doril?")
        >>> print(response)
    """
    messages = [{"role": "user", "content": question}]
    return call_medgemma_endpoint(messages, max_tokens=max_tokens)


# Example usage
if __name__ == "__main__":
    # For local testing, make sure GOOGLE_APPLICATION_CREDENTIALS is set

    # Simple question
    question = "Qual a posologia do Doril?"
    response = ask_medgemma(question)
    print("Question:", question)
    print("Response:", response)
    print()

    # Multi-turn conversation
    conversation = [
        {"role": "user", "content": "O que é paracetamol?"},
        {
            "role": "model",
            "content": "Paracetamol é um medicamento analgésico e antipirético...",
        },
        {"role": "user", "content": "Qual a dose máxima diária?"},
    ]
    response = call_medgemma_endpoint(conversation)
    print("Multi-turn response:", response)
