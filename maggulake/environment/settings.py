from enum import Enum
from typing import Self

from pydantic import BaseModel

from .configs import configs


class Stage(Enum):
    PRODUCTION = "production"
    STAGING = "staging"


class Settings(BaseModel):
    stage: Stage
    name: str
    name_short: str
    catalog: str
    bucket: str
    postgres_host: str
    postgres_replica_host: str | None
    postgres_port: int = 5432
    postgres_database: str
    postgres_user: str
    postgres_password: str
    openai_organization: str
    openai_api_key: str
    milvus_uri: str
    milvus_token: str
    data_api_url: str
    data_api_key: str
    gemini_api_key: str
    bigdatacorp_address: str
    bigdatacorp_token: str
    bluesoft_address: str
    bluesoft_token: str
    portal_obm_address: str
    portal_obm_token: str
    postgres_schema: str
    databricks_views_schema: str

    @classmethod
    def create(cls, stage: Stage, **kwargs) -> Self:
        return cls(stage=stage, **configs[stage.value], **kwargs)
