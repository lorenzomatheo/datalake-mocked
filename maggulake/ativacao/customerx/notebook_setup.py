from maggulake.environment import DatabricksEnvironmentBuilder, Environment
from maggulake.integrations.customerx.client import CustomerXClient


def setup_customerx_notebook(
    dbutils, notebook_name: str
) -> tuple[Environment, CustomerXClient, bool]:
    """
    Centralized setup for CustomerX integration notebooks.
    Creates widgets, initializes environment, API client, and Postgres adapter.
    """

    env = DatabricksEnvironmentBuilder.build(
        notebook_name,
        dbutils,
        widgets={
            "cx_environment": ["sandbox", "sandbox", "production"],
            "dry_run": ["true", "true", "false"],
        },
    )

    cx_environment = dbutils.widgets.get("cx_environment")
    dry_run = dbutils.widgets.get("dry_run") == "true"

    api_token_key = (
        "API_TOKEN_SANDBOX" if cx_environment == "sandbox" else "API_TOKEN_PRODUCTION"
    )
    customerx_api_token = dbutils.secrets.get(scope="customerx", key=api_token_key)

    customerx_client = CustomerXClient(
        api_token=customerx_api_token,
        cx_environment=cx_environment,
    )

    __print_settings(env, cx_environment, dry_run)

    return env, customerx_client, dry_run


def __print_settings(env, cx_environment, dry_run):
    print("🔧 Configuração:")
    print(f"   Stage: {env.settings.stage.value}")
    print(f"   Catalog: {env.settings.catalog}")
    print(f"   CX Environment: {cx_environment}")
    print(f"   Dry Run: {dry_run}")
    print()
