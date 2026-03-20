from enum import Enum
from textwrap import dedent
from typing import Literal, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame

from maggulake.utils.account_filters import EXCLUDED_ACCOUNT_SQL_ARRAY
from maggulake.utils.iters import batched

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 1000)

config_by_stage = {
    "dev": {
        "username": "app_staging",
        "password": "**",
        "database": "copilot_staging",
    },
    "prod": {
        "username": "datalake",
        "password": "**",
        "database": "maggu_production",
    },
}


class PostgresHost(Enum):
    STAGING = "copilot-staging.cbgww662dsmx.us-east-1.rds.amazonaws.com"
    PRODUCTION = "maggu-poc.cbgww662dsmx.us-east-1.rds.amazonaws.com"
    READ_REPLICA = "maggu-readreplica.cbgww662dsmx.us-east-1.rds.amazonaws.com"


class PostgresAdapter:
    def __init__(
        self,
        stage: Literal["prod", "dev"],
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        utilizar_read_replica: bool = False,
        database: Optional[str] = None,
        port: Optional[int] = None,
    ):
        self.STAGE = stage
        self.DATABASE_HOST = (
            host
            if host is not None
            else PostgresHost.STAGING.value
            if self.STAGE == "dev"
            else PostgresHost.PRODUCTION.value
            if not utilizar_read_replica
            else PostgresHost.READ_REPLICA.value
        )
        self.DATABASE_PORT = port or "5432"
        self.DATABASE_NAME = database or config_by_stage[self.STAGE]["database"]
        self.DATABASE_URL = f"jdbc:postgresql://{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"

        self.USER = (
            user if user is not None else config_by_stage[self.STAGE]["username"]
        )
        self.PASSWORD = (
            password
            if password is not None
            else config_by_stage[self.STAGE]["password"]
        )
        self.DRIVER = "org.postgresql.Driver"

    # --------------------------------------------------------------------------
    # Leitura e conexão (métodos core)
    # --------------------------------------------------------------------------

    def get_connection(self):
        try:
            conn = psycopg2.connect(
                database=self.DATABASE_NAME,
                user=self.USER,
                host=self.DATABASE_HOST,
                password=self.PASSWORD,
                port=self.DATABASE_PORT,
                options="-c default_transaction_read_only=off",
            )
        except psycopg2.DatabaseError as error:
            print(f"Error: {error}")

        return conn

    def read_table(self, spark, table: str) -> DataFrame:
        return (
            spark.read.format("jdbc")
            .option("driver", self.DRIVER)
            .option("url", self.DATABASE_URL)
            .option("dbtable", table)
            .option("user", self.USER)
            .option("password", self.PASSWORD)
            .load()
        )

    def read_query(self, spark, query: str) -> DataFrame:
        return self.read_table(spark, f"({query}) AS subquery")

    def read_table_pandas(self, table: str) -> pd.DataFrame:
        return self.execute_query(f"SELECT * FROM {table}")

    def list_tables(self) -> list[str]:
        conn = psycopg2.connect(
            database=self.DATABASE_NAME,
            user=self.USER,
            host=self.DATABASE_HOST,
            password=self.PASSWORD,
            port=self.DATABASE_PORT,
        )

        # Criar um cursor
        cursor = conn.cursor()

        # Consulta para listar todas as tabelas existentes
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """

        # Executar a consulta
        cursor.execute(query)

        # Buscar os resultados e imprimir no console
        tables = cursor.fetchall()
        tables.sort()

        # converter para list[str]
        tables = [table[0] for table in tables]

        # Encerra a conexão
        cursor.close()
        conn.close()

        return tables

    # --------------------------------------------------------------------------
    # Execução de queries (retorna pandas)
    # --------------------------------------------------------------------------

    def execute_query(self, query, connection=None) -> pd.DataFrame:
        conn = None
        connection_created = False
        try:
            if connection is None:
                conn = psycopg2.connect(
                    database=self.DATABASE_NAME,
                    user=self.USER,
                    host=self.DATABASE_HOST,
                    password=self.PASSWORD,
                    port=self.DATABASE_PORT,
                    options="-c default_transaction_read_only=off",
                )
                connection_created = True
            else:
                conn = connection

            cursor = conn.cursor()
            cursor.execute(query)

            if query.strip().lower().startswith(("select", "--select")):
                data = cursor.fetchall()
                if data:
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(data, columns=columns)
                    df.columns = self._rename_duplicates(list(df.columns))
                else:
                    df = pd.DataFrame(columns=["status", "message"])
                    df.loc[0] = ["success", "Query did not return any results"]
            else:
                df = pd.DataFrame(columns=["status", "message"])
                df.loc[0] = ["success", "Query executed successfully"]

            conn.commit()

        except psycopg2.DatabaseError as error:
            if conn is not None:
                conn.rollback()
            print(f"Error: {error}")
            df = pd.DataFrame(columns=["status", "message"])
            df.loc[0] = ["error", str(error)]

        finally:
            if connection_created and conn is not None:
                conn.close()

        return df

    def execute_query_with_params(
        self, query: str, params=None, connection=None
    ) -> pd.DataFrame:
        conn = None
        connection_created = False
        try:
            if connection is None:
                conn = psycopg2.connect(
                    database=self.DATABASE_NAME,
                    user=self.USER,
                    host=self.DATABASE_HOST,
                    password=self.PASSWORD,
                    port=self.DATABASE_PORT,
                    options="-c default_transaction_read_only=off",
                )
                connection_created = True
            else:
                conn = connection

            cursor = conn.cursor()
            cursor.execute(query, params)

            if query.strip().lower().startswith(("select", "--select")):
                data = cursor.fetchall()
                if data:
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(data, columns=columns)
                    df.columns = self._rename_duplicates(list(df.columns))
                else:
                    df = pd.DataFrame(columns=["status", "message"])
                    df.loc[0] = ["success", "Query did not return any results"]
            else:
                df = pd.DataFrame(columns=["status", "message"])
                df.loc[0] = ["success", "Query executed successfully"]

            conn.commit()

        except psycopg2.DatabaseError as error:
            if conn is not None:
                conn.rollback()
            print(f"Error: {error}")
            df = pd.DataFrame(columns=["status", "message"])
            df.loc[0] = ["error", str(error)]

        finally:
            if connection_created and conn is not None:
                conn.close()

        return df

    # --------------------------------------------------------------------------
    # Escrita em tabelas (insert / upsert)
    # --------------------------------------------------------------------------

    def insert_into_table(
        self, df: DataFrame, table: str, mode='append', batch_size=100
    ) -> None:
        print(f"Inserindo na tabela {table}...")

        def to_input(rows, cols):
            return [tuple(row[i] for i in cols) for row in rows]

        with psycopg2.connect(
            database=self.DATABASE_NAME,
            user=self.USER,
            host=self.DATABASE_HOST,
            password=self.PASSWORD,
            port=self.DATABASE_PORT,
        ) as conn:
            cursor = conn.cursor()

            if mode == "overwrite":
                print(f"Apagando os registros da tabela {table}.")
                self.execute_query(f"DELETE FROM {table}")

            columns = df.columns

            for i, batch in enumerate(batched(df.toLocalIterator(), batch_size)):
                print(f"Fazendo batch {i + 1} de {len(batch)} registros.")

                execute_values(
                    cursor,
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s",
                    to_input(batch, columns),
                )

        print("Sucesso!")

    def upsert_into_table(
        self,
        df: DataFrame,
        table: str,
        conflict_columns: list[str],
        update_columns: list[str] = None,
        batch_size: int = 1000,
    ) -> None:
        columns = df.columns

        def to_input(rows, cols):
            return [tuple(row[i] for i in cols) for row in rows]

        conflict_clause = ", ".join(conflict_columns)

        if update_columns is None:
            excluded_columns = set(conflict_columns) | {"id", "criado_em", "created_at"}
            update_columns = [col for col in columns if col not in excluded_columns]

        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])

        upsert_query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES %s
            ON CONFLICT ({conflict_clause}) DO UPDATE SET
                {update_clause}
        """

        with psycopg2.connect(
            database=self.DATABASE_NAME,
            user=self.USER,
            host=self.DATABASE_HOST,
            password=self.PASSWORD,
            port=self.DATABASE_PORT,
        ) as conn:
            cursor = conn.cursor()

            for i, batch in enumerate(batched(df.toLocalIterator(), batch_size)):
                print(f"Fazendo batch {i + 1} de {len(batch)} registros.")

                execute_values(
                    cursor,
                    upsert_query,
                    to_input(batch, columns),
                )

            conn.commit()

        print("Upsert concluído com sucesso!")

    # --------------------------------------------------------------------------
    # Queries de negócio — Campanhas
    # --------------------------------------------------------------------------

    @staticmethod
    def _where_campanha(
        tipo: Literal['INDUSTRIA', 'VAREJO'] | None,
        status: Literal['ATIVA', 'INATIVA', 'PAUSADA'] | None,
    ) -> str:
        clauses = [
            f"c.status = '{status}'" if status else None,
            "c.inicio_em <= now()",
            "c.fim_em >= now()",
            f"c.tipo_campanha = '{tipo}'" if tipo else None,
        ]
        return " AND ".join(c for c in clauses if c)

    def get_produtos_em_campanha(
        self,
        spark,
        tipo: Literal['INDUSTRIA', 'VAREJO'] | None = "INDUSTRIA",
        status: Literal['ATIVA', 'INATIVA', 'PAUSADA'] | None = "ATIVA",
    ) -> DataFrame:
        """Retorna produtos em campanhas ativas como Spark DataFrame.

        Colunas: ean, nome, conta_id, conta, loja_id, loja.
        """
        where = self._where_campanha(tipo, status)
        query = f"""
        WITH campanhas_por_loja AS (
            (SELECT campanha_id, loja_id
             FROM public.campanhas_campanha_lojas_especificas)
            UNION ALL
            (SELECT ccl.campanha_id, cl.id AS loja_id
             FROM public.campanhas_campanha_todas_as_lojas_das_contas ccl
             INNER JOIN contas_loja cl ON ccl.conta_id = cl.conta_id)
        )
        SELECT DISTINCT
            p.ean,
            p.nome,
            ct.id AS conta_id,
            ct.databricks_tenant_name AS conta,
            l.id AS loja_id,
            l.codigo_loja AS loja
        FROM produtos_produtov2 p
        INNER JOIN campanhas_campanhaproduto cp
            ON p.id = cp.produto_id AND cp.removido_em IS NULL
        INNER JOIN campanhas_campanha c
            ON c.id = cp.campanha_id
        LEFT JOIN campanhas_por_loja cl ON c.id = cl.campanha_id
        LEFT JOIN contas_loja l ON l.id = cl.loja_id
        LEFT JOIN contas_conta ct ON ct.id = l.conta_id
        WHERE {where}
        """
        return self.read_query(spark, query)

    def get_eans_em_campanha(
        self,
        spark,
        tipo: Literal['INDUSTRIA', 'VAREJO'] | None = "INDUSTRIA",
        status: Literal['ATIVA', 'INATIVA', 'PAUSADA'] | None = "ATIVA",
    ) -> list[str]:
        """Retorna lista de EANs distintos de produtos em campanhas ativas."""
        where = self._where_campanha(tipo, status)
        query = f"""
        SELECT DISTINCT p.ean
        FROM produtos_produtov2 p
        INNER JOIN campanhas_campanhaproduto cp
            ON p.id = cp.produto_id AND cp.removido_em IS NULL
        INNER JOIN campanhas_campanha c
            ON c.id = cp.campanha_id
        WHERE {where}
        """
        return [row.ean for row in self.read_query(spark, query).collect()]

    # --------------------------------------------------------------------------
    # Queries de negócio — Contas e Lojas
    # --------------------------------------------------------------------------

    def get_contas(self, spark) -> DataFrame:
        """
        Retorna contas disponíveis para sincronização, excluindo contas fake.
        """
        query = dedent(f"""
        SELECT
            id,
            name,
            databricks_tenant_name,
            status,
            created_at,
            updated_at
        FROM contas_conta
        WHERE
            NOT LOWER(name) LIKE ANY(ARRAY[{EXCLUDED_ACCOUNT_SQL_ARRAY}])
        ORDER BY created_at
        """)

        return self.read_query(spark, query)

    def get_lojas(
        self,
        spark,
        ativo: Optional[bool] = True,
        extra_fields: Optional[list[str]] = None,
    ) -> DataFrame:
        """
        Retorna lojas disponíveis para sincronização, excluindo:
        - Lojas de teste/ERPs (toolspharma, automatiza, hos, asasys, etc.)
        - Lojas e contas obsoletas (nome iniciado com "obsoleto")

        Args:
            spark: SparkSession para executar a query
            ativo: Filtro de status da loja.
                   - True (padrão): apenas lojas ativas
                   - False: apenas lojas inativas
                   - None: todas as lojas, independente do status
            extra_fields: Lista de campos adicionais para incluir na query.
                         Por padrão, retorna apenas id, conta_id, name.
        """
        base_fields = ["l.id", "l.conta_id", "l.name", "c.name as conta_name"]

        if extra_fields:
            # Adicionar prefixo "l." apenas para nomes simples de coluna.
            # Expressões (funções, aliases, colunas qualificadas) passam sem alteração.
            def _prefix(f: str) -> str:
                token = f.split()[0]  # ex: "coalesce(...)" ou "cnpj" ou "l.cidade"
                is_simple_name = token.isidentifier()
                return f"l.{f}" if is_simple_name else f

            formatted_extra = [_prefix(f) for f in extra_fields]
            all_fields = base_fields + formatted_extra
        else:
            all_fields = base_fields

        fields_str = ",\n    ".join(all_fields)

        if ativo is None:
            ativo_clause = ""
        else:
            ativo_value = "true" if ativo else "false"
            ativo_clause = f"AND l.ativo = {ativo_value}"

        query = dedent(f"""
        SELECT
            {fields_str}
        FROM contas_loja l
        JOIN contas_conta c ON l.conta_id = c.id
        WHERE
            1 = 1
            {ativo_clause}
            AND NOT LOWER(l.name) LIKE ANY(ARRAY[{EXCLUDED_ACCOUNT_SQL_ARRAY}])
            AND NOT LOWER(c.name) LIKE ANY(ARRAY[{EXCLUDED_ACCOUNT_SQL_ARRAY}])
        ORDER BY l.created_at
        """)

        return self.read_query(spark, query)

    def get_donos(self, spark) -> DataFrame:
        """
        Retorna donos de contas (tabela contas_donoconta).

        Returns:
            DataFrame com todos os campos da tabela contas_donoconta
        """
        query = dedent("""
        SELECT *
        FROM contas_donoconta
        ORDER BY updated_at
        """)

        return self.read_query(spark, query)

    def get_cnpj_list_by_user_id(self):
        """
        Retorna uma tabela contendo user_id, quais CNPJs de lojas o usuário
        está associado e a quantidade de lojas.
        """

        return self.execute_query(
            dedent("""
            --select
            WITH lojas_filtradas AS (
                SELECT
                    id AS loja_id,
                    cnpj
                FROM
                    contas_loja
                WHERE
                    cnpj IS NOT NULL
                        AND
                    ativo IS TRUE
            ),
            lojas_usuarios AS (
                SELECT
                    lu.user_id, lf.cnpj
                FROM
                    contas_loja_users lu
                        FULL JOIN
                    lojas_filtradas lf ON lu.loja_id = lf.loja_id
                WHERE
                    lf.cnpj IS NOT NULL
            ),
            lojas_corrigidas AS (
                SELECT
                    user_id,
                    TRIM(LEADING '0' FROM REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g')) AS cnpj
                FROM
                    lojas_usuarios
                GROUP BY
                    user_id,
                    TRIM(LEADING '0' FROM REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'))
            )
            SELECT user_id,
                STRING_AGG(cnpj, ', ') AS cnpjs,
                COUNT(cnpj) AS n_lojas
            FROM
                lojas_corrigidas
            GROUP BY
                user_id;
            """)
        )

    # --------------------------------------------------------------------------
    # Queries de negócio — Atendentes e Gerentes
    # --------------------------------------------------------------------------

    def get_gerentes_lojas(self, spark) -> DataFrame:
        """
        Retorna tabela com dados de gerentes.
        Seleciona um gerente por loja (o mais recentemente cadastrado).

        Estratégia de fallback:
        1. Primeiro busca atendentes com cargo 'gerente' (atendentes_atendenteloja).
        2. Para lojas sem gerente na tabela de atendentes, usa a tabela
           contas_gerenteloja como fallback.
        """
        query = dedent("""
        WITH gerentes_atendentes AS (
            SELECT DISTINCT ON (al.loja_id)
                al.loja_id AS loja_id,
                ai.nome AS nome_gerente,
                ai.telefone AS telefone_gerente,
                ai.email AS email_gerente,
                aa.cadastrado_em AS data_cadastro_gerente
            FROM
                atendentes_atendenteloja al
            LEFT JOIN
                atendentes_atendente a            ON a.id = al.atendente_id
            LEFT JOIN
                atendentes_ativacao aa            ON a.id = aa.atendente_id
            LEFT JOIN
                atendentes_informacoespessoais ai ON a.id = ai.atendente_id
            LEFT JOIN
                contas_loja cl                    ON cl.id = al.loja_id
            WHERE
                a.is_active = true
                    AND
                aa.status LIKE 'cadastrado'
                    AND
                lower(al.cargo) LIKE 'gerente'
            ORDER BY al.loja_id, aa.cadastrado_em DESC NULLS LAST
        ),
        gerentes_fallback AS (
            SELECT DISTINCT ON (gl.loja_id)
                gl.loja_id,
                gl.nome AS nome_gerente,
                gl.telefone AS telefone_gerente,
                NULL AS email_gerente,
                gl.updated_at AS data_cadastro_gerente
            FROM
                contas_gerenteloja gl
            INNER JOIN
                contas_loja cl ON gl.loja_id = cl.id
            WHERE
                gl.loja_id NOT IN (
                    SELECT loja_id FROM gerentes_atendentes
                )
            ORDER BY gl.loja_id, gl.updated_at DESC NULLS LAST
        ),
        all_gerentes AS (
            SELECT * FROM gerentes_atendentes
            UNION ALL
            SELECT * FROM gerentes_fallback
        )
        SELECT *
        FROM all_gerentes
        WHERE
            nome_gerente IS NOT NULL
                AND
            data_cadastro_gerente IS NOT NULL
        """)

        return self.read_query(spark, query)

    def get_atendentes(self) -> pd.DataFrame:
        return self.execute_query(
            dedent("""
            SELECT
                a.id as atendente_id,
                a.is_active,
                a.conta_id,
                a.username,
                aa.status as status_ativacao,
                aa.cadastrado_em,
                aa.deve_receber_comunicacoes_no_whatsapp,
                ai.nome,
                ai.telefone,
                ai.email,
                ai.cpf
            FROM
                atendentes_atendente a
            LEFT JOIN
                atendentes_ativacao aa
                    ON
                a.id = aa.atendente_id
            LEFT JOIN
                atendentes_informacoespessoais ai
                    ON
                a.id = ai.atendente_id
            """)
        )

    def get_atendentes_com_lojas(
        self,
        apenas_ativos: bool = True,
        apenas_com_vendas_ultimos_30_dias: bool = False,
    ) -> pd.DataFrame:
        """Retorna atendentes com dados de loja e rede"""

        conditions: list[str] = []

        if apenas_ativos:
            conditions.append("a.is_active = true")

        if apenas_com_vendas_ultimos_30_dias:
            conditions.append(
                "a.ultima_venda_realizada_em >= CURRENT_DATE - INTERVAL '30 days'"
            )

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return self.execute_query(
            dedent(f"""
            SELECT
                -- Dados da loja
                cl.id AS id_loja,
                cl.name AS nome_loja,
                cl.codigo_de_seis_digitos AS codigo_de_seis_digitos,
                cl.status AS status_loja,
                cl.cnpj AS cnpj_loja,
                cl.erp,

                -- Dados da rede
                cc.name AS nome_rede,

                -- Dados do atendente
                al.codigo_externo AS codigo_externo_atendente,
                a.id AS id_atendente,
                a.username,
                aa.status AS status_ativacao,
                aa.cadastrado_em,
                aa.deve_receber_comunicacoes_no_whatsapp,
                ai.nome AS nome_atendente,
                ai.telefone AS telefone_atendente,
                a.ultima_venda_realizada_em
            FROM
                atendentes_atendenteloja al
            LEFT JOIN contas_loja cl ON al.loja_id = cl.id
            LEFT JOIN contas_conta cc ON cc.id = cl.conta_id
            LEFT JOIN atendentes_atendente a ON al.atendente_id = a.id
            LEFT JOIN atendentes_ativacao aa ON a.id = aa.atendente_id
            LEFT JOIN atendentes_informacoespessoais ai ON a.id = ai.atendente_id
            {where}
            ORDER BY aa.cadastrado_em
            """)
        )

    # --------------------------------------------------------------------------
    # Helpers internos
    # --------------------------------------------------------------------------

    @staticmethod
    def _rename_duplicates(old_columns):
        seen = {}
        for idx, column in enumerate(old_columns):
            if column in seen:
                seen[column] += 1
                old_columns[idx] = f"{column}_{seen[column]}"
            else:
                seen[column] = 0
        return old_columns
