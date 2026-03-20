import pandas as pd
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from maggulake.environment.settings import Stage
from maggulake.tables.utils import ValidationError


class BaseTable:
    """
    Conjunto de utilidades para lidar com tabelas no datalake.

    Uso: Crie uma subclasse preenchendo os atributos
    e redefina o método validate

    Para facilitar sair usando, dá pra começar só com o nome da tabela:

    @tabela_facil
    class TabelaTeste(BaseTable):
        table_name = "teste.tabela_teste"
    """

    table_name: str
    schema: str | StructType | None = None
    force_schema: bool = False

    def __init__(self, spark: SparkSession, stage: Stage = Stage.PRODUCTION):
        self.spark = spark
        self.stage = stage

    def validate(self, new: DataFrame) -> DataFrame | None:
        """
        Validação dos novos dados. Sobrescrever.
        Pode retornar um subconjunto dos dados válidos para salvar (nulo são todos).
        Pode lançar um ValidationError pra parar o pipeline.
        Recomendável colocar uns prints ou display para deixar claro os problemas.
        validate_schema abaixo é um bom exemplo.
        Validação de schema já é feita quando há schema definido e imutável.
        Validações não transformam dados.
        """
        raise ValidationError("Tabela sem validação")

    @property
    def delta(self) -> DeltaTable:
        return DeltaTable.forName(self.spark, self.table_name)

    @property
    def spark_df(self) -> DataFrame:
        return self.spark.read.table(self.table_name)

    @property
    def schema_ddl(self) -> StructType:
        return (
            self.schema
            if isinstance(self.schema, StructType)
            else StructType.fromDDL(self.schema)
        )

    def create_if_not_exists(self):
        if not self.schema:
            raise RuntimeError("Tabela sem schema, não posso criar")

        (
            DeltaTable.createIfNotExists(self.spark)
            .tableName(self.table_name)
            .addColumns(self.schema_ddl)
            .execute()
        )

    @staticmethod
    def get_stats(df: DataFrame) -> pd.DataFrame:
        completeness = [
            f"""
                count_if(
                    `{col}` is not null
                    or trim(`{col}`::string) not in ('', '[]', '()', '{{}}')
                ) as `completeness_{col}`
            """
            for col in df.columns
        ]

        uniqueness = [
            f"count(distinct `{col}`) as `uniqueness_{col}`" for col in df.columns
        ]

        stats = df.selectExpr(
            "count(*) as total",
            *completeness,
            *uniqueness,
        ).head()

        return pd.DataFrame(
            [
                {
                    "coluna": col,
                    "total": stats.total,
                    "completeness": stats[f"completeness_{col}"],
                    "completeness_perc": round(
                        stats[f"completeness_{col}"] / stats.total, 4
                    ),
                    "uniqueness": stats[f"uniqueness_{col}"],
                    "uniqueness_perc": round(
                        stats[f"uniqueness_{col}"] / stats.total, 4
                    ),
                }
                for col in df.columns
            ]
        ).set_index("coluna", drop=False)

    def validate_schema(self, new: DataFrame) -> DataFrame:
        all_columns = set(new.columns + self.schema_ddl.names)
        tipos_novos = {c.name: c.dataType.simpleString() for c in new.schema}
        tipos_originais = {c.name: c.dataType.simpleString() for c in self.schema_ddl}

        diferencas = [
            {
                "coluna": col,
                "tipo_original": tipos_originais.get(col),
                "tipo_novo": tipos_novos.get(col),
            }
            for col in all_columns
            if tipos_originais.get(col) != tipos_novos.get(col)
        ]

        if diferencas:
            self.spark.createDataFrame(
                diferencas,
                "coluna string, tipo_original string, tipo_novo string",
            ).display()

        # É feito um to(schema) depois, então só dá pau se as colunas tiverem tipos diferentes
        erros = [
            e
            for e in diferencas
            if e["tipo_novo"] is not None and e["tipo_original"] is not None
        ]

        if not self.force_schema and erros:
            raise ValidationError("Schema diferente do esperado")

    def write(self, new, mode="append") -> None:
        new = new.cache()

        if self.schema:
            self.validate_schema(new)

        to_write = self.validate(new) or new

        if not self.force_schema:
            if not self.spark.catalog.tableExists(self.table_name):
                raise RuntimeError("Tabela precisa ser criada antes de salvar.")

            to_write = to_write.to(self.spark_df.schema)

        writer = to_write.write.mode(mode)

        if self.force_schema:
            if mode == "append":
                writer = writer.option("mergeSchema", "true")
            if mode == "overwrite":
                writer = writer.option("overwriteSchema", "true")

        writer.saveAsTable(self.table_name)

    def merge(
        self,
        new: DataFrame,
        join_columns: str | list[str],
        insert_columns: list[str] = None,
        update_columns: list[str] = None,
    ) -> None:
        new = new.cache()

        if self.schema and not self.force_schema:
            self.validate_schema(new)

        to_write = self.validate(new) or new

        join_columns = (
            join_columns if isinstance(join_columns, list) else [join_columns]
        )
        to_write = to_write.dropDuplicates(join_columns)
        insert_columns = insert_columns or new.columns
        update_columns = update_columns or [
            c for c in new.columns if c not in join_columns
        ]

        join_columns_formatted = " AND ".join(
            [f'target.{c} = source.{c}' for c in join_columns]
        )

        insert_columns_formatted = {c: c for c in insert_columns or new.columns}

        update_columns_formatted = {
            c: f'source.{c}'
            for c in update_columns or new.columns
            if c not in join_columns
        }

        if self.force_schema:
            self.spark.conf.set(
                "spark.databricks.delta.schema.autoMerge.enabled", "true"
            )

        (
            self.delta.alias("target")
            .merge(
                to_write.alias("source"),
                join_columns_formatted,
            )
            .whenMatchedUpdate(set=update_columns_formatted)
            .whenNotMatchedInsert(values=insert_columns_formatted)
            .execute()
        )

        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
