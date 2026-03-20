import pyspark.sql.functions as F

from maggulake.schemas import schema_ids_produtos


class ProdutosRepository:
    def __init__(self, spark, stage='dev', catalog='staging'):
        self.spark = spark
        self.stage = stage
        self.tabela_ids = f'{catalog}.utils.ids_produtos'

    def com_ids(self, produtos):
        produtos = produtos.dropna(subset='ean').dropDuplicates(['ean']).cache()

        ids_produtos = self.get_ids_produtos()

        produtos_com_id = produtos.filter(F.col('id').isNotNull())

        ids_produtos = self.update_ids_produtos(ids_produtos, produtos_com_id).cache()

        produtos_sem_id = produtos.filter(F.col('id').isNull())

        condicao = F.col("p.ean") == F.col("ip.ean")

        com_id = (
            produtos_sem_id.drop('id', 'gerado_em')
            .alias('p')
            .join(ids_produtos.alias('ip'), condicao)
            .selectExpr('p.*', 'ip.id', 'ip.gerado_em')
        )

        sem_id = (
            produtos_sem_id.drop('id', 'gerado_em')
            .alias('p')
            .join(produtos_sem_id.alias('ip'), condicao, 'leftanti')
            .withColumn('id', F.expr('uuid()'))
            .withColumn('gerado_em', F.current_timestamp())
        )

        novos_ids_produtos = sem_id.select('id', 'ean', 'gerado_em')
        ids_produtos.union(novos_ids_produtos).write.mode('overwrite').saveAsTable(
            self.tabela_ids
        )

        return produtos_com_id.unionByName(
            com_id.unionByName(sem_id, allowMissingColumns=True),
            allowMissingColumns=True,
        )

    def get_ids_produtos(self):
        if self.spark.catalog.tableExists(self.tabela_ids):
            return self.spark.read.table(self.tabela_ids).cache()
        else:
            return self.spark.createDataFrame([], schema=schema_ids_produtos)

    def update_ids_produtos(self, ids_produtos, produtos_com_id):
        novos_ids_produtos = produtos_com_id.select("id", "ean").withColumn(
            'gerado_em', F.current_timestamp()
        )

        ids_sem_antigos = ids_produtos.join(
            novos_ids_produtos, ['id'], 'leftanti'
        ).join(novos_ids_produtos, ['ean'], 'leftanti')

        return ids_sem_antigos.union(novos_ids_produtos)
