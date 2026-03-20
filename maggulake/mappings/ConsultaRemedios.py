import json
from uuid import uuid4

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from maggulake.enums import FaixaEtaria
from maggulake.enums.tipo_medicamento import TipoMedicamento
from maggulake.io.repositorio_s3 import RepositorioS3
from maggulake.utils.objects import remove_empty_elements
from maggulake.utils.strings import string_to_column_name


class CrParser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_raw_dfs(self, stage: str):
        path = self.get_latest(stage)
        rdd = self.spark.sparkContext.wholeTextFiles(path)
        rdd_flat = flatten_rdd(rdd).cache()

        marcas_df = rdd_flat.map(
            lambda marca: {
                string_to_column_name(k): v
                for k, v in marca.items()
                if k != "variacoes"
            }
        ).toDF()

        def flatten_apresentacoes(marca):
            if not marca.get("variacoes"):
                return []

            return [
                {
                    "marca_id": marca["marca_id"],
                    **{string_to_column_name(k): v for k, v in variacao.items()},
                }
                for variacao in marca["variacoes"]
            ]

        apresentacoes_df = (
            rdd_flat.flatMap(flatten_apresentacoes)
            .map(self.parse_apresentacao_raw)
            .map(remove_empty_elements)
            .toDF(sampleRatio=1)
        )

        return marcas_df, apresentacoes_df

    @staticmethod
    def parse_apresentacao_raw(entry):
        if isinstance(entry.get("weightkg"), int):
            entry["weightkg"] = float(entry["weightkg"])
        if isinstance(entry.get("hightcm"), int):
            entry["hightcm"] = float(entry["hightcm"])
        if isinstance(entry.get("widthcm"), int):
            entry["widthcm"] = float(entry["widthcm"])
        if isinstance(entry.get("lengthcm"), int):
            entry["lengthcm"] = float(entry["lengthcm"])
        if entry.get("variationimage") is not None:
            entry["variationimage"] = [i["image"] for i in entry["variationimage"]]
        if entry.get("factoryprice") is not None:
            entry["factoryprice"] = [float(i["price"]) for i in entry["factoryprice"]]
        if entry.get("customermaximumprice") is not None:
            entry["customermaximumprice"] = [
                float(i["price"]) for i in entry["customermaximumprice"]
            ]
        if entry.get("variationattribute") is not None:
            entry["variationattribute"] = [
                {
                    "title": v["attribute"]["title"],
                    "value": v["value"],
                }
                for v in entry["variationattribute"]
            ]

        for k, v in entry["substance"].items():
            entry[f"substance_{k}"] = v

        del entry["substance"]

        safe_columns = [
            "weightkg",
            "hightcm",
            "widthcm",
            "lengthcm",
            "variationimage",
            "factoryprice",
            "customermaximumprice",
            "variationattribute",
            "substance",
            "adultchilduse",
            "marca_id",
            "ean",
            "title",
            "stripe",
            "control",
            "canbesplitted",
            "continuoususe",
            "discontinued",
        ]

        for col in list(entry.keys()):
            if col not in safe_columns:
                entry[col] = json.dumps(entry[col])

        return entry

    def to_produtos_raw(self, marcas_df, apresentacoes_df):
        marcas_df = self.parse_marcas(marcas_df).cache()
        apresentacoes_df = self.parse_apresentacoes(apresentacoes_df).cache()

        return (
            marcas_df.alias("m")
            .join(apresentacoes_df.alias("a"), "marca_id", "left")
            .selectExpr(
                "coalesce(a.ean, m.ean) AS ean",
                "numero_registro",
                """
                    case
                        when nome_complemento is not null
                            then concat(nome, ' ', nome_complemento)
                        else nome
                    end AS nome
                """,
                "nome_complemento",
                "marca",
                "fabricante",
                "principio_ativo",
                "eh_medicamento",
                "coalesce(a.eh_tarjado, m.eh_tarjado) AS eh_tarjado",
                "coalesce(a.eh_otc, m.eh_otc) AS eh_otc",
                "eh_controlado",
                "coalesce(a.pode_partir, m.pode_partir) AS pode_partir",
                "categorias",
                "tipo_medicamento",
                "tipo_de_receita_completo",
                "tarja",
                "classes_terapeuticas",
                "especialidades",
                "doencas_relacionadas",
                "temperatura_armazenamento",
                "via_administracao",
                "dosagem",
                "variantes",
                "uso_continuo",
                "descontinuado",
                "idade_recomendada",
                "descricao",
                "coalesce(a.imagem_url, m.imagem_url) AS imagem_url",
                "bula",
                "arquivo_s3",
                "'ConsultaRemedios' AS fonte",
                "current_timestamp() AS gerado_em",
            )
            .dropna(subset=["ean", "nome"])
            .dropDuplicates(["ean"])
        )

    def parse_marcas(self, marcas_df):
        @F.udf("map<string, string>")
        def especificacoes_to_map(especificacoes):
            return {row["nome"]: row["valor"] for row in especificacoes}

        @F.udf("array<struct<id:string,title:string,content:string>>")
        def bula_to_struct(bula):
            return bula

        return (
            marcas_df.withColumn("bula", bula_to_struct("bula"))
            .withColumn("especificacoes", especificacoes_to_map("especificacoes"))
            .withColumn("ean", F.element_at("especificacoes", "Código de Barras:"))
            .withColumn("marca_especificada", F.element_at("especificacoes", "Marca:"))
            .withColumn("fabricante", F.element_at("especificacoes", "Fabricante:"))
            .withColumn(
                "principio_ativo", F.element_at("especificacoes", "Princípio Ativo:")
            )
            .withColumn(
                "categoria_produto",
                F.element_at("especificacoes", "Categoria do Produto:"),
            )
            .withColumn(
                "categoria_medicamento",
                F.element_at("especificacoes", "Categoria do Medicamento:"),
            )
            .withColumn(
                "tipo_medicamento",
                F.element_at("especificacoes", "Tipo do Medicamento:"),
            )
            .withColumn(
                "tipo_de_receita_completo",
                F.element_at("especificacoes", "Necessita de Receita:"),
            )
            .withColumn(
                "classe_terapeutica",
                F.element_at("especificacoes", "Classe Terapêutica:"),
            )
            .withColumn(
                "especialidades", F.element_at("especificacoes", "Especialidades:")
            )
            .withColumn(
                "doencas_relacionadas",
                F.element_at("especificacoes", "Doenças Relacionadas:"),
            )
            .withColumn(
                "temperatura_armazenamento",
                F.element_at("especificacoes", "Temperatura de Armazenamento:"),
            )
            .withColumn(
                "numero_registro",
                F.element_at("especificacoes", "Registro no Ministério da Saúde:"),
            )
            .withColumn("pode_partir", F.element_at("especificacoes", "Pode partir:"))
            .withColumn(
                "via_administracao", F.element_at("especificacoes", "Modo de Uso:")
            )
            .selectExpr(
                "marca_id",
                "ean",
                "numero_registro",
                "marca AS nome",
                "coalesce(marca_especificada, marca) As marca",
                "fabricante",
                "principio_ativo",
                "categoria_medicamento is not null AS eh_medicamento",
                """
                    tipo_de_receita_completo is not null
                    and tipo_de_receita_completo != 'Isento de Prescrição Médica'
                    AS eh_tarjado
                """,
                """
                    tipo_de_receita_completo is null
                    or tipo_de_receita_completo = 'Isento de Prescrição Médica'
                    AS eh_otc
                """,
                "pode_partir = 'Esta apresentação pode ser partida' AS pode_partir",
                """
                    case
                        when categoria_medicamento is not null
                            then array(categoria_medicamento)
                        when categoria_produto is not null
                            then array(categoria_produto)
                    end AS categorias
                """,
                "tipo_medicamento",
                "tipo_de_receita_completo",
                """
                    case
                        when classe_terapeutica is not null
                            then array(classe_terapeutica)
                    end AS classes_terapeuticas
                """,
                "split(especialidades, ', ') as especialidades",
                "split(doencas_relacionadas, ', ') as doencas_relacionadas",
                "temperatura_armazenamento",
                "via_administracao",
                "descricao",
                "element_at(imagens, 1) AS imagem_url",
                "bula",
                "from_page AS arquivo_s3",
            )
            .replace(
                {
                    "Genérico": TipoMedicamento.GENERICO.value,
                    "Similar": TipoMedicamento.SIMILAR.value,
                    "Similar Intercambiável": TipoMedicamento.SIMILAR_INTERCAMBIAVEL.value,
                    "Referência": TipoMedicamento.REFERENCIA.value,
                    "Novo": TipoMedicamento.REFERENCIA.value,
                    "Biológico": TipoMedicamento.BIOLOGICO.value,
                    "Específico": TipoMedicamento.ESPECIFICO.value,
                    "Fitoterápico": TipoMedicamento.FITOTERAPICO.value,
                    "Radio fármaco": TipoMedicamento.RADIOFARMACO.value,
                    "Outros": None,
                },
                subset=["tipo_medicamento"],
            )
        )

    def parse_apresentacoes(self, apresentacoes_df):
        @F.udf("string")
        def get_dosagem(packageitem):
            packageitem = json.loads(packageitem)
            if not packageitem or not packageitem[0]["packageItemSubstance"]:
                return None

            partes = []
            for p in packageitem[0]["packageItemSubstance"]:
                qty = p.get("quantity")
                unit = p.get("unit") or {}
                unit_name = unit.get("unitName")
                if qty is None or unit_name is None:
                    return None
                partes.append(f"{qty}{unit_name}")
            dosagem = " + ".join(partes)

            return dosagem

        return (
            apresentacoes_df.withColumn("dosagem", get_dosagem("packageitem"))
            .withColumn(
                "idade_recomendada", F.element_at("adultchilduse", "adultChildUseName")
            )
            .replace(
                {
                    "Infantil": FaixaEtaria.CRIANCA.value,
                    "Ambos": FaixaEtaria.TODOS.value,
                    "Adulto": FaixaEtaria.ADULTO.value,
                },
                subset=["idade_recomendada"],
            )
            .selectExpr(
                "marca_id",
                "ean",
                "title AS nome_complemento",
                "dosagem",
                "element_at(variationimage, 1) AS imagem_url",
                "element_at(stripe, 'stripeName') AS tarja",
                "(stripe is not null or element_at(stripe, 'id') != '1') AS eh_tarjado",
                "(stripe is null or element_at(stripe, 'id') = '1') AS eh_otc",
                "element_at(control, 'id') in ('1', '2') as eh_controlado",
                "collect_list(title) over (partition by marca_id) AS variantes",
                "canbesplitted AS pode_partir",
                "continuoususe AS uso_continuo",
                "discontinued AS descontinuado",
                "idade_recomendada",
            )
        )

    @staticmethod
    def get_latest(stage: str):
        latest_folder = RepositorioS3.ultima_pasta(
            '1-raw-layer/maggu/consultaremedios/', stage
        )

        return f"s3://maggu-datalake-{stage}/{latest_folder}produtos/*.jsonl.gz"


def flatten_rdd(rdd):
    return rdd.flatMap(
        lambda f: [
            {
                "arquivo_s3": f[0],
                "marca_id": str(uuid4()),
                **json.loads(p),
            }
            for p in f[1].split('\n')
        ]
    )
