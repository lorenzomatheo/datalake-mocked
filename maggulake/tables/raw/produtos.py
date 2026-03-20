import requests

from maggulake.environment.settings import Stage
from maggulake.schemas.produtos_raw import schema
from maggulake.tables.base_table import BaseTable


class ProdutosTable(BaseTable):
    table_name = "raw.produtos_raw"
    schema = schema

    def validate(self, new):
        is_prod = self.stage == Stage.PRODUCTION

        stats = self.get_stats(new)
        print("Estatísticas totais:")
        display(stats)

        total = stats.iloc[0].total
        assert total >= (150000 if is_prod else 1000)
        assert stats.loc["ean"].completeness == stats.loc["ean"].total
        assert stats.loc["ean"].uniqueness == stats.loc["ean"].total
        assert stats.loc["nome"].completeness == stats.loc["nome"].total
        assert stats.loc["fonte"].completeness == stats.loc["fonte"].total
        assert stats.loc["fabricante"].completeness_perc > 0.8
        assert stats.loc["marca"].completeness_perc > 0.9
        assert stats.loc["descricao"].completeness_perc > 0.9
        assert stats.loc["categorias"].completeness_perc > 0.3
        assert stats.loc["eh_medicamento"].completeness_perc > 0.9

        exprs_booleanos = [
            "count_if(eh_medicamento) AS eh_medicamento",
            "count_if(eh_tarjado) AS eh_tarjado",
            "count_if(eh_otc) AS eh_otc",
            "count_if(eh_controlado) AS eh_controlado",
            "count_if(not eh_medicamento) AS nao_eh_medicamento",
            "count_if(not eh_tarjado) AS nao_eh_tarjado",
            "count_if(not eh_otc) AS nao_eh_otc",
            "count_if(not eh_controlado) AS nao_eh_controlado",
        ]

        conta_booleanos = new.selectExpr(*exprs_booleanos).first()

        if is_prod and not self.spark_df.isEmpty():
            anterior = self.spark_df.selectExpr(*exprs_booleanos).first()
            assert conta_booleanos.eh_medicamento >= anterior.eh_medicamento * 0.5
            assert (
                conta_booleanos.nao_eh_medicamento >= anterior.nao_eh_medicamento * 0.5
            )
            assert conta_booleanos.eh_tarjado >= anterior.eh_tarjado * 0.5
            assert conta_booleanos.eh_controlado >= anterior.eh_controlado * 0.5
        else:
            assert conta_booleanos.eh_medicamento >= 1000
            assert conta_booleanos.nao_eh_medicamento >= 1000
            assert conta_booleanos.eh_tarjado >= 1000
            assert conta_booleanos.eh_controlado >= 1000

        stats_meds = self.get_stats(new.filter("eh_medicamento"))
        print("Estatísticas dos medicamentos:")
        display(stats_meds)

        assert stats_meds.loc["tipo_medicamento"].completeness_perc >= 0.8
        assert stats_meds.loc["principio_ativo"].completeness_perc > 0.9
        assert stats_meds.loc["dosagem"].completeness_perc > 0.6
        assert stats_meds.loc["categorias"].completeness_perc > 0.8

        sample = new.filter("imagem_url is not null").sample(5.0 / total).collect()
        imagens = [s.imagem_url for s in sample]

        for imagem in imagens:
            r = requests.get(imagem)

            assert r.status_code == 200
            assert r.headers['Content-Type'].startswith('image/')

        return new.dropna(subset="nome")
