from maggulake.tables.base_table import BaseTable


class ConsultaRemediosApresentacoesTable(BaseTable):
    table_name = "raw.consulta_remedios_apresentacoes"
    force_schema = True

    def validate(self, new):
        stats = self.get_stats(new)
        display(stats)

        assert stats.iloc[0].total >= 100000
        assert stats.loc["ean"].completeness == stats.loc["ean"].total
        assert stats.loc["ean"].uniqueness_perc >= 0.95
        assert stats.loc["title"].completeness == stats.loc["title"].total
        assert stats.loc["packageitem"].completeness_perc >= 0.95
        assert stats.loc["stripe"].completeness_perc >= 0.2
        assert stats.loc["control"].completeness_perc >= 0.2
