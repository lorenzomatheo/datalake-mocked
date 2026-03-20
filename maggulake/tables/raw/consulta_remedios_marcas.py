from maggulake.tables.base_table import BaseTable


class ConsultaRemediosMarcasTable(BaseTable):
    table_name = "raw.consulta_remedios_marcas"
    force_schema = True

    def validate(self, new):
        stats = self.get_stats(new)
        display(stats)

        assert stats.iloc[0].total >= 80000
        assert stats.loc["marca"].completeness == stats.loc["marca"].total
        assert stats.loc["descricao"].completeness == stats.loc["descricao"].total
