from maggulake.tables.base_table import BaseTable


class RdTable(BaseTable):
    table_name = "raw.rd"
    force_schema = True

    def validate(self, new):
        stats = self.get_stats(new)
        display(stats)

        assert stats.iloc[0].total >= 150000
        assert stats.loc["ean"].completeness == stats.loc["ean"].total
        assert stats.loc["ean"].uniqueness_perc >= 0.6
        assert stats.loc["name"].completeness == stats.loc["name"].total
        assert stats.loc["principioativonovo"].completeness > 0
        assert stats.loc["description"].completeness_perc > 0.8
        assert stats.loc["fabricante"].completeness > 0
        assert stats.loc["marca"].completeness > 0
        assert stats.loc["dosagem"].completeness > 0
        assert stats.loc["codtarja"].completeness_perc > 0.9
