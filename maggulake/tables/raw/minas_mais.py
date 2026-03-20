from maggulake.tables.base_table import BaseTable


class MinasMaisTable(BaseTable):
    table_name = "raw.minas_mais"
    force_schema = True

    def validate(self, new):
        stats = self.get_stats(new)
        display(stats)
