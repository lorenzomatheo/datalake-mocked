from maggulake.tables.base_table import BaseTable


class IqviaTable(BaseTable):
    table_name = "raw.iqvia"
    force_schema = True

    def validate(self, new):
        stats = self.get_stats(new)
        display(stats)
