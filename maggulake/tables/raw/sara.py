from maggulake.schemas.sara import schema
from maggulake.tables.base_table import BaseTable


class SaraTable(BaseTable):
    table_name = "raw.scraper_sara_website"
    force_schema = True
    schema = schema

    def validate(self, new):
        display(self.get_stats(new))
        return new
