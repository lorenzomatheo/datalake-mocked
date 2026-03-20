from maggulake.utils.html import clean_html

# TODO: Marcos disse: Inicialmente eu peguei a bula com o formato html pq eu
# achei que algumas marcações poderiam ser úteis. Mas vendo hoje acho que todo
# lugar que usa ela limpa o html antes de usar. Podemos ver de salvar a bula
# como texto já na raw.


def get_text_from_bula(rows):
    if rows is None:
        return ""

    output_string = ""

    for row in rows:
        row_dict = row.asDict()
        for value in row_dict.values():
            if isinstance(value, str):
                value = clean_html(value)
            output_string += f"{value} "

    return output_string.strip()
