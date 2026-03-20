from textwrap import dedent


def get_prompt_categorias(opcoes_meso_micro: str = "") -> str:
    """
    Retorna o prompt template para categorização de produtos.

    Args:
        opcoes_meso_micro: String formatada com todas as opções de meso->micro
        categorias disponíveis para uma super categoria específica. Usado apenas
        no Nível 2 da categorização.
    """

    base_prompt = dedent("""\
        Você é um especialista em categorização de produtos vendidos em farmácias,
        com profundo conhecimento sobre as características dos produtos farmacêuticos
        e de saúde. Produtos vendidos em farmácias abrangem uma ampla gama de itens,
        incluindo medicamentos, produtos de higiene pessoal, perfumaria, suplementos
        alimentares, entre outros.

        Analise cuidadosamente as informações do produto abaixo e categorize-o conforme as instruções.

        IMPORTANTE: Escolha APENAS categorias que realmente se encaixam no produto.
        Se nenhuma das opções disponíveis for adequada, retorne uma lista vazia ou None.
        NÃO tente forçar o produto em uma categoria que não seja adequada.

        **REGRA FUNDAMENTAL**: Se o produto se encaixa em uma Meso Categoria mas NÃO se encaixa
        em NENHUMA das Micro Categorias disponíveis para aquela Meso, retorne APENAS a "Meso Categoria"
        (sem a micro categoria). É melhor deixar a micro categoria vazia do que forçar uma categoria errada!

        INFORMAÇÕES DO PRODUTO:
        {input}
        """).strip()

    if opcoes_meso_micro:
        base_prompt += f"\n\nOpções disponíveis:\n{opcoes_meso_micro}"

    base_prompt += "\n\nAnalise as informações do produto e retorne a categorização no formato estruturado solicitado."

    return base_prompt
