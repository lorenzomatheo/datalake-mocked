"""
O enum de categorias é uma árvore hierárquica com 3 níveis (atualmente), por isso
não herdamos de uma classe Enum.

Os 3 níveis podem assim ser entendidos:
- Super categoria -> Meso Categoria -> Micro Categoria

Para um determinado produto, define-se sua categoria como a junção dos 3 níveis, assim:
- "Super Categoria > Meso Categoria > Micro Categoria"

Exemplo:
- Rivotril?  "Medicamentos > Sistema Nervoso > Ansiedade"
- Protetor Solar? "Perfumaria e Cuidados > Cuidado com a Pele > Protetores Solares"

Importante: Um produto pode ter mais de uma categoria associada, por exemplo:
- Alcool 70? "Produtos para Casa > Limpeza > Desinfetantes" e "Materiais para Saúde > Primeiros Socorros > Soluções"

Referencia: https://docs.google.com/document/d/1FJbxgowGzBANyKdsMUYhIAh6IrzlfvCDTTbJyj5DBfU/edit?tab=t.0#heading=h.c7ccli2q1lt8
"""


class CategoriasWrapper:
    arvore_categorias = {
        # --- Medicamentos ---------------------------------------------------------
        "Medicamentos": {
            "Sistema Nervoso": [
                "Antidepressivos",
                "Ansiedade",
                "Insônia",
                "Convulsão",
                "Vertigem",
                "Anestésico",
                "Anorexígenos",
                "Antipsicótico",
            ],
            "Dor e Febre": [
                "Analgésicos",
                "Antitérmicos",
                "Anti-inflamatórios",
            ],
            "Sistema Circulatório": [
                "Anti-hipertensivos",
                "Diuréticos",
                "Colesterol",
                "Anticoagulante",
                "Anemia",
                "Arritmia",
            ],
            "Sistema de Sustentação": [
                "Osteoporose",
            ],
            "Digestivo": [
                "Azia",
                "Gastrite",
                "Constipação",
                "Antiparasitário",
            ],
            "Respiratório": [
                "Asma",
                "Rinite",
                "Gripe/Resfriado",
                "Tosse",
                "Dor de Garganta",
            ],
            "Infecções": [
                "Antibióticos",
                "Antifúngicos",
                "Antiviral",
                "Antiparasitários",
            ],
            "Visão": [
                "Colírios",
            ],
            "Tratamento de Câncer": [
                "Antineoplásicos",
                "Antiandrógenos",
            ],
            "Sistema Imune": [
                "Imunossupressor",
            ],
            "Ginecologia": [
                "Anticoncepcionais",
                "Antifúngicos Íntimos",
            ],
            "Dermatologia": [
                "Acne",
                "Anestésicos Tópicos",
                "Renovação Celular",
                "Antifúngicos",
            ],
            "Endócrino": [
                "Diabetes",
                "Tireoide",
                "Controle de Peso",
            ],
            "Antialérgico": [
                "Anti Histamínico",
                "Corticoides",
                "Anti Leucotrienos",
            ],
            "Parar de Fumar": [
                "Reposição com Nicotina",
                "Reposição sem Nicotina",
            ],
            "Alopécia": [
                "Antiandrogênico",
                "Vasodilatador",
            ],
            "Vacinas": [
                "Hepatite A",
                "Hepatite B",
                "Meningite B",
                "Influenza",
                "Dengue",
                "Herpes Zoster",
                "Tríplice Viral",
                "HPV",
                "Varicela",
                "COVID-19",
                "Pentavalente",
                "Tuberculose",
            ],
        },
        # --- Não Medicamentos -----------------------------------------------------
        "Perfumaria e Cuidados": {
            "Cuidado com a Pele": [
                "Hidratantes",
                "Protetores Solares",
                "Limpeza Facial",
                "Acne",
                "Esfoliantes",
            ],
            "Cuidado Capilar": [
                "Shampoos",
                "Condicionadores",
                "Anti-caspa",
                "Anti-queda",
                "Coloração",
                "Máscara",
                "Creme para Pentear/Leave In",
                "Papel para Permanente",
            ],
            "Cuidado Pessoal": [
                "Perfume",
                "Lubrificante",
                "Preservativo",
                "Acessórios Sexuais",
                "Óleos",
                "Repelentes",
                "Talco",
                "Gel/Pomada Massageadora",
                "Sérum",
            ],
            "Higiene Pessoal": [
                "Sabonetes",
                "Desodorantes",
                "Depilação",
                "Fraldas",
                "Absorventes",
                "Antissépticos",
                "Lenços Umedecidos",
                "Lâminas",
                "Hastes Flexíveis",
                "Coletor Menstrual",
            ],
            "Higiene Bucal": [
                "Escovas",
                "Cremes Dentais",
                "Enxaguantes",
                "Fita/Fio Dental",
            ],
            "Maquiagem": [
                "Bases",
                "Batons",
                "Demaquilantes",
                "Lápis",
                "Sombras",
                "Delineador",
                "Máscara para Cílios",
                "Pó",
                "Blush",
                "Iluminador",
                "Apontador",
                "Corretivos",
            ],
            "Oftalmológicos": [
                "Lubrificantes",
                "Limpeza de Lentes",
                "Lenços para Olhos",
            ],
            "Produtos para Unhas": [
                "Esmaltes",
                "Alicates",
                "Removedores",
                "Fortalecedores",
                "Palito",
                "Lixa",
                "Cortador",
                "Espátula",
                "Cabines UV",
                "Amolecedores de Cutículas",
            ],
            "Infantil": [
                "Chupetas",
                "Mamadeiras",
                "Mordedores",
                "Copos",
                "Brinquedos",
                "Prendedor de Chupeta",
                "Porta Leite",
                "Porta Chupeta",
                "Bolsas",
                "Garrafa Térmica",
                "Escova de Mamadeira",
                "Babador",
                "Bombas para Tirar Leite",
            ],
            "Acessórios": [
                "Pincéis",
                "Esponjas",
                "Pinças",
                "Secadores",
                "Chapinhas",
                "Chinelos",
            ],
            "Joias": [
                "Brincos",
                "Correntes",
                "Pulseiras",
                "Piercings",
            ],
            "Cabelo": [
                "Presilhas",
                "Elásticos",
                "Toucas",
                "Escovas",
            ],
            "Vestuário e Acessórios": [
                "Moda Íntima",
                "Vestuário Geral",
                "Calçados Gerais",
                "Diversos",
                "Vestuário Esportivo",
            ],
        },
        "Produtos para Casa": {
            "Limpeza": [
                "Sabão em Pó",
                "Detergentes",
                "Amaciantes",
                "Inseticidas",
                "Água Sanitária",
                "Desinfetantes",
                "Desengordurante",
                "Álcool",
            ],
            "Aromatização": [
                "Difusores",
                "Óleos Essenciais",
                "Bloqueadores de Odor",
                "Velas",
            ],
            "Utensílios": [
                "Esponjas",
                "Pilhas",
                "Baterias",
                "Sacos",
                "Refeição",
                "Saboneteiras",
                "Raquete Elétrica",
                "Super Cola",
                "Recipientes",
                "Bolsa Térmica",
                "Displays e Expositores",
                "Copa e Cozinha",
                "Materiais Gerais",
            ],
            "Papelaria e Organização": [
                "Estojos Escolares",
                "Cadernos",
                "Escrita e Correção",
                "Organização e Acessórios",
                "Livros",
                "Revistas",
            ],
            "Eletrônicos e Tecnologia": [
                "Energia e Cabos",
                "Fones",
                "Caixas de Som",
                "Celulares",
                "Armazenamento e Periféricos",
            ],
        },
        "Materiais para Saúde": {
            "Testes e Aparelhos": [
                "Glicosímetros",
                "Tiras",
                "Teste COVID",
                "Teste de Gravidez",
                "Medidor de Pressão",
                "Oxímetros",
                "Termômetros",
                "Seringas",
                "Acesso Venoso",
                "Tesoura",
                "Lancetas",
            ],
            "Primeiros Socorros": [
                "Curativos",
                "Gazes",
                "Esparadrapos",
                "Soluções",
                "Máscaras",
                "Luvas",
                "Porta Comprimidos",
                "Água Oxigenada",
                "Algodão",
            ],
            "Ortopedia": [
                "Joelheiras",
                "Cintas",
                "Muletas",
                "Meias de Compressão",
                "Tipoia",
                "Munhequeira",
                "Bermuda Térmica",
                "Protetor Palmar",
                "Bola de Massagem",
                "Eletroestimulador",
            ],
            "Inalação": [
                "Nebulizadores",
                "Inaladores",
            ],
            "Esporte e Reabilitação": [
                "Boxe e Muay Thai",
                "Futebol",
                "Ciclismo",
            ],
        },
        "Alimentos e Suplementos": {
            "Fórmulas": [
                "Infantil",
                "Adulto",
            ],
            "Bebidas": [
                "Chás",
                "Águas",
                "Sucos",
                "Energéticos",
                "Refrigerantes",
                "Repositor de Eletrólitos",
            ],
            "Snacks": [
                "Salgadinhos",
                "Biscoitos",
                "Chocolates",
                "Barras de Proteína",
                "Balas",
                "Barra de Cereais",
                "Pirulito",
                "Goma de Mascar",
            ],
            "Suplementos": [
                "Colágeno Hidrolisado",
                "Colágeno não Hidrolisado",
                "Ácido Hialurônico",
                "Proteínas",
                "Ômega",
                "Creatina",
                "Vitaminas",  # Inclui (Vitamina A, Complexo B, Vitamina C, Vitamina D, Vitamina E, Vitamina K)
                "Minerais",  # Inclui (Cálcio, Magnésio, Zinco, Ferro, Selênio, Potássio, Cobre, Manganês, Iodo, Cromo)
                "Hormônios",
                "Probióticos",
                "Prebióticos",
                "Enzimáticos",
            ],
            "Adoçantes": [],
        },
        "Produtos para Animais": {
            "Medicamentos": [
                "Antiparasitário",
            ],
            "Higiene": [
                "Shampoo",
                "Tapete Higiênico",
            ],
        },
    }

    def get_arvore_categorias(self) -> dict[str, dict[str, list[str]]]:
        return self.arvore_categorias

    # --- Metodos para retornar listas de categorias em diferentes níveis ------

    def get_all_super_categorias(
        self,
    ) -> list[str]:
        return list(self.arvore_categorias.keys())

    def get_all_meso_categorias(self, super_categoria) -> list[str]:
        return list(self.arvore_categorias.get(super_categoria, {}).keys())

    def get_all_micro_categorias(self, super_categoria, meso_categoria) -> list[str]:
        return list(
            self.arvore_categorias.get(super_categoria, {}).get(meso_categoria, [])
        )

    def get_all_categorias_flat(self):
        return [
            self.flatten_categoria(macro, cat, micro)
            for macro, categorias in self.arvore_categorias.items()
            for cat, micros in categorias.items()
            for micro in micros or [None]
        ]

    # --- Metodos para retornar o restante da arvore a partir de um nivel ------

    def get_super_and_meso_from_micro(
        self, micro_categoria: str
    ) -> list[tuple[str, str, str]]:
        results = []
        for super_cat, meso_dict in self.arvore_categorias.items():
            for meso_cat, micro_list in meso_dict.items():
                if micro_categoria in micro_list:
                    results.append((super_cat, meso_cat, micro_categoria))
        return results

    def get_super_from_meso(self, meso_categoria: str) -> list[str]:
        results = []
        for super_cat, meso_dict in self.arvore_categorias.items():
            if meso_categoria in meso_dict:
                results.append(super_cat)
        return results

    def get_micros_from_meso(
        self, meso_categoria: str, super_categoria: str | None = None
    ) -> list[str]:
        if super_categoria:
            return self.arvore_categorias.get(super_categoria, {}).get(
                meso_categoria, []
            )

        results = []
        for _, meso_dict in self.arvore_categorias.items():
            if meso_categoria in meso_dict:
                results.extend(meso_dict[meso_categoria])
        return results

    def get_meso_and_micros_from_super(
        self, super_categoria: str
    ) -> dict[str, list[str]]:
        return self.arvore_categorias.get(super_categoria, {})

    # --- Metodos para validacao de categorias ---------------------------------

    def normalizar_categoria(
        self, categoria: str, lista_categorias: list[str]
    ) -> str | None:
        """
        Normaliza uma categoria fazendo match case-insensitive.

        Args:
            categoria: Categoria a normalizar
            lista_categorias: Lista de categorias válidas

        Returns:
            Categoria normalizada ou None se não encontrada
        """
        # Primeiro tenta match exato
        if categoria in lista_categorias:
            return categoria

        # Depois tenta case insensitive
        # NOTE: usa o next que é mais eficiente
        return next(
            (cat for cat in lista_categorias if cat.lower() == categoria.lower()), None
        )

    def validar_e_normalizar_super_categoria(self, super_categoria: str) -> str | None:
        """
        Valida e normaliza uma super categoria.

        Args:
            super_categoria: Nome da super categoria a validar

        Returns:
            Super categoria normalizada ou None se inválida
        """
        lista_super = self.get_all_super_categorias()
        return self.normalizar_categoria(super_categoria, lista_super)

    def validar_e_normalizar_meso_categoria(
        self, meso_categoria: str, super_categoria: str
    ) -> str | None:
        """
        Valida e normaliza uma meso categoria para uma super categoria.

        Args:
            meso_categoria: Nome da meso categoria a validar
            super_categoria: Nome da super categoria (já validada)

        Returns:
            Meso categoria normalizada ou None se inválida
        """
        lista_meso = self.get_all_meso_categorias(super_categoria)
        return self.normalizar_categoria(meso_categoria, lista_meso)

    def validar_e_normalizar_micro_categoria(
        self, micro_categoria: str | None, super_categoria: str, meso_categoria: str
    ) -> str | None:
        """
        Valida e normaliza uma micro categoria para uma combinação super->meso.

        Args:
            micro_categoria: Nome da micro categoria a validar (pode ser None)
            super_categoria: Nome da super categoria (já validada)
            meso_categoria: Nome da meso categoria (já validada)

        Returns:
            Micro categoria normalizada ou None (se inválida ou não fornecida)
        """
        if not micro_categoria:
            return None

        lista_micro = self.get_all_micro_categorias(super_categoria, meso_categoria)
        return self.normalizar_categoria(micro_categoria, lista_micro)

    # --- Métodos auxiliares para formatação e parsing -------------------------

    def get_opcoes_meso_micro_formatadas(self, super_categoria: str) -> str:
        """
        Retorna as opções de meso->micro categorias formatadas para o prompt.

        Args:
            super_categoria: Nome da super categoria

        Returns:
            String formatada com as opções no formato:
            - Meso Categoria -> Micro Categoria
            - Meso Categoria -> Micro Categoria
            ...
        """
        meso_micros = self.get_meso_and_micros_from_super(super_categoria)

        if not meso_micros:
            return "Nenhuma categoria disponível"

        opcoes = []
        for meso, micros in meso_micros.items():
            for micro in micros:
                opcoes.append(f"- {meso} -> {micro}")

        return "\n".join(opcoes)

    @staticmethod
    def parse_meso_micro_string(categoria_string: str) -> tuple[str, str]:
        """
        Parseia uma string "Meso -> Micro" e retorna tupla (meso, micro).

        Args:
            categoria_string: String no formato "Meso -> Micro"

        Returns:
            Tupla (meso_categoria, micro_categoria)

        Raises:
            ValueError: Se o formato estiver incorreto
        """
        if " -> " not in categoria_string:
            raise ValueError(
                f"Formato inválido: {categoria_string}. Esperado 'Meso -> Micro'"
            )

        partes = categoria_string.split(" -> ")
        if len(partes) != 2:
            raise ValueError(
                f"Formato inválido: {categoria_string}. Esperado exatamente uma seta '->'"
            )

        return partes[0].strip(), partes[1].strip()

    @staticmethod
    def parse_meso_micro_lista(
        categorias_meso_micro: list[str],
    ) -> list[tuple[str, str | None]]:
        """
        Parseia uma lista de strings de categorias em diferentes formatos.

        Formatos aceitos:
        - "Meso -> Micro" => retorna (meso, micro)
        - "Meso"          => retorna (meso, None)

        Args:
            categorias_meso_micro: Lista de strings de categorias

        Returns:
            Lista de tuplas (meso_categoria, micro_categoria ou None)
        """
        resultado = []
        for cat_str in categorias_meso_micro:
            cat_str = cat_str.strip()
            if not cat_str:
                continue

            if " -> " in cat_str:
                # Formato completo: "Meso -> Micro"
                try:
                    meso, micro = CategoriasWrapper.parse_meso_micro_string(cat_str)
                    resultado.append((meso, micro))
                except ValueError:
                    # Se parsing falhar, ignora
                    continue
            else:
                # Formato apenas meso: "Meso"
                resultado.append((cat_str, None))

        return resultado

    # --- Métodos que podem ser usados sem instanciar a classe -----------------

    @staticmethod
    def flatten_categoria(macro_categoria, categoria, micro_categoria):
        list_macro_categoria = [macro_categoria] if macro_categoria else []
        list_categoria = [categoria] if categoria else []
        list_micro_categoria = (
            [micro_categoria[0]]
            if isinstance(micro_categoria, tuple)
            else [micro_categoria]
            if micro_categoria
            else []
        )

        return " -> ".join(list_macro_categoria + list_categoria + list_micro_categoria)


arvore_categorias = CategoriasWrapper().get_arvore_categorias()
