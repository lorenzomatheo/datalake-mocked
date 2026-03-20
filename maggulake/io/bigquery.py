"""
BigQuery Adapter para centralizar queries analíticas.

Este adapter abstrai o acesso ao BigQuery via Spark SQL, centralizando queries
comuns de vendas, produtos e lojas para evitar duplicação de código e reduzir
carga no PostgreSQL.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from pyspark.sql import DataFrame, SparkSession

from maggulake.environment.tables import CopilotTable
from maggulake.utils.account_filters import EXCLUDED_ACCOUNT_REGEXP


@dataclass
class BigQueryAdapter:
    """
    Adapter para queries BigQuery via Spark SQL.
    Isso alivia o PostgreSQL de produção para queries pesadas de leitura.
    """

    spark: SparkSession
    schema: str

    def table(self, table: CopilotTable) -> str:
        """Retorna o nome completo da tabela com schema."""
        return f"{self.schema}.{table.value}"

    def sql(self, query: str) -> DataFrame:
        """Executa uma query SQL no BigQuery via Spark."""
        return self.spark.sql(query)

    def get_vendas_por_periodo(
        self,
        data_inicio: datetime,
        loja_ids: Optional[list[int | str]] = None,
        eans: Optional[list[str]] = None,
        status: str = "venda-concluida",
        incluir_tenant: bool = True,
    ) -> DataFrame:
        """
        Busca vendas em um período específico.

        Args:
            data_inicio: Data de início como datetime
            loja_ids: Lista de IDs de lojas para filtrar (opcional)
            eans: Lista de EANs para filtrar (opcional)
            status: Status da venda (default: 'venda-concluida')
            incluir_tenant: Se True, inclui tenant e codigo_loja no resultado

        Returns:
            DataFrame com vendas filtradas
        """
        data_inicio_str = data_inicio.strftime("%Y-%m-%d")

        vendas_table = self.table(CopilotTable.vendas)
        vendas_item_table = self.table(CopilotTable.vendas_item)
        contas_loja_table = self.table(CopilotTable.contas_loja)
        contas_conta_table = self.table(CopilotTable.contas_conta)

        # Colunas extras de tenant se solicitado
        tenant_cols = ""
        tenant_join = ""
        if incluir_tenant:
            tenant_cols = """
                c.databricks_tenant_name as tenant,
                cl.codigo_loja,
            """
            tenant_join = f"""
                JOIN {contas_loja_table} cl ON v.loja_id = cl.id
                JOIN {contas_conta_table} c ON cl.conta_id = c.id
            """

        # Filtros opcionais
        loja_filter = ""
        if loja_ids:
            # Adicionar aspas em cada ID para evitar problemas com UUIDs
            loja_ids_str = "','".join(str(lid) for lid in loja_ids)
            loja_filter = f"AND v.loja_id IN ('{loja_ids_str}')"

        # NOTE: Essa estrategia de listar EANS e fazer um ISIN nao eh muito sustentavel
        # se a lista tiver muitos valores. Nesses casos poderia ser melhor fazer um JOIN ou usar broadcast
        ean_filter = ""
        if eans:
            eans_str = "','".join(eans)
            ean_filter = f"AND vi.ean IN ('{eans_str}')"

        query = f"""
            SELECT
                v.id AS venda_id,
                v.loja_id,
                v.realizada_em,
                v.status,
                vi.ean,
                vi.quantidade,
                vi.valor_final,
                vi.preco_venda_desconto,
                vi.custo_compra,
                {tenant_cols}
                vi.produto_id
            FROM {vendas_table} v
            JOIN {vendas_item_table} vi ON v.id = vi.venda_id
            {tenant_join}
            WHERE DATE(v.realizada_em) >= '{data_inicio_str}'
              AND v.status = '{status}'
              {loja_filter}
              {ean_filter}
        """
        return self.sql(query)

    def get_vendedores_com_vendas(
        self,
        data_inicio: datetime,
        status_venda: str = "venda-concluida",
        status_item: str = "vendido",
    ) -> DataFrame:
        """
        Retorna vendedores que tiveram vendas concluídas no período.

        Args:
            data_inicio: Data de início como datetime
            status_venda: Status da venda
            status_item: Status do item de venda

        Returns:
            DataFrame com coluna vendedor_id
        """
        data_inicio_str = data_inicio.strftime("%Y-%m-%d")

        vendas_table = self.table(CopilotTable.vendas)
        vendas_item_table = self.table(CopilotTable.vendas_item)

        query = f"""
            SELECT DISTINCT v.vendedor_id
            FROM {vendas_table} v
            INNER JOIN {vendas_item_table} vi ON v.id = vi.venda_id
            WHERE v.vendedor_id IS NOT NULL
              AND v.status = '{status_venda}'
              AND vi.status = '{status_item}'
              AND DATE(v.venda_concluida_em) >= '{data_inicio_str}'
        """
        return self.sql(query)

    def get_produtos_mais_vendidos(
        self,
        dias: int = 7,
        classificacao_abc: Literal["A", "B", "C"] = "A",
        apenas_lojas_ativas: bool = True,
    ) -> DataFrame:
        """
        Retorna produtos com classificação ABC baseada em vendas.

        Args:
            dias: Número de dias para análise
            classificacao_abc: Classificação desejada ('A', 'B', 'C')
            apenas_lojas_ativas: Se True, filtra apenas lojas com status ativo

        Returns:
            DataFrame com id, ean do produto e métricas de venda
        """
        vendas_table = self.table(CopilotTable.vendas)
        vendas_item_table = self.table(CopilotTable.vendas_item)
        contas_loja_table = self.table(CopilotTable.contas_loja)
        produtos_table = self.table(CopilotTable.produtos)

        loja_filter = ""
        if apenas_lojas_ativas:
            loja_filter = "AND cl.status IN ('ATIVA', 'EM_ATIVACAO')"

        query = f"""
            WITH base_vendas AS (
              SELECT
                p.id,
                p.ean,
                SUM(vi.quantidade) AS quantidade_vendas,
                SUM(vi.valor_final) AS valor_vendas
              FROM {vendas_item_table} AS vi
              JOIN {vendas_table} AS v ON vi.venda_id = v.id
              INNER JOIN {contas_loja_table} AS cl ON v.loja_id = cl.id
              INNER JOIN {produtos_table} AS p ON vi.produto_id = p.id
              WHERE v.venda_concluida_em >= DATE_SUB(CURRENT_DATE(), {dias})
                {loja_filter}
              GROUP BY p.id, p.ean
            ),
            ordenado AS (
              SELECT
                *,
                RANK() OVER (ORDER BY valor_vendas DESC) AS rank_vendas
              FROM base_vendas
            ),
            com_classificacao AS (
              SELECT
                *,
                SUM(valor_vendas) OVER () AS total_geral,
                SUM(valor_vendas) OVER (
                  ORDER BY valor_vendas DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS acumulado,
                ROUND(acumulado / total_geral, 4) AS perc_acumulado,
                CASE
                  WHEN acumulado / total_geral <= 0.80 THEN 'A'
                  WHEN acumulado / total_geral <= 0.95 THEN 'B'
                  ELSE 'C'
                END AS classificacao_abc
              FROM ordenado
            )
            SELECT
              id,
              ean,
              quantidade_vendas,
              valor_vendas,
              rank_vendas,
              classificacao_abc
            FROM com_classificacao
            WHERE classificacao_abc = '{classificacao_abc}'
            ORDER BY quantidade_vendas DESC
        """
        return self.sql(query)

    def get_produtos_por_eans(
        self,
        eans: list[str],
        colunas: Optional[list[str]] = None,
    ) -> DataFrame:
        """
        Busca produtos por lista de EANs.

        Args:
            eans: Lista de EANs para buscar
            colunas: Lista de colunas a retornar (default: id, ean, nome, marca)

        Returns:
            DataFrame com produtos encontrados
        """
        produtos_table = self.table(CopilotTable.produtos)

        if colunas is None:
            colunas = ["id as produto_id", "ean", "nome", "marca"]

        colunas_str = ", ".join(colunas)
        eans_str = "','".join(eans)

        query = f"""
            SELECT {colunas_str}
            FROM {produtos_table}
            WHERE ean IN ('{eans_str}')
        """
        return self.sql(query)

    def get_estoque_por_eans(
        self,
        eans: list[str],
    ) -> DataFrame:
        """
        Busca dados de estoque para uma lista de EANs.

        Args:
            eans: Lista de EANs para buscar

        Returns:
            DataFrame com dados de estoque
        """
        produtos_loja_table = self.table(CopilotTable.produtos_loja)

        eans_str = "','".join(eans)
        query = f"""
            SELECT
                id,
                ean,
                tenant,
                codigo_loja,
                loja_id,
                estoque_unid AS estoque_atual,
                produto_id,
                atualizado_em
            FROM {produtos_loja_table}
            WHERE estoque_unid IS NOT NULL
              AND ean IN ('{eans_str}')
        """
        return self.sql(query)

    # TODO: unificar get_produtos_campanha_ativa e get_produtos_em_campanha, pois são bem similares
    def get_produtos_campanha_ativa(self) -> DataFrame:
        """
        Retorna EANs de produtos em campanhas ativas.

        Returns:
            DataFrame com coluna ean dos produtos em campanhas ativas
        """
        campanhas_produto_table = self.table(CopilotTable.campanhas_produto)
        campanhas_table = self.table(CopilotTable.campanhas)
        produtos_table = self.table(CopilotTable.produtos)

        query = f"""
            SELECT DISTINCT p.ean
            FROM {campanhas_produto_table} cp
            INNER JOIN {campanhas_table} c
                ON cp.campanha_id = c.id
            INNER JOIN {produtos_table} p
                ON cp.produto_id = p.id
            WHERE c.status = 'ATIVA'
              AND cp.removido_em IS NULL
              AND p.ean IS NOT NULL
        """
        return self.sql(query)

    def get_produtos_com_vendas_recentes(self, horizonte_meses: int = 3) -> DataFrame:
        """
        Retorna EANs de produtos vendidos nos últimos N meses.

        Args:
            horizonte_meses: Número de meses para considerar no histórico

        Returns:
            DataFrame com coluna ean dos produtos vendidos
        """
        vendas_item_table = self.table(CopilotTable.vendas_item)
        vendas_table = self.table(CopilotTable.vendas)

        query = f"""
            SELECT DISTINCT vi.ean
            FROM {vendas_item_table} vi
            INNER JOIN {vendas_table} v
                ON vi.venda_id = v.id
            WHERE v.status = 'venda-concluida'
              AND v.realizada_em >= ADD_MONTHS(CURRENT_DATE(), -{horizonte_meses})
              AND vi.status = 'vendido'
              AND vi.criado_em >= ADD_MONTHS(CURRENT_DATE(), -{horizonte_meses})
              AND vi.ean IS NOT NULL
        """
        return self.sql(query)

    def get_concorrentes(self) -> DataFrame:
        """
        Retorna produtos concorrentes cadastrados em campanhas.

        Returns:
            DataFrame com campanha_id, campanhaproduto_id, produto_ean,
            produto_nome, power_phrase, id_concorrente, ean_concorrente,
            nome_concorrente
        """
        campanhas_produto_table = self.table(CopilotTable.campanhas_produto)
        campanhas_produto_concorrentes_table = self.table(
            CopilotTable.campanhas_produto_concorrentes
        )
        produtos_table = self.table(CopilotTable.produtos)

        query = f"""
            SELECT
                ccp.campanha_id         AS campanha_id,
                ccp.id                  AS campanhaproduto_id,
                ppv21.ean               AS produto_ean,
                ppv21.nome              AS produto_nome,
                ccp.power_phrase        AS power_phrase,
                ccpc.produtov2_id       AS id_concorrente,
                ppv22.ean               AS ean_concorrente,
                ppv22.nome              AS nome_concorrente
            FROM {campanhas_produto_table} ccp
            LEFT JOIN {produtos_table} ppv21
                ON ppv21.id = ccp.produto_id
            LEFT JOIN {campanhas_produto_concorrentes_table} ccpc
                ON ccp.id = ccpc.campanhaproduto_id
            LEFT JOIN {produtos_table} ppv22
                ON ccpc.produtov2_id = ppv22.id
            WHERE ccpc.produtov2_id IS NOT NULL
        """
        return self.sql(query)

    def get_produtos_em_campanha(
        self,
        tipo: Optional[Literal["INDUSTRIA", "VAREJO"]] = "INDUSTRIA",
        status: Literal["ATIVA", "INATIVA", "PAUSADA"] = "ATIVA",
    ) -> DataFrame:
        """
        Retorna produtos em campanhas filtrados por tipo e status.

        Args:
            tipo: Tipo da campanha ('INDUSTRIA', 'VAREJO') ou None para todos
            status: Status da campanha (default: 'ATIVA')

        Returns:
            DataFrame com ean, nome, conta_id, conta, loja_id, loja
        """
        campanhas_table = self.table(CopilotTable.campanhas)
        campanhas_produto_table = self.table(CopilotTable.campanhas_produto)
        campanhas_lojas_especificas_table = self.table(
            CopilotTable.campanhas_lojas_especificas
        )
        campanhas_todas_lojas_table = self.table(
            CopilotTable.campanhas_todas_as_lojas_das_contas
        )
        produtos_table = self.table(CopilotTable.produtos)
        contas_loja_table = self.table(CopilotTable.contas_loja)
        contas_conta_table = self.table(CopilotTable.contas_conta)

        tipo_filter = f"AND c.tipo_campanha = '{tipo}'" if tipo else ""

        query = f"""
            WITH campanhas_por_loja AS (
                SELECT campanha_id, loja_id
                FROM {campanhas_lojas_especificas_table}
                UNION ALL
                SELECT ccl.campanha_id, cl.id AS loja_id
                FROM {campanhas_todas_lojas_table} ccl
                INNER JOIN {contas_loja_table} cl ON ccl.conta_id = cl.conta_id
            )
            SELECT DISTINCT
                p.ean,
                p.nome,
                ct.id       AS conta_id,
                ct.databricks_tenant_name AS conta,
                l.id        AS loja_id,
                l.codigo_loja AS loja
            FROM {produtos_table} p
            INNER JOIN {campanhas_produto_table} cp
                ON p.id = cp.produto_id AND cp.removido_em IS NULL
            INNER JOIN {campanhas_table} c
                ON c.id = cp.campanha_id
            LEFT JOIN campanhas_por_loja cl ON c.id = cl.campanha_id
            LEFT JOIN {contas_loja_table} l ON l.id = cl.loja_id
            LEFT JOIN {contas_conta_table} ct ON ct.id = l.conta_id
            WHERE c.status = '{status}'
              AND c.inicio_em <= current_timestamp()
              AND c.fim_em >= current_timestamp()
              {tipo_filter}
        """
        return self.sql(query)

    def get_produtos_novos_sem_vendas(self, janela_meses: int = 1) -> DataFrame:
        """
        Retorna EANs de produtos criados recentemente que não tiveram vendas.

        Produtos são considerados novos se foram criados nos últimos N meses
        e não possuem nenhuma venda associada.

        Args:
            janela_meses: Número de meses para considerar produto como novo

        Returns:
            DataFrame com coluna ean dos produtos novos sem vendas
        """
        produtos_table = self.table(CopilotTable.produtos)
        vendas_item_table = self.table(CopilotTable.vendas_item)
        vendas_table = self.table(CopilotTable.vendas)

        query = f"""
            SELECT DISTINCT p.ean
            FROM {produtos_table} p
            WHERE p.criado_em >= ADD_MONTHS(CURRENT_DATE(), -{janela_meses})
              AND p.ean IS NOT NULL
              AND NOT EXISTS (
                SELECT 1
                FROM {vendas_item_table} vi
                INNER JOIN {vendas_table} v
                    ON vi.venda_id = v.id
                WHERE vi.ean = p.ean
                  AND vi.quantidade > 0
              )
        """
        return self.sql(query)

    def get_lojas_mais_vendas(
        self,
        data_inicio: datetime,
        apenas_lojas_ativas: bool = True,
    ) -> DataFrame:
        """
        Retorna as lojas com maior faturamento no período estipulado.
        Args:
            dias: Número de dias para análise retrospectiva.
            apenas_lojas_ativas: Se True, filtra apenas lojas com status ativo em contas_loja.
        Returns:
            DataFrame com colunas: id, name, tenant, quantidade_vendas, valor_vendas, rank_vendas.
        """

        contas_loja_table = self.table(CopilotTable.contas_loja)
        vendas_item_table = self.table(CopilotTable.vendas_item)
        vendas_table = self.table(CopilotTable.vendas)

        loja_filter = ""
        if apenas_lojas_ativas:
            loja_filter = "AND cl.status IN ('ATIVA', 'EM_ATIVACAO')"

        data_inicio_str = data_inicio.strftime("%Y-%m-%d")

        query = f"""
        WITH lojas_validas AS (
            SELECT
                id,
                name,
                databricks_tenant_name AS tenant
            FROM {contas_loja_table} AS cl
            WHERE 1=1
            {loja_filter}
            AND NOT REGEXP_CONTAINS(LOWER(cl.name), r'{EXCLUDED_ACCOUNT_REGEXP}')
        ),
        vendas_filtradas AS (
            SELECT
                v.loja_id,
                vi.quantidade,
                vi.valor_final
            FROM {vendas_table} AS v
            INNER JOIN {vendas_item_table} AS vi
                ON vi.venda_id = v.id
            WHERE v.venda_concluida_em >= '{data_inicio_str}'
        ),
        base_vendas AS (
            SELECT
                lv.id,
                lv.name,
                lv.tenant,
                SUM(vf.quantidade) AS quantidade_vendas,
                SUM(vf.valor_final) AS valor_vendas
            FROM vendas_filtradas AS vf
            INNER JOIN lojas_validas AS lv
                ON vf.loja_id = lv.id
            GROUP BY lv.id, lv.name, lv.tenant
        )
        SELECT
            id,
            name,
            tenant,
            quantidade_vendas,
            valor_vendas,
            RANK() OVER (ORDER BY valor_vendas DESC) AS rank_vendas
        FROM base_vendas
        ORDER BY rank_vendas ASC
        """

        return self.sql(query)
