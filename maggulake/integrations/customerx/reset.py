"""
Módulo para operações de reset/limpeza de ambientes CustomerX.

Este módulo fornece funcionalidades para limpar completamente um ambiente do
CustomerX, removendo todos os clientes.

NOTE: Deve ser usado APENAS em ambientes de sandbox.
"""

import time
from typing import Literal

import requests

from maggulake.integrations.customerx.client import (
    CustomerXClient,
    CustomerXCustomerDTO,
)
from maggulake.integrations.customerx.models.contact_customerx import (
    ContactCustomerXDTO,
)
from maggulake.integrations.customerx.models.customerx_group import (
    CustomerXGroupDTO,
)


class CustomerXSandboxReset:
    """Gerenciador de reset de ambiente sandbox CustomerX."""

    def __init__(
        self,
        client: CustomerXClient,
        environment: Literal["sandbox", "production"],
        request_interval_seconds: int = 1,
    ):
        self.client = client
        self.environment = environment
        self.request_interval_seconds = request_interval_seconds
        """Quantidade de segundos para aguardar entre requests (para evitar rate limiting)"""

    def delete_one_customer(self, customer_id: int, customer_name: str) -> None:
        self.client._make_request(  # pylint: disable=protected-access
            method="DELETE",
            endpoint=f"/api/v1/clients/{customer_id}",
        )
        print(f"   ✅ Deletado cliente: [{customer_id}] {customer_name}")

    def delete_one_contact(self, contact_id: int, contact_name: str) -> None:
        """Deleta um contato."""
        self.client._make_request(  # pylint: disable=protected-access
            method="DELETE",
            endpoint=f"/api/v1/contacts/{contact_id}",
        )
        print(f"   ✅ Deletado contato: [{contact_id}] {contact_name}")

    def delete_one_group(self, group_id: int, group_name: str) -> None:
        """Deleta um grupo."""
        self.client._make_request(  # pylint: disable=protected-access
            method="DELETE",
            endpoint=f"/api/v1/groups/{group_id}",
        )
        print(f"   ✅ Deletado grupo: [{group_id}] {group_name}")

    def _print_reset_header(self) -> None:
        """Imprime cabeçalho de início do reset."""
        print("=" * 80)
        print("🧹 RESET DE SANDBOX - CustomerX")
        print("=" * 80)
        print("⚠️  Esta operação irá DELETAR:")
        print("   - TODOS os contatos")
        print("   - TODOS os clientes (matrizes e filiais)")
        print("   - TODOS os grupos")
        print("   Não há como desfazer esta ação!")

    def _delete_all_contacts(
        self, contacts: list[ContactCustomerXDTO], debug: bool
    ) -> tuple[int, int]:
        """Deleta todos os contatos de uma lista."""
        deleted_count = 0
        failed_count = 0

        for contact in contacts:
            contact_id = contact.id_customerx
            contact_name = contact.nome or "Sem Nome"

            if contact_id is None:
                failed_count += 1
                if debug:
                    print(f"   ⚠️  Contato sem ID: {contact_name}")
                continue

            try:
                self.delete_one_contact(contact_id, contact_name)
                deleted_count += 1
            except requests.RequestException as e:
                failed_count += 1
                if debug:
                    print(
                        f"   ❌ Falha ao deletar contato [{contact_id}] {contact_name}: {e}"
                    )

            time.sleep(self.request_interval_seconds)

        return deleted_count, failed_count

    def _delete_all_customers(
        self, clientes: list[CustomerXCustomerDTO], debug: bool
    ) -> tuple[int, int]:
        """Deleta todos os clientes de uma lista."""
        deleted_count = 0
        failed_count = 0

        for cliente in clientes:
            id_cx = cliente.id_customerx
            nome = cliente.nome_empresa or "Sem Nome"

            if id_cx is None:
                failed_count += 1
                if debug:
                    print(f"   ⚠️  Cliente sem ID: {nome}")
                continue

            try:
                self.delete_one_customer(id_cx, nome)
                deleted_count += 1
            except requests.RequestException as e:
                failed_count += 1
                if debug:
                    print(f"   ❌ Falha ao deletar [{id_cx}] {nome}: {e}")

            time.sleep(self.request_interval_seconds)

        return deleted_count, failed_count

    def _delete_all_groups(
        self, groups: list[CustomerXGroupDTO], debug: bool
    ) -> tuple[int, int]:
        """Deleta todos os grupos de uma lista."""
        deleted_count = 0
        failed_count = 0

        for group in groups:
            group_id = group.id
            group_name = group.description or "Sem Nome"

            if group_id is None:
                failed_count += 1
                if debug:
                    print(f"   ⚠️  Grupo sem ID: {group_name}")
                continue

            try:
                self.delete_one_group(group_id, group_name)
                deleted_count += 1
            except requests.RequestException as e:
                failed_count += 1
                if debug:
                    print(
                        f"   ❌ Falha ao deletar grupo [{group_id}] {group_name}: {e}"
                    )

            time.sleep(self.request_interval_seconds)

        return deleted_count, failed_count

    def _print_reset_summary(
        self,
        contacts_total: int,
        contacts_deleted: int,
        contacts_failed: int,
        customers_total: int,
        customers_deleted: int,
        customers_failed: int,
        groups_total: int,
        groups_deleted: int,
        groups_failed: int,
    ) -> None:
        print("=" * 80)
        print("🏁 Reset de sandbox concluído!")
        print(
            f"   Contatos: ✅ {contacts_deleted}/{contacts_total} removidos"
            + (f" | ❌ {contacts_failed} falhas" if contacts_failed > 0 else "")
        )
        print(
            f"   Clientes: ✅ {customers_deleted}/{customers_total} removidos"
            + (f" | ❌ {customers_failed} falhas" if customers_failed > 0 else "")
        )
        print(
            f"   Grupos: ✅ {groups_deleted}/{groups_total} removidos"
            + (f" | ❌ {groups_failed} falhas" if groups_failed > 0 else "")
        )
        print("=" * 80)

    def reset_environment(
        self,
        clientes_existentes: list[CustomerXCustomerDTO] | None = None,
        debug: bool = True,
    ) -> dict[str, dict[str, int]]:
        """
        Executa reset completo do ambiente, removendo TODOS os contatos, clientes e grupos.

        Ordem de deleção (respeitando constraints de FK):
        1. Contatos (dependem de clientes)
        2. Clientes (dependem de grupos)
        3. Grupos (podem ser removidos por último)

        Este método é destrutivo e irreversível. Use com extrema cautela.

        Args:
            clientes_existentes: Lista de clientes já obtida (opcional).
                                Se não fornecida, buscará automaticamente.
            debug: Se True, imprime informações detalhadas do processo.

        Returns:
            Dicionário com estatísticas de cada tipo:
            {
                "contacts": {"total": N, "deleted": N, "failed": N},
                "customers": {"total": N, "deleted": N, "failed": N},
                "groups": {"total": N, "deleted": N, "failed": N}
            }
        """

        if debug:
            self._print_reset_header()

        # FASE 1: Deletar contatos (dependem de clientes)
        if debug:
            print("\n🔥 FASE 1: Deletando contatos...")
        contacts = self.client.fetch_all_contacts()
        contacts_total = len(contacts)
        contacts_deleted, contacts_failed = self._delete_all_contacts(contacts, debug)

        # FASE 2: Deletar clientes
        if debug:
            print("\n🔥 FASE 2: Deletando clientes...")
        if clientes_existentes is not None:
            clientes = clientes_existentes
        else:
            # NOTE: A validacao_estrita eh ignorada pois queremos resetar tudo, mesmo clientes sem postgres_uuid
            clientes = self.client.fetch_all_customers(validacao_estrita=False)
        customers_total = len(clientes)
        customers_deleted, customers_failed = self._delete_all_customers(
            clientes, debug
        )

        # FASE 3: Deletar grupos (após clientes)
        if debug:
            print("\n🔥 FASE 3: Deletando grupos...")
        groups = self.client.fetch_all_groups()
        groups_total = len(groups)
        groups_deleted, groups_failed = self._delete_all_groups(groups, debug)

        if debug:
            self._print_reset_summary(
                contacts_total,
                contacts_deleted,
                contacts_failed,
                customers_total,
                customers_deleted,
                customers_failed,
                groups_total,
                groups_deleted,
                groups_failed,
            )

        return {
            "contacts": {
                "total": contacts_total,
                "deleted": contacts_deleted,
                "failed": contacts_failed,
            },
            "customers": {
                "total": customers_total,
                "deleted": customers_deleted,
                "failed": customers_failed,
            },
            "groups": {
                "total": groups_total,
                "deleted": groups_deleted,
                "failed": groups_failed,
            },
        }
