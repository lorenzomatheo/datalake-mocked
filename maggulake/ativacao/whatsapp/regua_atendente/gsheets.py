import warnings
from typing import List

import pandas as pd
from gspread import Spreadsheet

from maggulake.ativacao.whatsapp.enums import AbasPlanilhaReguasAtendente
from maggulake.ativacao.whatsapp.regua_atendente.models import MensagemEnviadaRow

SHEETS_LINK_MISSAO_INTERNA = "https://docs.google.com/spreadsheets/d/13847F3YqyzsAUXx0AApoNsLkGqD82WSox8PUf_UclYs/edit?gid=0"
SHEETS_LINK_MISSAO_MAGGU = "https://docs.google.com/spreadsheets/d/1qK2ZZcy2asTQt6KbNiEuBRb44a8ejXMKbafXkW3EV90/edit?gid=434203094"


# === Retriever das planilhas de cada aba ==============================================


def _get_sheets_atendentes(spreadsheet: Spreadsheet) -> pd.DataFrame:
    df = spreadsheet.worksheet(
        AbasPlanilhaReguasAtendente.CONTATOS.value
    ).get_all_records()

    df = pd.DataFrame(df)

    # Precisa "limpar" a coluna username
    df["username"] = df["username"].str.strip()
    df["username"] = df["username"].replace("\n", "").replace("\t", "")

    # Padroniza coluna de datas
    df["data_inicio_desafio"] = pd.to_datetime(
        df["data_inicio_desafio"], format="%d/%m/%Y", errors="coerce"
    )

    # Padroniza colunas booleanas
    df["usuario_ativo"] = df["usuario_ativo"] == "TRUE"
    df["eh_usuario_somente_teste"] = df["eh_usuario_somente_teste"] == "TRUE"
    df["ja_enviou_desafio_comecou"] = df["ja_enviou_desafio_comecou"] == "TRUE"

    # --- Lógica de Remoção de Duplicatas ---
    colunas_chave = ["username"]
    df_teste = df[df["eh_usuario_somente_teste"] == True]
    # 2. Trabalhar apenas com os usuários que NÃO são de teste
    df_prod = df[df["eh_usuario_somente_teste"] == False]

    # 3. Encontrar e remover duplicatas APENAS do df de produção
    duplicatas_em_prod = df_prod.duplicated(subset=colunas_chave, keep=False)

    if duplicatas_em_prod.any():
        warnings.warn(
            "Foram encontradas e removidas todas as aparições de 'username' duplicados "
            "entre os usuários de produção."
        )
        # Mantém apenas os usuários de produção que NÃO são duplicados
        df_prod_sem_duplicatas = df_prod[~duplicatas_em_prod]
    else:
        df_prod_sem_duplicatas = df_prod

    # 4. Juntar os usuários de produção já filtrados com os de teste (que não foram tocados)
    df = pd.concat([df_prod_sem_duplicatas, df_teste], ignore_index=True)

    # --- Filtros Finais ---
    # Remover usuários que não estão ativos
    df = df[df["usuario_ativo"] == True]

    return df


def _get_sheets_mensagens_enviadas(spreadsheet: Spreadsheet) -> pd.DataFrame:
    df = spreadsheet.worksheet(
        AbasPlanilhaReguasAtendente.MENSAGENS_ENVIADAS.value
    ).get_all_records()
    df = pd.DataFrame(df)

    return df


def _get_sheets_missoes(spreadsheet: Spreadsheet) -> pd.DataFrame:
    df = spreadsheet.worksheet(
        AbasPlanilhaReguasAtendente.IDS_MISSOES.value
    ).get_all_records()
    df = pd.DataFrame(df)

    # Limpeza de colunas
    df["id_missao"] = df["id_missao"].str.strip()
    df = df[df["id_missao"].notna() & (df["id_missao"] != "")]

    return df


def _get_sheets_ids_imagens(spreadsheet: Spreadsheet) -> pd.DataFrame:
    df = spreadsheet.worksheet(
        AbasPlanilhaReguasAtendente.IDS_IMAGENS.value
    ).get_all_records()
    df = pd.DataFrame(df)
    return df


def get_sheets_regua_na_aba_correta(
    spreadsheet: Spreadsheet, aba: AbasPlanilhaReguasAtendente
) -> pd.DataFrame:
    match aba:
        case AbasPlanilhaReguasAtendente.CONTATOS:
            return _get_sheets_atendentes(spreadsheet)
        case AbasPlanilhaReguasAtendente.MENSAGENS_ENVIADAS:
            return _get_sheets_mensagens_enviadas(spreadsheet)
        case AbasPlanilhaReguasAtendente.IDS_MISSOES:
            return _get_sheets_missoes(spreadsheet)
        case AbasPlanilhaReguasAtendente.IDS_IMAGENS:
            return _get_sheets_ids_imagens(spreadsheet)
        case _:
            raise ValueError(f"Aba {aba} não reconhecida.")


# === Outros ===================================================================


def troca_coluna_para_true(
    username: str,
    nome_usuario: str,
    coluna: str,
    spreadsheet,  # Obriga o spreadsheet para evitar reautenticar toda hora
) -> None:
    sheet = spreadsheet.worksheet(AbasPlanilhaReguasAtendente.CONTATOS.value)

    all_records = sheet.get_all_records()
    for idx, record in enumerate(all_records):
        username_da_linha = str(record.get("username", "")).strip()
        nome_usuario_da_linha = str(record.get("nome", "")).strip()

        # O strip eh importante pra comparacao, mas nada mais do que isso.
        if (
            username_da_linha != username.strip()
            or nome_usuario_da_linha != nome_usuario.strip()
        ):
            continue

        row_number = idx + 2
        headers = list(all_records[0].keys()) if all_records else []

        if coluna not in headers:
            raise ValueError(f"Coluna '{coluna}' não encontrada na planilha.")

        col_number = headers.index(coluna) + 1
        sheet.update_cell(row_number, col_number, "TRUE")
        print(f"Atualizada coluna '{coluna}' para TRUE para o usuário '{username}'.")
        return  # Precisa quebrar o loop depois de atualizar.

    raise ValueError(
        f"Usuário '{username}' com nome '{nome_usuario}' não encontrado na planilha."
    )


def grava_mensagens_enviadas(spreadsheet, msgs: List[MensagemEnviadaRow]) -> None:
    if not msgs:
        print("Nenhuma mensagem para gravar.")
        return

    sheet = spreadsheet.worksheet(AbasPlanilhaReguasAtendente.MENSAGENS_ENVIADAS.value)

    new_rows = [
        [
            msg.id_remetente,
            msg.numero_destinatario,
            msg.username,
            msg.mensagem,
            msg.data_hora_envio.strftime("%Y-%m-%d %H:%M:%S"),
            str(msg.testes).upper(),
            msg.tipo_mensagem,
        ]
        for msg in msgs
    ]
    sheet.append_rows(new_rows)
    print(f"Gravadas {len(msgs)} mensagens na planilha de mensagens enviadas.")
