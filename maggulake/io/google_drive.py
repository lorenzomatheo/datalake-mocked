from pydrive2.drive import GoogleDrive


def update_or_create_file(
    drive: GoogleDrive, file_name: str, folder_id: str, content: str
) -> None:
    # Busca arquivo
    file_list = drive.ListFile(
        {'q': f"title='{file_name}' and '{folder_id}' in parents and trashed=false"}
    ).GetList()

    # Seleciona arquivo ou cria novo
    file = (
        file_list[0]
        if file_list
        else drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
    )

    # Define novo conteúdo e faz upload
    file.SetContentString(content)
    file.Upload()

    print(
        f"Arquivo '{file_name}' foi {'atualizado' if file_list else 'criado'} com sucesso!"
    )
