# Enriquecimento de produtos

Durante o processo de enriquecimento de dados, vamos ler produtos da `refined._produtos_em_processamento` e salvar na mesma, para não sobrescrever a tabela original e correr o risco de a atualização ocorrer pela metade.

Idealmente, somente o primeiro notebook da etapa de enriquecimento do workflow `tudo_producao` (que atualmente é a etapa `agrega_enriquecimento`) deve ler da tabela `refined.produtos_refined` e somente a última etapa deve salvar na tabela `refined.produtos_refined`.
