--liquibase formatted sql

--changeset Gui-FernandesBR:1
ALTER TABLE analytics.clusterizacao_lojas ADD COLUMNS (nome_loja STRING, conta_loja STRING, codigo_loja STRING);
ALTER TABLE analytics.clusterizacao_lojas ADD COLUMNS (pontos_roxos_totais BIGINT);
ALTER TABLE analytics.clusterizacao_lojas ADD COLUMNS (quantidade_atendimentos_total BIGINT);
ALTER TABLE analytics.clusterizacao_lojas ADD COLUMNS (cnpj STRING);
ALTER TABLE analytics.clusterizacao_lojas ADD COLUMNS (ativo BOOLEAN);
