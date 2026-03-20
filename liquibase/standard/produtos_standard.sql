--liquibase formatted sql

--changeset marcos:1
ALTER TABLE standard.produtos_standard SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');
ALTER TABLE standard.produtos_standard RENAME COLUMN marca TO fabricante;

--changeset marcos:2
ALTER TABLE standard.produtos_standard RENAME COLUMN nome_simples TO marca;

--changeset carlosdani:3
ALTER TABLE standard.produtos_standard ADD COLUMNS (match_nao_medicamentos_em TIMESTAMP);