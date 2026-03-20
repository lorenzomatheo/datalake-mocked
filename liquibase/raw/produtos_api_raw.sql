--liquibase formatted sql

--changeset marcos:1
ALTER TABLE raw.produtos_api_raw SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');
ALTER TABLE raw.produtos_api_raw RENAME COLUMN marca TO fabricante;

--changeset marcos:2
ALTER TABLE raw.produtos_api_raw RENAME COLUMN nome_simples TO marca;
