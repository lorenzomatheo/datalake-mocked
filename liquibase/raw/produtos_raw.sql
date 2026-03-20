--liquibase formatted sql

--changeset marcos:1
ALTER TABLE raw.produtos_raw SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');
ALTER TABLE raw.produtos_raw RENAME COLUMN marca TO fabricante;

--changeset marcos:2
ALTER TABLE raw.produtos_raw RENAME COLUMN nome_simples TO marca;

--changeset marcos:3
ALTER TABLE raw.produtos_raw ADD COLUMN gerado_em TIMESTAMP;
UPDATE raw.produtos_raw SET gerado_em = current_timestamp();
ALTER TABLE raw.produtos_raw ALTER COLUMN gerado_em SET NOT NULL;
