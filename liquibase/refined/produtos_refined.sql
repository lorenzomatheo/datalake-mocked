--liquibase formatted sql

--changeset marcos:1
ALTER TABLE refined.produtos_refined SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');
ALTER TABLE refined.produtos_refined RENAME COLUMN marca TO fabricante;

--changeset marcos:2
ALTER TABLE refined.produtos_refined RENAME COLUMN nome_simples TO marca;

--changeset luizvieira:3
ALTER TABLE refined.produtos_refined ADD COLUMN power_phrase STRING;
