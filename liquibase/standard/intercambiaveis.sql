--liquibase formatted sql

--changeset marcos:1
ALTER TABLE standard.intercambiaveis SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');
ALTER TABLE standard.intercambiaveis RENAME COLUMN nome_simples_referencia TO marca_referencia;
ALTER TABLE standard.intercambiaveis RENAME COLUMN nome_simples_similar TO marca_similar;
