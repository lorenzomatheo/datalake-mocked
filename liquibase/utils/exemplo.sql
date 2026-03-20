--liquibase formatted sql

--changeset marcos:1
CREATE TABLE utils.exemplo (a int, b timestamp, c struct<a int>, d array<string>);

--changeset marcos:2
ALTER TABLE utils.exemplo SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5');
ALTER TABLE utils.exemplo RENAME COLUMN a TO x;

--changeset marcos:3
DROP TABLE utils.exemplo;
