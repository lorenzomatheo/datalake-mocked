--liquibase formatted sql

--changeset Gui-FernandesBR:1
ALTER TABLE standard.clientes_conta_standard SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE standard.clientes_conta_standard DROP COLUMN programa_fidelidade;
ALTER TABLE standard.clientes_conta_standard DROP COLUMN participacao_pbm;
ALTER TABLE standard.clientes_conta_standard DROP COLUMN participacao_farmacia_popular;

--changeset Gui-FernandesBR:2
ALTER TABLE standard.clientes_conta_standard DROP COLUMN tags_triagem;

--changeset Carlos-Daniel:3
ALTER TABLE standard.clientes_conta_standard ADD COLUMN (
    endereco_completo STRING
);
