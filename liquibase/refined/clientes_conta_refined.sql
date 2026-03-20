--liquibase formatted sql

--changeset Gui-FernandesBR:1
ALTER TABLE refined.clientes_conta_refined ADD COLUMN total_de_cestas INT;

--changeset Gui-FernandesBR:2
ALTER TABLE refined.clientes_conta_refined SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE refined.clientes_conta_refined DROP COLUMN programa_fidelidade;
ALTER TABLE refined.clientes_conta_refined DROP COLUMN participacao_pbm;
ALTER TABLE refined.clientes_conta_refined DROP COLUMN participacao_farmacia_popular;
ALTER TABLE refined.clientes_conta_refined DROP COLUMN loja_mais_proxima;
ALTER TABLE refined.clientes_conta_refined DROP COLUMN alergias_ou_restricoes;
ALTER TABLE refined.clientes_conta_refined DROP COLUMN historico_vacinas;
ALTER TABLE refined.clientes_conta_refined DROP COLUMN habitos_vida_saudavel;

--changeset Carlos-Daniel:3
ALTER TABLE refined.clientes_conta_refined ADD COLUMN medicamentos_tarjados ARRAY<STRING>;

--changeset Gui-FernandesBR:4
ALTER TABLE standard.clientes_conta_standard DROP COLUMN tags_triagem;

--changeset Gui-FernandesBR:5
ALTER TABLE refined.clientes_conta_refined ADD COLUMN ja_comprou_medicamento BOOLEAN;

--changeset Carlos-Daniel:6
ALTER TABLE refined.clientes_conta_refined ADD COLUMN endereco_completo STRING;
