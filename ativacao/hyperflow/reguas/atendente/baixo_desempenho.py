# Se o atendente está há mais de três dias (ou um tempo que podemos definir por meio de uma variavel) sem vender produtos de missão

# Esse aqui é mais fácil, só precisa calcular a data de ultima venda de produto de missao e comparar com a data atual
# Mas pra isso vai ter um passo a passo chatinho de fazer:
# 1. Pegar a lista de atendentes do banco
# 2. Pegar a lista de mensagens enviadas
# 3. Pegar os status atuais dos atendentes (banco)
# 4. Pegar a lista de missoes e produtos de missoes que cada atendente está correndo
# 5. Pegar lista de vendas... (usar a tabela do Bigquery) (essa parte aqui é mais chata)
# 6. Fazer a comparação de quantos dias se passaram desde a ultima venda de produto de missao
# 7. Criar loop para logica de envio, assim como fizemos no 'reporte_diario'
