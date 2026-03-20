# Regua de comunicação 2.0

## Import dados airtable

- [ ] Baixar dados do airtable e salvar num google sheets de backup.
- [ ] Empurrar dados do airtable para o admin (pessoas, lojas e redes).

## Tipos de mensagens:

- Subiu de nivel:
  a. Se o atendente passou de nivel em alguma missão (rodar notebook a cada 1 hora).
- Baixo desempenho:
  a. Se o atendente está há mais de três dias ( ou um tempo que podemos definir) sem vender produtos de missão (E se o gerente não atualizou o status de mensagens dele)
- Fim da rodada:
  a. Se o atendente tem saldo a receber
  b. Se o atendente nao tem saldo a receber

## Premissas importantes:

- Nao havera mais distincao entre "missao interna" e "missao maggu". Se passou de nivel, a mensagem é a mesma. Por isso acho importante comunicar magguletes, não dinheiro, pois quem define o cambio de dinheiro/magguletes é o dono da loja (isso na missão interna).
- Todas as missoes serao utilizadas para envio de notificacoes. Nao ha possibilidade de "ligar/desligar" as missões
- Por default, todos os atendentes recebem comunicacoes da regua. Porem o atendente pode parar de receber comunicacoes de 3 formas:
  - O atendente clica na opcao "interromper comunicacoes"
  - Endpoint desmarca "deve_receber_comunicoes_whatsapp" no copilot.
  - O gerente da pessoa informa que o atendente precisa para de receber mensagens, entao recai o caso 2.
