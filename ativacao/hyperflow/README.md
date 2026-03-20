# Implementação HyperFlow

Integracao Maggu \<> HyperFlow \<> WhatsApp Flows

## Flow 1: Registro de usuarios (atendentes, farmacêuticos e gerentes)

Abaixo esta o passo a passo da jornada do usuario:

- 1: Entrar no WhatsApp:

  - -1 - Atendente le o QR Code na mini maggu.
    - [COPILOT] Precisa gerar um QR code apontando para `https://wa.me/yourphonenumber?text=yourmessage`
  - -2 - Atendente usa numero de telefone e codigo loja para contactar via WhatsApp.

- 2: Atendente Envia a mensagem (no link)

  - [HYPERFLOW] Precisa capturar o codigo da loja e salvar em alguma variavel de usuario (@carlosdaniss)
  - [HYPERFLOW] Disparar flow de menu

- 3: Usuario clica em "Menu"

  - 3.1 - Usuario clica em "Falar com o Suporte"
    - [HYPERFLOW] Enviar o link do whatsapp do Zendesk da Maggu (@carlosdaniss)
    - Fim
  - 3.2 - Usuario clica em "Crie seu cadastro"
    - Abre o formulário para captura dos dados do usuario
    - Pre-preencher o "codigo da farmacia" -> [HYPERFLOW] salvar em variavel do usuario
    - [HYPERFLOW] -> Consultar API de usuarios filtrando por loja
    - [COPILOT] -> Criar endpoint que retornar lista de usuarios de uma determinada loja a partir do codigo loja
    - 3.2.1 - Usuario nao encontra seu username,
      - precisa clicar em "Fale com o suporte"
      - Fim
    - 3.2.2 - Usuario seleciona seu username na lista predefinida.
      - [HYPERFLOW] - Apos selecao, pegar os dados existentes do banco e pre-preencher os campos seguintes (cargo, CPF, nome, email)
      - [HYPERFLOW] - Pre-preencher o wpp com base no numero de telefone
    - Usuario Clica em "Aceito os termos de condicao"
      - [HYPERFLOW] - incluir link para os termos de condicao da maggu
    - usuario clica em "Concluir meu cadastro"
      - [HYPERFLOW] - Enviar POST request para API do copilot atualizando dados do username
      - [HYPERFLOW] - Enviar mensagem

- 4: usuario recebe mensagem "Tudo certo com o cadastro, receba o pix!"

  - 4.1 - Usuario clica em "Nao tenho chave pix"
    - [HYPERFLOW] - Enviar msg de "Converse com o suporte"
  - 4.2 - Usuario clica em "Enviar chave pix"
    - Usuario escreve a chave pix
    - [HYPERFLOW] - Usar endpoint externo para verificar os dados da conta
    - Envia mensagem de confirmacao da chave pix.
    - 4.2.1 - Usuario confirma a chave pix
      - [HYPERFLOW] - Usar API do Woovi para enviar pix de R$20
      - [HYPERFLOW] - Enviar mensagem de sucesso
    - 4.2.2 - Usuario clica em "chave pix errada"
      - Recomeçar o fluxo de chave pix (i.e. perguntar chave novamente)

## Decisões:

- Criar um repo novo ou ir salvando no copilot?
- Qual o codigo da loja que vamos usar?
  - CNPJ? Codigo randômico?
  - Se exigirmos um codigo de loja genérico (e.g. CNPJ da loja), qualquer um que tenha esse numero de telefone poderá se cadastrar na maggu e ganhar os R$20 de pix.
- Como garantir a unicidade do cadastro?
- Qual será o fluxo para exceções e erros?
- O que fazer se o usuario interromper o fluxo na metade? Timeout?
  - R: Combinamos que por enquanto nao vamos nos preocupar com isso

## Observacoes:

- Pode usar o [ambiente de desenvolvimento do flows](https://business.facebook.com/latest/whatsapp_manager/flow_edit) para validar os arquivos .json
- tem que testar no próprio Whatsapp do celular pois o Whatsapp Flow não funciona no Whatsapp Web.
- (usuario | chave pix | CPF | celular) -> Nao deveria haver duplicidade na nossa base.
