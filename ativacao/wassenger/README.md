# Automações WhatsApp — Wassenger (Legado)

**Status:** Descontinuado
**Período:** Nov/2024 — Dez/2025

Resumo curto:

- Implementamos automações de envio via WhatsApp usando a plataforma não oficial Wassenger para permitir envios em massa (incluindo grupos). Devido a limitações operacionais e riscos de bloqueio, o serviço foi substituído por Hyperflow (API oficial/Meta) e desligado em 22/12/2025.

Motivação

- Objetivo: automatizar réguas de comunicação (envios para lojas, atendentes e testes de cartela digital).
- Requisito determinante: necessidade de disparos em massa para grupos, recurso disponível no Wassenger à época.

Arquitetura e limitações principais

- Papel: broker entre nossos serviços e o WhatsApp (API não oficial).
- Limitações observadas:
  - Escalabilidade insuficiente para volumes crescentes.
  - Instabilidade e erros intermitentes exigindo intervenção manual.
  - Risco de banimento de números por uso de rota não oficial.

Migração e encerramento

- Decisão: migrar para Hyperflow (API oficial da Meta) para melhorar estabilidade e conformidade. Note que Hyperflow não suporta envios para grupos — o fluxo de comunicação foi ajustado para envios individuais quando necessário.
- Data de desligamento: 22/12/2025

Checklist de encerramento (concluído)

- **Infraestrutura:** artefatos legados removidos no Databricks; ajustes no Terraform (PR #841).
- **Contratos:** assinaturas encerradas (Wassenger, Botconversa).
- **Código:** integrações e repositórios limpos.

Links e referências

- Notion (histórico das atividades): https://www.notion.so/1301b9bda5b88028a6a9c009a3525802
- Miro (fluxos): https://miro.com/app/board/uXjVK4OH7hE=

Observação final

- Este documento é histórico. Para novas implementações, use a documentação do Hyperflow e siga práticas oficiais da Meta.
