## Regra do Projeto: busca de contatos por período (PONTO FIXO)

### Objetivo
Evitar regressões no ponto que já deu problema: **“contatos criados ontem/hoje/essa semana/últimos 7 dias”**.

### Regra (não quebrar)
- Consultas simples por período devem usar **fluxo determinístico** (`direct_contact_list`) e **não** depender do planner/LLM.
- O formato de `sorts` no Search do HubSpot deve permanecer como **lista de strings** (ex.: `["-createdate"]`).

### Antes de qualquer mudança nesse trecho
Rodar obrigatoriamente:
- `python livia_sdr_motor.py --smoke-search`
- `python livia_sdr_motor.py --debug-yesterday-contacts`

Se qualquer um falhar, a mudança não pode ser considerada segura.

