# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import Counter
import json
import logging
import os
import re
import sys
import threading
import time
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from requests.exceptions import RequestException, Timeout

HUBSPOT_BASE = "https://api.hubapi.com"
ANTHROPIC_BASE = "https://api.anthropic.com"

# Reutiliza conexão (mais estável/rápido que requests.post solto)
_ANTHROPIC_SESSION = requests.Session()

# Token de acesso do HubSpot (curta duração). Renovado automaticamente via refresh token em caso de 401.
_HUBSPOT_ACCESS_TOKEN: Optional[str] = None
_CAPABILITY_CACHE: Optional[dict[str, Any]] = None
CAPABILITY_CACHE_PATH = os.path.join(os.path.dirname(__file__), "hubspot_capability_cache.json")

# [CORRIGIDO] CRÍTICO 3 — lock reentrante para leitura/escrita segura do cache em disco
_CACHE_LOCK = threading.RLock()

_logger = logging.getLogger("livia")
if not _logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))
    _logger.addHandler(_handler)
    _logger.setLevel(logging.INFO)

HUBSPOT_OBJECT_CONFIGS: dict[str, dict[str, Any]] = {
    "contacts": {
        "date_property": "createdate",
        "default_properties": ["firstname", "lastname", "email", "phone", "mobilephone", "createdate", "hubspot_owner_id"],
        "supports_pipelines": False,
    },
    "leads": {
        "date_property": "hs_createdate",
        "default_properties": ["hs_lead_name", "hs_lead_label", "hs_pipeline", "hs_pipeline_stage", "hs_createdate", "hubspot_owner_id"],
        "supports_pipelines": True,
    },
    "deals": {
        "date_property": "createdate",
        "default_properties": ["dealname", "dealstage", "pipeline", "amount", "createdate", "hubspot_owner_id"],
        "supports_pipelines": True,
    },
    "tasks": {
        "date_property": "hs_timestamp",
        "default_properties": ["hs_task_subject", "hs_task_status", "hs_task_priority", "hs_timestamp", "hubspot_owner_id"],
        "supports_pipelines": False,
    },
    "meetings": {
        "date_property": "hs_meeting_start_time",
        "default_properties": ["hs_meeting_title", "hs_meeting_start_time", "hs_timestamp", "hubspot_owner_id"],
        "supports_pipelines": False,
    },
}


SYSTEM_PROMPT = """Você é Lívia, uma Agente Autônoma SDR com permissões totais no HubSpot CRM v3.
Seu comportamento deve ser focado em EXECUÇÃO DIRETA, evitando verificações (GET) redundantes.

AUTO-CORREÇÃO DE ERROS (CRÍTICO):
- Se uma ferramenta retornar erro HTTP (especialmente 400), você NÃO pode desistir na primeira falha.
- Leia o `tool_result` completo (HubSpot retorna JSON com `message`, `errors` e `context`), identifique qual parâmetro está inválido e corrija o JSON.
- Refaça a chamada no mesmo turno, com pelo menos 3 tentativas incrementais de correção antes de concluir que não foi possível.
- Em caso de 400, priorize ajustar: `sorts`, `filterGroups`, `properties`, nomes de propriedades e operadores.

AUTENTICAÇÃO / TOKEN (IMPORTANTE):
- Nunca diga "token expirou" a menos que o tool_result mostre HTTP 401 do HubSpot.
- Se houver 401, o sistema tentará renovar automaticamente via refresh token; em seguida, repita a chamada que falhou.

DIRETRIZ ULTRA PARA ALTO VOLUME (CRÍTICO):
- Quando precisar atualizar MUITOS registros com as MESMAS propriedades (ex.: 60+ leads para IQL, mover fase/pipeline para a mesma etapa),
  NUNCA monte arrays JSON enormes nem use a ferramenta universal com batch/update manual.
- Use SEMPRE a ferramenta nativa: hubspot_bulk_update_tool.
- Você só fornece: object_type, record_ids (lista) e properties_to_update (um dict único aplicado a todos).
- O sistema monta o payload correto, faz chunk automático de 100 em 100 e retorna um resumo.

- Se você NÃO tiver a lista de IDs (ou se forem muitos), use a ferramenta ainda mais forte:
  hubspot_bulk_update_by_search_tool
  Você fornece object_type + search_body (filtros) + properties_to_update, e o sistema busca os IDs e atualiza em lote.
  Assim você evita travar por output ao listar dezenas/centenas de IDs.

PLAYBOOK DE SDR - REGRAS DE OURO DO ALMEIDA ARANTES (CRÍTICO):
Você não é um robô de cliques, você é uma SDR Plena. Antes de decidir prospectar ou avançar um lead, você DEVE aplicar estas regras lógicas:

Leitura de Contexto Histórico: Antes de abordar um lead antigo, verifique as notas e o histórico de contatos dele. Se um lead já foi desqualificado anteriormente por um motivo válido, NÃO o prospecte novamente a menos que o usuário peça explicitamente.

A Regra dos 3 Contatos (Descarte Lógico): Se você notar no histórico que o escritório já tentou falar com o lead 3 vezes e ele nunca atendeu/respondeu, considere esse lead como 'Frio/Descartado'. Não o coloque na fila de prioridades do dia.

Priorização de Origem (Source): Quando ler a propriedade hs_analytics_source (ou a fonte do lead), priorize automaticamente leads que vieram de campanhas pagas (Paid Search, Ads) ou indicações diretas, pois são mais quentes.

Memória de Sessão: Aja como se lembrasse do contexto. Se você analisar um lead agora e o usuário pedir uma ação para esse mesmo lead na próxima mensagem, não faça uma nova busca no HubSpot. Use os dados que você acabou de ler na memória da conversa.

5. Criação de Tarefas: Você está PROIBIDA de criar tarefas usando a universal_api_call. Sempre use a ferramenta hubspot_create_task_tool para agendar follow-ups de forma segura, passando a quantidade de dias para o vencimento (ex: 3).

6. Agendamento de Reuniões: O utilizador fala sempre em Brasília (BRT). NUNCA calcule timestamps de cabeça. Use EXCLUSIVAMENTE as variáveis pré-calculadas do seu contexto de tempo. Você DEVE ler a hora exata pedida (ex: 17h) e buscar a variável EXATA correspondente na lista (ex: AMANHA_17H_BRT_epoch_ms). NUNCA copie o exemplo das 14h se o usuário pediu outro horário.

7. TRAVA DE ASSOCIAÇÕES: Você está ESTRITAMENTE PROIBIDA de tentar adicionar ou remover convidados/contatos de reuniões ou negócios alterando propriedades de texto (como hs_guest_emails). Isso é falso e não funciona. Para vincular pessoas, use EXCLUSIVAMENTE a hubspot_associate_records_tool. Para desvincular/remover pessoas, use EXCLUSIVAMENTE a hubspot_remove_association_tool.

8. Listar, cancelar ou remarcar reuniões na timeline: NÃO use POST /crm/v3/objects/meetings/search para listar reuniões de um contato (costuma gerar HTTP 400). Use hubspot_list_meetings_for_contact_tool para obter os engagement_id. Para cancelar (desativar) ou remarcar horário, use hubspot_update_meeting_engagement_tool com engagement_id e, ao remarcar, start_time_ms vindo do contexto de tempo (ex.: AMANHA_14H_BRT_epoch_ms ou variável equivalente para depois de amanhã às 14:30 se estiver no contexto).

9. Agenda global (sem nome de cliente): se o usuário falar de reuniões em um dia/período ('amanhã', 'hoje') sem citar contato, NÃO varra a base de contatos. Use hubspot_check_calendar_tool com start_time_ms e end_time_ms do CONTEXTO DE TEMPO (ex.: AMANHA_inicio_epoch_ms e AMANHA_fim_epoch_ms).

COMUNICAÇÃO COM LEIGOS E EXECUÇÃO SILENCIOSA (CRÍTICO):

O usuário que está conversando com você NÃO é um programador. Ele é um advogado ou assistente leigo.

VOCÊ ESTÁ ESTRITAMENTE PROIBIDA de mencionar termos técnicos nas suas respostas (ex: 'API', 'endpoint', 'JSON', 'payload', 'IDs', 'read-only', 'CRM Associations', etc.).

Se uma ferramenta falhar (ex: bloqueio de atualização de propriedade) e você souber uma rota alternativa (ex: associar via outra ferramenta), NÃO PEÇA PERMISSÃO. Execute a alternativa imediatamente, de forma silenciosa, no mesmo turno de pensamento.

Só fale com o usuário quando o trabalho estiver 100% concluído, dizendo de forma natural: 'Reunião atualizada com sucesso!' ou, se falhar de vez, 'Tive um problema no sistema para atualizar, pode verificar no HubSpot?'.

Se precisar de uma informação que faltou, peça em linguagem de negócios (ex: 'Qual é o e-mail da pessoa?') e nunca em linguagem de máquina (ex: 'O campo hs_guest_emails está nulo').

10. TRAVA ANTI-ALUCINAÇÃO DE SUCESSO: Você está ESTRITAMENTE PROIBIDA de dizer que uma ação de criação, atualização ou associação foi 'feita' ou 'concluída' se você usou apenas ferramentas de leitura (Search/Read). Para associar registros que já existem, use SEMPRE a ferramenta hubspot_associate_records_tool e só confirme ao usuário se o retorno da ferramenta for um SUCESSO REAL.

DICIONÁRIO DE PROPRIEDADES DO HUBSPOT (CRÍTICO):
- Fases do Funil (Pipeline Stages): quando o usuário pedir para filtrar ou alterar "fase", "etapa", "pipeline" ou "coluna" de um LEAD,
  NUNCA use hs_lead_status. A propriedade correta da etapa do funil é:
  - hs_pipeline_stage (etapa/fase)
  - hs_pipeline (o funil/pipeline em si)
  Exemplo de busca por fase:
  POST /crm/v3/objects/leads/search
  Body: {"filterGroups":[{"filters":[{"propertyName":"hs_pipeline_stage","operator":"EQ","value":"<stage-id>"}]}]}
  Exemplo de update em lote por fase:
  POST /crm/v3/objects/leads/batch/update
  Body: {"inputs":[{"id":"<LEAD_ID>","properties":{"hs_pipeline_stage":"<stage-id>","hs_pipeline":"<pipeline-id>"}}]}

- Tipos/Labels de Lead (IQL/MQL/SQL etc): quando o usuário quiser classificar o lead (tipo/label),
  use preferencialmente hs_lead_label. Em algumas contas pode existir hs_lead_type; se houver dúvida, verifique o schema (via API) antes de assumir.
  Exemplo batch para mudar label:
  {"inputs":[{"id":"123","properties":{"hs_lead_label":"IQL"}}]}

MODO EXECUÇÃO IMEDIATA (SEM PERGUNTAS):
- Se o usuário disser "crie" / "associe" / "faça" / "gere" / "cadastre" (ou equivalente), isso é uma ORDEM.
- Você deve responder executando tool_use imediatamente (HubSpot), sem pedir clarificação.
- Só peça clarificação se estiver faltando um identificador essencial (ex.: nenhum ID/nome/critério foi fornecido e não há como inferir).

REGRA DE ALVOS (quando você já listou contatos sem lead):
- Se você acabou de listar contatos "SEM Lead" e incluiu IDs, e o usuário disser "agora crie/associe", os alvos são exatamente esses IDs listados por você.
- Não reconsulte a lista. Não pergunte "tem certeza?". Apenas execute:
  1) criar Lead para cada contato (nome = nome completo do contato)
  2) associar Lead->Contato
  3) ao final, confirmar os IDs criados/associados.

REGRA DE PERFORMANCE:
- Para listas, use batch sempre que possível (associações e associações em lote), evitando 1 chamada por contato.

ASSOCIAÇÕES (CRÍTICO PARA VELOCIDADE):
Você deve evitar completamente fazer 1 GET de associação por contato quando estiver lidando com listas (ex.: 10/20/100 contatos).
Use SEMPRE leitura em lote (batch read) para buscar associações em uma única chamada.

RETRIEVE ASSOCIATED RECORDS (OFICIAL):
- Batch read: POST /crm/associations/2026-03/{fromObjectType}/{toObjectType}/batch/read
  Payload: {"inputs":[{"id":"<FROM_ID_1>"},{"id":"<FROM_ID_2>"}]}
  Limite: até 1000 inputs por request.
  Resposta retorna, para cada "from.id", uma lista "to" com "toObjectId" e "associationTypes" (label/category/typeId).

- Single record associations (use só se for 1 registro isolado):
  GET /crm/objects/2026-03/{fromObjectType}/{objectId}/associations/{toObjectType}

COMO USAR NA PRÁTICA (Contato -> Lead -> Negócio):
1) Você já tem uma lista de contatos (ex.: 10 ou 20 ids). Faça UM batch read:
   POST /crm/associations/2026-03/contacts/leads/batch/read com inputs dos contact ids.
2) Para cada contato:
   - Se o array "to" estiver vazio => não tem Lead associado => crie Lead e associe.
   - Se existir "toObjectId" => já tem Lead => guarde os leadIds.
3) Para buscar Negócios a partir dos leads, faça outro batch read:
   POST /crm/associations/2026-03/leads/deals/batch/read com inputs dos leadIds.
4) Só depois busque detalhes:
   - Leads: POST /crm/v3/objects/leads/batch/read (se disponível) OU GET /crm/v3/objects/leads/{id} em lote pequeno
   - Deals: POST /crm/v3/objects/deals/batch/read (se disponível) OU GET /crm/v3/objects/deals/{id}

LABELS / typeId (quando um PUT falhar):
- Para descobrir labels/typeId entre dois objetos: GET /crm/associations/2026-03/{fromObjectType}/{toObjectType}/labels
- Use o typeId/category retornados para ajustar a criação/remoção de labels se necessário.

REGRAS PARA FILTRO DE DATA (CRÍTICO):
Você SEMPRE consegue filtrar contatos pela data de criação usando o endpoint de SEARCH.
- Use: POST /crm/v3/objects/contacts/search
- Filtro: a propriedade se chama "createdate".
- Operador recomendado para intervalo: BETWEEN, usando epoch em milissegundos (veja exemplo oficial de BETWEEN na documentação do Search).
- Ordenação: para pegar os mais recentes, use "sorts": ["-createdate"]
- Paginação: use "limit": 20. Se precisar mais, use "after" retornado e repita.
- Propriedades: sempre peça no mínimo firstname, lastname, email, phone, mobilephone, createdate, hs_lead_status.

INTERPRETAÇÃO AUTOMÁTICA DE PERÍODOS (NÃO PERGUNTE CLARIFICAÇÃO):
- "última semana": últimos 7 dias (agora-7d até agora).
- "essa semana": de segunda-feira 00:00 (no fuso do usuário, assuma America/Sao_Paulo) até agora.
- "hoje": 00:00 até agora.
- Se o usuário mencionar um intervalo explícito (ex.: "9 a 15 de abril de 2026"), converta para timestamps e aplique.
- Se o usuário pedir "últimos contatos" sem período, assuma "últimos 20" por createdate.

EXEMPLO DE PAYLOAD (createdate BETWEEN em epoch ms):
{
  "filterGroups": [
    {"filters": [
      {"propertyName":"createdate","operator":"BETWEEN","value":"<INICIO_MS>","highValue":"<FIM_MS>"}
    ]}
  ],
  "sorts": ["-createdate"],
  "properties": ["firstname","lastname","email","phone","mobilephone","createdate","hs_lead_status"],
  "limit": 20
}

DOCUMENTAÇÃO OBRIGATÓRIA - CRIAÇÃO E ASSOCIAÇÃO DE LEADS:
Para criar leads e associá-los a contatos, VOCÊ ESTÁ ESTRITAMENTE PROIBIDA de usar a ferramenta universal_api_call.
Você deve usar EXCLUSIVAMENTE a ferramenta hubspot_create_and_associate_lead, passando o ID do contato e o nome do lead.
Se o usuário pedir para colocar em uma fase específica, passe o ID da fase no parâmetro pipeline_stage.

DOCUMENTAÇÃO OBRIGATÓRIA - OBSERVAÇÃO / NOTA EM CONTATO (TIMELINE):
Quando o usuário pedir para registrar "observação", "nota", "comentário" ou "anotação" em um CONTATO,
NUNCA tente gravar isso em propriedade do contato (ex.: "notes", "hs_analytics_notes", campos custom etc.).
O correto é criar um registro do objeto `notes` no HubSpot e associar ao contato.
Use SEMPRE a ferramenta hubspot_create_note_for_contact com contact_id + note_body (texto livre).

PROTOCOLOS DE ERRO (CRÍTICO):
Se a ferramenta retornar um erro HTTP, NÃO FAÇA UM NOVO GET. Leia o tool_result (inclui response_text cru) e ajuste POST/PUT imediatamente.

OPERAÇÕES EM LOTE (BATCH) - CRÍTICO PARA GRANDES VOLUMES:
- Se o usuário pedir para atualizar/deletar/criar/associar dezenas de registros (ex.: 50-200), você está PROIBIDA de executar 1 a 1.
- Você deve usar endpoints batch e dividir em lotes de até 100 registros por chamada (limite típico do HubSpot).
- Regra de ouro: 1 ferramenta batch > 100 ferramentas individuais.

BATCH UPDATE (OFICIAL) - Leads:
Ferramenta: hubspot_universal_api_call
Endpoint: /crm/v3/objects/leads/batch/update
Method: POST
JSON Payload: {"inputs":[{"id":"<LEAD_ID>","properties":{...}}, ...]}
Exemplo:
{
  "inputs": [
    { "id": "401564003176", "properties": { "hs_lead_label": "IQL" } },
    { "id": "401574043297", "properties": { "hs_lead_label": "IQL" } }
  ]
}

BATCH UPDATE (OFICIAL) - Deals e Contacts (mesma ideia):
- /crm/v3/objects/deals/batch/update
- /crm/v3/objects/contacts/batch/update

BATCH ASSOCIATE (v4, default):
- POST /crm/v4/associations/{fromObjectType}/{toObjectType}/batch/associate/default
  Payload: {"inputs":[{"from":{"id":"<FROM_ID>"},"to":{"id":"<TO_ID>"}}]}

REGRAS PARA EVITAR LIMITE DE OUTPUT DO MODELO:
- Não gere dezenas de blocos tool_use na mesma resposta.
- Prefira 1-3 chamadas batch grandes em vez de muitas chamadas pequenas.
"""


def hubspot_create_task_tool(access_token: str, title: str, body: str, days_from_now: int, target_id: str, target_type: str) -> dict[str, Any]:
    try:
        from datetime import datetime, timedelta, timezone

        # Calcula a data futura em milissegundos (Epoch)
        future_date = datetime.now(timezone.utc) + timedelta(days=days_from_now)
        timestamp_ms = int(future_date.timestamp() * 1000)

        # Cria a tarefa
        task_payload = {
            "properties": {
                "hs_task_body": body,
                "hs_task_subject": title,
                "hs_task_status": "WAITING",
                "hs_timestamp": str(timestamp_ms),
            }
        }
        res_task = hubspot_universal_api_call(access_token, endpoint="/crm/v3/objects/tasks", method="POST", json_payload=task_payload)

        if not res_task.get("ok"):
            return {"ok": False, "error": f"Falha ao criar tarefa: {res_task.get('response_text')}"}

        task_id = (res_task.get("response_json") or {}).get("id")

        # Associa a tarefa ao objeto (contact, deal ou lead)
        # HubSpot v4 associations default
        assoc_payload = {"inputs": [{"from": {"id": task_id}, "to": {"id": target_id}}]}
        res_assoc = hubspot_universal_api_call(
            access_token,
            endpoint=f"/crm/v4/associations/tasks/{target_type}/batch/associate/default",
            method="POST",
            json_payload=assoc_payload,
        )

        if not res_assoc.get("ok"):
            return {"ok": False, "error": f"Falha ao associar tarefa {task_id} ao {target_type} {target_id}: {res_assoc.get('response_text')}"}

        return {
            "ok": True,
            "message": f"Tarefa '{title}' (ID: {task_id}) criada para daqui a {days_from_now} dias e associada ao {target_type} {target_id}.",
            "task_id": str(task_id),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def hubspot_create_meeting_tool(
    access_token: str,
    contact_id: str,
    title: str,
    body: str,
    start_time_ms: int,
    duration_minutes: int = 60,
    location: str = "",
) -> dict[str, Any]:
    try:
        end_time_ms = start_time_ms + (duration_minutes * 60 * 1000)

        loc_lower = location.lower()
        location_type = "CUSTOM"
        if "meet" in loc_lower or "zoom" in loc_lower or "teams" in loc_lower or "video" in loc_lower:
            location_type = "VIDEOCONFERENCE"
        elif "telefone" in loc_lower or "ligar" in loc_lower or "call" in loc_lower:
            location_type = "PHONE"
        elif "pessoalmente" in loc_lower or "endereço" in loc_lower or "rua" in loc_lower:
            location_type = "ADDRESS"

        owner_id = None
        contact_res = hubspot_universal_api_call(
            access_token,
            endpoint=f"/crm/v3/objects/contacts/{contact_id}",
            method="GET",
            params={"properties": "hubspot_owner_id"},
        )
        if contact_res.get("ok"):
            owner_id = (contact_res.get("response_json") or {}).get("properties", {}).get("hubspot_owner_id")

        # Se não tiver owner no contato, busca e força o Vinycius (ou primeiro owner)
        if not owner_id:
            owner_res = hubspot_universal_api_call(access_token, endpoint="/crm/v3/owners", method="GET")
            if owner_res.get("ok"):
                owners = (owner_res.get("response_json") or {}).get("results") or []
                for o in owners:
                    if "vinycius" in str(o.get("email", "")).lower():
                        owner_id = str(o.get("id"))
                        break
                if not owner_id and owners:
                    owner_id = str(owners[0].get("id"))

        meeting_payload = {
            "properties": {
                "hs_timestamp": str(start_time_ms),
                "hs_meeting_title": title,
                "hs_meeting_body": body,
                "hs_meeting_start_time": str(start_time_ms),
                "hs_meeting_end_time": str(end_time_ms),
                "hs_internal_meeting_notes": "Agendado via Assistente Livia",
            }
        }

        if location.strip():
            meeting_payload["properties"]["hs_meeting_location"] = location
            meeting_payload["properties"]["hs_meeting_location_type"] = location_type

        if owner_id:
            meeting_payload["properties"]["hubspot_owner_id"] = owner_id

        res_meeting = hubspot_universal_api_call(access_token, endpoint="/crm/v3/objects/meetings", method="POST", json_payload=meeting_payload)
        if not res_meeting.get("ok"):
            return {"ok": False, "error": f"Falha ao criar reunião v3: {res_meeting.get('response_text')}"}

        meeting_id = (res_meeting.get("response_json") or {}).get("id")
        assoc_payload = {"inputs": [{"from": {"id": str(meeting_id)}, "to": {"id": str(contact_id)}}]}
        res_assoc = hubspot_universal_api_call(
            access_token,
            endpoint="/crm/v4/associations/meetings/contacts/batch/associate/default",
            method="POST",
            json_payload=assoc_payload,
        )

        if not res_assoc.get("ok"):
            return {
                "ok": True,
                "message": f"Reunião criada (ID: {meeting_id}), mas erro ao associar ao contato {contact_id}: {res_assoc.get('response_text')}",
            }

        return {"ok": True, "message": f"Reunião '{title}' (ID: {meeting_id}) agendada com sucesso."}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def hubspot_list_meetings_for_contact_tool(access_token: str, contact_id: str, limit: int = 100) -> dict[str, Any]:
    """Lista reuniões associadas a um contato usando a API v3/v4 para garantir compatibilidade de IDs com o Update v3."""
    try:
        assoc_res = hubspot_universal_api_call(
            access_token,
            endpoint=f"/crm/v4/objects/contacts/{contact_id}/associations/meetings",
            method="GET",
        )
        if not assoc_res.get("ok"):
            return {"ok": False, "error": f"Erro ao buscar associações: {assoc_res.get('response_text')}"}

        assoc_data = assoc_res.get("response_json") or {}
        meeting_ids = [str(r.get("toObjectId")) for r in (assoc_data.get("results") or []) if r.get("toObjectId")]

        if not meeting_ids:
            return {"ok": True, "contact_id": str(contact_id), "meetings": []}

        lim = max(1, min(100, int(limit)))
        batch_payload = {
            "inputs": [{"id": mid} for mid in meeting_ids[:lim]],
            "properties": [
                "hs_meeting_title",
                "hs_meeting_start_time",
                "hs_meeting_end_time",
                "hs_meeting_body",
                "hs_meeting_outcome",
            ],
        }

        read_res = hubspot_universal_api_call(
            access_token,
            endpoint="/crm/v3/objects/meetings/batch/read",
            method="POST",
            json_payload=batch_payload,
        )

        if not read_res.get("ok"):
            return {"ok": False, "error": f"Erro ao ler reuniões v3: {read_res.get('response_text')}"}

        rows: list[dict[str, Any]] = []
        for item in (read_res.get("response_json") or {}).get("results") or []:
            props = (item or {}).get("properties") or {}
            rows.append(
                {
                    "engagement_id": item.get("id"),
                    "title": props.get("hs_meeting_title"),
                    "startTime": props.get("hs_meeting_start_time"),
                    "endTime": props.get("hs_meeting_end_time"),
                    "body": (props.get("hs_meeting_body") or "")[:500],
                    "outcome": props.get("hs_meeting_outcome"),
                }
            )

        return {"ok": True, "contact_id": str(contact_id), "meetings": rows}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def hubspot_get_meeting_engagement_tool(access_token: str, engagement_id: str) -> dict[str, Any]:
    try:
        res = hubspot_universal_api_call(access_token, endpoint=f"/engagements/v1/engagements/{int(engagement_id)}", method="GET")
        if not res.get("ok"):
            return {"ok": False, "error": res.get("response_text"), "status_code": res.get("status_code")}
        return {"ok": True, "payload": res.get("response_json") or {}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def hubspot_update_meeting_engagement_tool(
    access_token: str,
    engagement_id: str,
    *,
    active: Optional[bool] = None,
    start_time_ms: Optional[int] = None,
    duration_minutes: Optional[int] = None,
    location: Optional[str] = None,
) -> dict[str, Any]:
    """
    Atualiza registro de reunião no CRM (Meetings v3): PATCH /crm/v3/objects/meetings/{id}.
    engagement_id deve ser o ID do objeto meeting (v3), alinhado ao retorno da listagem quando for o mesmo ID.
    """
    try:
        properties: dict[str, Any] = {}
        if start_time_ms is not None and start_time_ms > 0:
            end_time_ms = start_time_ms + ((duration_minutes or 60) * 60 * 1000)
            properties["hs_timestamp"] = str(start_time_ms)
            properties["hs_meeting_start_time"] = str(start_time_ms)
            properties["hs_meeting_end_time"] = str(end_time_ms)

        if location is not None and location.strip():
            properties["hs_meeting_location"] = location
            loc_lower = location.lower()
            location_type = "CUSTOM"
            if "meet" in loc_lower or "zoom" in loc_lower or "teams" in loc_lower or "video" in loc_lower:
                location_type = "VIDEOCONFERENCE"
            elif "telefone" in loc_lower or "ligar" in loc_lower or "call" in loc_lower:
                location_type = "PHONE"
            elif "pessoalmente" in loc_lower or "endereço" in loc_lower or "rua" in loc_lower:
                location_type = "ADDRESS"
            properties["hs_meeting_location_type"] = location_type

        owner_res = hubspot_universal_api_call(access_token, endpoint="/crm/v3/owners", method="GET")
        if owner_res.get("ok"):
            owners = (owner_res.get("response_json") or {}).get("results") or []
            for o in owners:
                if "vinycius" in str(o.get("email", "")).lower():
                    properties["hubspot_owner_id"] = str(o.get("id"))
                    break

        if active is False:
            properties["hs_meeting_outcome"] = "CANCELED"

        if not properties:
            return {"ok": False, "error": "Nenhum campo válido para atualizar."}

        res = hubspot_universal_api_call(
            access_token,
            endpoint=f"/crm/v3/objects/meetings/{engagement_id}",
            method="PATCH",
            json_payload={"properties": properties},
        )

        if not res.get("ok"):
            return {"ok": False, "error": f"Erro ao atualizar reunião: {res.get('response_text')}"}

        return {"ok": True, "message": "Reunião atualizada com sucesso no V3.", "engagement_id": str(engagement_id)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def hubspot_check_calendar_tool(access_token: str, start_time_ms: int, end_time_ms: int) -> dict[str, Any]:
    try:
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hs_timestamp",
                            "operator": "BETWEEN",
                            "value": str(start_time_ms),
                            "highValue": str(end_time_ms),
                        }
                    ]
                }
            ],
            "properties": ["hs_timestamp", "hs_meeting_title", "hs_meeting_body"],
            "limit": 100,
        }
        res = hubspot_universal_api_call(access_token, endpoint="/crm/v3/objects/meetings/search", method="POST", json_payload=payload)

        if not res.get("ok"):
            return {"ok": False, "error": f"Falha ao ler agenda: {res.get('response_text')}"}

        return {"ok": True, "meetings": (res.get("response_json") or {}).get("results", [])}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def hubspot_associate_records_tool(
    access_token: str,
    from_object_type: str,
    from_object_id: str,
    to_object_type: str,
    to_object_id: str,
) -> dict[str, Any]:
    try:
        endpoint = f"/crm/v4/associations/{from_object_type}/{to_object_type}/batch/associate/default"
        payload = {
            "inputs": [
                {"from": {"id": from_object_id}, "to": {"id": to_object_id}},
            ]
        }
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="POST", json_payload=payload)

        if res.get("ok"):
            return {
                "ok": True,
                "message": f"SUCESSO REAL: O {from_object_type} ({from_object_id}) foi associado ao {to_object_type} ({to_object_id}).",
            }
        return {"ok": False, "error": f"Falha na associação: {res.get('response_text')}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def hubspot_remove_association_tool(
    access_token: str,
    from_object_type: str,
    from_object_id: str,
    to_object_type: str,
    to_object_id: str,
) -> dict[str, Any]:
    try:
        endpoint = f"/crm/v4/associations/{from_object_type}/{to_object_type}/batch/archive"
        payload = {
            "inputs": [
                {"from": {"id": from_object_id}, "to": {"id": to_object_id}},
            ]
        }
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="POST", json_payload=payload)

        if res.get("status_code") == 204 or res.get("ok"):
            return {
                "ok": True,
                "message": (
                    f"SUCESSO REAL: A associação entre o {from_object_type} ({from_object_id}) e o "
                    f"{to_object_type} ({to_object_id}) foi REMOVIDA/DESFEITA com sucesso."
                ),
            }
        return {"ok": False, "error": f"Falha ao remover associação: {res.get('response_text')}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


TOOLS: list[dict[str, Any]] = [
    {
        "name": "hubspot_universal_api_call",
        "description": "Chamada universal para a API REST do HubSpot.",
        "input_schema": {
            "type": "object",
            "properties": {
                "endpoint": {"type": "string"},
                "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"]},
                "params": {"type": ["object", "null"]},
                "json_payload": {"type": ["object", "null"]},
            },
            "required": ["endpoint", "method"],
        },
    }
    ,
    {
        "name": "hubspot_create_and_associate_lead",
        "description": "Use ESTA FERRAMENTA SEMPRE que precisar CRIAR um novo lead e associá-lo a um contato existente. NUNCA use a universal_api_call para isso.",
        "input_schema": {
            "type": "object",
            "properties": {
                "contact_id": {"type": "string", "description": "ID do contato no HubSpot"},
                "lead_name": {"type": "string", "description": "Nome do Lead a ser criado"},
                "pipeline_stage": {"type": "string", "description": "ID da fase/etapa (stage) se o usuário solicitar"},
            },
            "required": ["contact_id", "lead_name"],
        },
    },
    {
        "name": "hubspot_create_note_for_contact",
        "description": "Cria uma NOTA (objeto notes / timeline) associada a um contato. Use para observações, comentários e anotações. Não use bulk_update em propriedades do contato para isso.",
        "input_schema": {
            "type": "object",
            "properties": {
                "contact_id": {"type": "string", "description": "ID do contato no HubSpot"},
                "note_body": {"type": "string", "description": "Texto da nota/observação"},
            },
            "required": ["contact_id", "note_body"],
        },
    },
    {
        "name": "hubspot_create_task_tool",
        "description": "Use ESTA FERRAMENTA SEMPRE que precisar CRIAR UMA TAREFA (task / follow-up) e associá-la a um contato, negócio (deal) ou lead.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Título da tarefa"},
                "body": {"type": "string", "description": "Descrição ou anotações da tarefa"},
                "days_from_now": {"type": "integer", "description": "Quantos dias a partir de hoje a tarefa deve vencer (ex: 3 para D+3)"},
                "target_id": {"type": "string", "description": "ID do contato, negócio ou lead"},
                "target_type": {
                    "type": "string",
                    "description": "O tipo do objeto alvo. Exatamente um destes: 'contacts', 'deals', 'leads'",
                    "enum": ["contacts", "deals", "leads"],
                },
            },
            "required": ["title", "body", "days_from_now", "target_id", "target_type"],
        },
    },
    {
        "name": "hubspot_create_meeting_tool",
        "description": "Use ESTA FERRAMENTA SEMPRE que precisar AGENDAR UMA REUNIÃO com um contacto. NUNCA use a universal_api_call para reuniões.",
        "input_schema": {
            "type": "object",
            "properties": {
                "contact_id": {"type": "string", "description": "ID do contacto no HubSpot"},
                "title": {"type": "string", "description": "Título da reunião"},
                "body": {"type": "string", "description": "Descrição ou pauta (links de videoconferência vão em location)"},
                "start_time_ms": {
                    "type": "integer",
                    "description": "Timestamp em ms. OBRIGATÓRIO buscar no contexto de tempo a variável correspondente à hora exata solicitada (ex: se for 17h, use AMANHA_17H_BRT_epoch_ms).",
                },
                "duration_minutes": {"type": "integer", "description": "Duração em minutos (padrão 60)"},
                "location": {"type": "string", "description": "Local da reunião (ex: link do Zoom, Google Meet, endereço)"},
            },
            "required": ["contact_id", "title", "body", "start_time_ms"],
        },
    },
    {
        "name": "hubspot_check_calendar_tool",
        "description": "Use ESTA FERRAMENTA para consultar a agenda de reuniões de um período específico. Ideal para quando o usuário fala 'reunião de amanhã' sem dizer o nome do cliente.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_time_ms": {"type": "integer", "description": "Timestamp de início em ms (ex: AMANHA_inicio_epoch_ms)"},
                "end_time_ms": {"type": "integer", "description": "Timestamp de fim em ms (ex: AMANHA_fim_epoch_ms)"},
            },
            "required": ["start_time_ms", "end_time_ms"],
        },
    },
    {
        "name": "hubspot_list_meetings_for_contact_tool",
        "description": "Lista reuniões do contato via associações CRM v4 + batch read Meetings v3. Os engagement_id retornados são IDs v3 (compatíveis com hubspot_update_meeting_engagement_tool). Não use meetings/search só para listar por contato.",
        "input_schema": {
            "type": "object",
            "properties": {
                "contact_id": {"type": "string", "description": "ID do contato no HubSpot"},
                "limit": {"type": "integer", "description": "Máximo de reuniões a detalhar (1–100, default 100)"},
            },
            "required": ["contact_id"],
        },
    },
    {
        "name": "hubspot_update_meeting_engagement_tool",
        "description": "Atualiza reunião no CRM (Meetings v3): cancelar (active=false), reagendar (start_time_ms + duration_minutes), local (location). Use após listar com hubspot_list_meetings_for_contact_tool; o ID deve ser o da reunião no CRM v3 quando aplicável.",
        "input_schema": {
            "type": "object",
            "properties": {
                "engagement_id": {"type": "string", "description": "ID numérico do engagement (retornado na listagem)"},
                "active": {"type": "boolean", "description": "false para cancelar/desativar a reunião"},
                "start_time_ms": {
                    "type": "integer",
                    "description": "CRÍTICO: COPIE a variável do contexto de tempo correspondente à hora solicitada (ex: AMANHA_15H_BRT_epoch_ms). É PROIBIDO calcular ou inventar números.",
                },
                "duration_minutes": {"type": "integer", "description": "Duração ao reagendar (default 60)"},
                "location": {"type": "string", "description": "Local da reunião (ex: Google Meet, telefone)"},
            },
            "required": ["engagement_id"],
        },
    },
    {
        "name": "hubspot_associate_records_tool",
        "description": "Use ESTA FERRAMENTA SEMPRE que precisar VINCULAR/ASSOCIAR dois registros existentes (ex: uma reunião a um contato, um lead a um negócio).",
        "input_schema": {
            "type": "object",
            "properties": {
                "from_object_type": {"type": "string", "description": "Tipo do objeto de origem (ex: 'meetings', 'leads', 'contacts')"},
                "from_object_id": {"type": "string", "description": "ID do objeto de origem"},
                "to_object_type": {"type": "string", "description": "Tipo do objeto de destino (ex: 'contacts', 'deals')"},
                "to_object_id": {"type": "string", "description": "ID do objeto de destino"},
            },
            "required": ["from_object_type", "from_object_id", "to_object_type", "to_object_id"],
        },
    },
    {
        "name": "hubspot_remove_association_tool",
        "description": "Use ESTA FERRAMENTA SEMPRE que precisar REMOVER, DESVINCULAR ou RETIRAR a associação entre dois registros (ex: tirar um contato de uma reunião, remover um lead de um negócio).",
        "input_schema": {
            "type": "object",
            "properties": {
                "from_object_type": {"type": "string", "description": "Tipo do objeto de origem (ex: 'meetings', 'leads')"},
                "from_object_id": {"type": "string", "description": "ID do objeto de origem"},
                "to_object_type": {"type": "string", "description": "Tipo do objeto de destino (ex: 'contacts', 'deals')"},
                "to_object_id": {"type": "string", "description": "ID do objeto de destino"},
            },
            "required": ["from_object_type", "from_object_id", "to_object_type", "to_object_id"],
        },
    },
    {
        "name": "hubspot_bulk_update_tool",
        "description": "Atualiza em lote (chunk 100) vários registros com as mesmas propriedades, sem exigir que o LLM formate arrays JSON grandes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "object_type": {"type": "string", "description": "Ex.: 'leads', 'contacts', 'deals'"},
                "record_ids": {"type": "array", "items": {"type": "string"}, "description": "IDs dos registros a atualizar."},
                "properties_to_update": {"type": "object", "description": "Propriedades a aplicar a TODOS os IDs."},
            },
            "required": ["object_type", "record_ids", "properties_to_update"],
        },
    },
    {
        "name": "hubspot_bulk_update_by_search_tool",
        "description": "Busca registros via /crm/v3/objects/{object_type}/search e aplica o mesmo update em massa (chunk 100). Evita o LLM listar muitos IDs.",
        "input_schema": {
            "type": "object",
            "properties": {
                "object_type": {"type": "string", "description": "Ex.: 'leads', 'contacts', 'deals'"},
                "search_body": {
                    "type": "object",
                    "description": "Body completo do endpoint /crm/v3/objects/{object_type}/search (filterGroups, query, sorts, properties, limit, after).",
                },
                "properties_to_update": {"type": "object", "description": "Propriedades a aplicar a todos os registros encontrados."},
                "max_records": {"type": ["integer", "null"], "description": "Máximo de registros a atualizar (default 500)."},
            },
            "required": ["object_type", "search_body", "properties_to_update"],
        },
    },
]


def _chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def _status(msg: str, level: str = "info") -> None:  # [CORRIGIDO] MELHORIA 1 — logging estruturado
    log_fn = getattr(_logger, level, _logger.info)
    log_fn("[Livia] " + msg)


def _safe_truncate_tool_result(content: str, max_chars: int = 8000) -> str:  # [CORRIGIDO] MELHORIA 2 — truncagem JSON-safe
    """
    Trunca resultados grandes sem perder o que é crítico para auto-correção.

    Regra: em erros HTTP (status_code >= 400 / ok=False), preserva o máximo possível de:
    - response_text
    - response_json (especialmente context/errors/message)
    """
    if len(content) <= max_chars:
        return content
    try:
        obj = json.loads(content)
        if isinstance(obj, dict):
            is_error = (obj.get("ok") is False) or (int(obj.get("status_code") or 0) >= 400)

            # Sempre pode remover payload ecoado (normalmente enorme) sem prejudicar o diagnóstico.
            if "json_payload" in obj:
                obj["json_payload"] = obj.get("json_payload") if is_error else "(truncado)"
            if "response_headers" in obj and not is_error:
                obj["response_headers"] = "(truncado)"

            # Em sucesso, podemos truncar campos grandes agressivamente.
            if not is_error:
                for big_key in ["response_text", "details", "sample_rows", "samples"]:
                    if big_key in obj and isinstance(obj[big_key], (str, list)):
                        obj[big_key] = "(truncado)"
            else:
                # Em erro, preserve response_text/response_json, mas reduza partes ruidosas.
                for big_key in ["sample_rows", "samples", "results"]:
                    if big_key in obj and isinstance(obj[big_key], list) and len(obj[big_key]) > 10:
                        obj[big_key] = obj[big_key][:10] + ["...(truncado)"]

            truncated = json.dumps(obj, ensure_ascii=False)
            if len(truncated) <= max_chars:
                return truncated
    except Exception:
        pass
    return content[:max_chars] + "\n...(truncado)"


def _hubspot_merge_new_token(target: dict[str, Any], res: dict[str, Any]) -> None:  # [CORRIGIDO] CRÍTICO 1 — propagação de token
    nt = res.get("new_access_token")
    if nt:
        target["new_access_token"] = str(nt)


def _hubspot_token_sync_from_tool_result(current: str, result: Any) -> str:  # [CORRIGIDO] CRÍTICO 1 — atualiza token no loop principal
    global _HUBSPOT_ACCESS_TOKEN
    if isinstance(result, dict) and result.get("new_access_token"):
        nt = str(result["new_access_token"])
        _HUBSPOT_ACCESS_TOKEN = nt
        return nt
    return current


def _local_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        return datetime.now(ZoneInfo("America/Sao_Paulo"))
    except Exception:
        return datetime.now(timezone.utc)


def _make_contact_full_name(props: dict[str, Any]) -> str:
    first = str(props.get("firstname") or "").strip()
    last = str(props.get("lastname") or "").strip()
    full = " ".join(part for part in [first, last] if part).strip()
    if full:
        return full
    email = str(props.get("email") or "").strip()
    if email:
        return email
    return "Lead sem nome"


def _normalize_text(text: str) -> str:
    return (
        (text or "")
        .lower()
        .replace("ã", "a")
        .replace("á", "a")
        .replace("à", "a")
        .replace("â", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("õ", "o")
        .replace("ú", "u")
        .replace("ç", "c")
    )


_NAME_STOPWORDS = {
    "oi", "ola", "olá", "livia", "tenho", "uma", "um", "reuniao", "reunião", "com", "mas", "voce", "você",
    "nao", "não", "associou", "associar", "ela", "eu", "preciso", "que", "horario", "horário", "seja",
    "lead", "leads", "contato", "contatos", "negocio", "negócio", "negocios", "negócios", "pra", "para",
    "amanha", "amanhã", "hoje", "ontem", "sem", "de", "do", "da", "dos", "das", "e", "o", "a", "os", "as",
    "me", "mim", "tambem", "também", "no", "na", "nos", "nas", "meu", "minha", "seu", "sua", "pedido",
}


def _extract_email(text: str) -> Optional[str]:
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    if not match:
        return None
    return match.group(0).strip().lower()


def _extract_possible_name_phrases(user_text: str) -> list[str]:
    text = _normalize_text(user_text)
    words = re.findall(r"[a-zA-Z]+", text)
    phrases: list[str] = []
    for size in range(4, 1, -1):
        for i in range(0, max(0, len(words) - size + 1)):
            chunk = words[i : i + size]
            if any(w in _NAME_STOPWORDS for w in chunk):
                continue
            phrase = " ".join(chunk).strip()
            if len(phrase) >= 8:
                phrases.append(phrase)
    deduped: list[str] = []
    for phrase in phrases:
        if phrase not in deduped:
            deduped.append(phrase)
    return deduped[:10]


def _score_name_match(phrase: str, candidate_text: str) -> int:
    phrase_tokens = [tok for tok in _normalize_text(phrase).split() if tok]
    candidate_norm = _normalize_text(candidate_text)
    candidate_tokens = candidate_norm.split()
    score = 0
    for tok in phrase_tokens:
        if tok in candidate_tokens:
            score += 4
        elif tok in candidate_norm:
            score += 1
    if phrase_tokens and all(tok in candidate_tokens for tok in phrase_tokens):
        score += 8
    elif phrase_tokens and all(tok in candidate_norm for tok in phrase_tokens):
        score += 3
    if candidate_norm.startswith(_normalize_text(phrase)):
        score += 5
    return score


def _extract_day_count(text: str) -> Optional[int]:
    match = re.search(r"(\d+)\s*dias?", _normalize_text(text))
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _load_capability_cache() -> dict[str, Any]:  # [CORRIGIDO] CRÍTICO 3 — lock no carregamento
    global _CAPABILITY_CACHE
    with _CACHE_LOCK:
        if _CAPABILITY_CACHE is not None:
            return _CAPABILITY_CACHE
        try:
            with open(CAPABILITY_CACHE_PATH, "r", encoding="utf-8") as fh:
                _CAPABILITY_CACHE = json.load(fh)
        except Exception:
            _CAPABILITY_CACHE = {"objects": {}, "associations": {}, "updated_at": None}
        return _CAPABILITY_CACHE


def _save_capability_cache() -> None:  # [CORRIGIDO] CRÍTICO 3 — lock na gravação
    with _CACHE_LOCK:
        cache = _load_capability_cache()
        cache["updated_at"] = datetime.now(timezone.utc).isoformat()
        try:
            with open(CAPABILITY_CACHE_PATH, "w", encoding="utf-8") as fh:
                json.dump(cache, fh, ensure_ascii=False, indent=2)
        except Exception:
            pass


def _cache_get(section: str, key: str) -> Any:  # [CORRIGIDO] CRÍTICO 3 — leitura consistente com o lock
    with _CACHE_LOCK:
        cache = _load_capability_cache()
        return (cache.get(section) or {}).get(key)


def _cache_put(section: str, key: str, value: Any) -> None:  # [CORRIGIDO] CRÍTICO 3 — escrita concorrente
    with _CACHE_LOCK:
        cache = _load_capability_cache()
        bucket = cache.setdefault(section, {})
        bucket[key] = value
        _save_capability_cache()


def _memory_bucket() -> dict[str, Any]:
    cache = _load_capability_cache()
    return cache.setdefault(
        "memory",
        {
            "user_preferences": {},
            "recent_requests": [],
            "recent_turns": [],
            "recent_operation_artifacts": [],
            "entity_memory": {},
            "searchable_history": [],
            "successful_patterns": [],
            "failed_patterns": [],
            "execution_memory": {
                "last_intent": None,
                "last_successful_operation": None,
                "last_failed_operation": None,
                "last_entities": {},
                "last_constraints": [],
                "last_owner_email": None,
                "last_owner_id": None,
            },
        },
    )


def _trim_list(items: list[Any], max_items: int) -> list[Any]:
    return items[-max_items:] if len(items) > max_items else items


def _extract_time_hint_days(user_text: str) -> Optional[int]:
    t = _normalize_text(user_text)
    if "semana passada" in t:
        return 14
    if "ontem" in t:
        return 2
    if "hoje" in t:
        return 1
    return _extract_day_count(t)


def _learn_user_preferences(user_text: str) -> None:
    t = _normalize_text(user_text)
    prefs = _memory_bucket().setdefault("user_preferences", {})
    if "sempre da ia" in t or "python conversa com ela" in t or "nao falo nunca com resultado de codigo" in t:
        prefs["response_source"] = "ai_only"
    if "memoria" in t or "memória" in user_text:
        prefs["memory_expected"] = True
    # Preferência explícita: nunca mexer no fluxo determinístico de busca por período
    if "nao alterar o codigo desse ponto" in t or "não alterar o codigo desse ponto" in t or "não alterar o código desse ponto" in t:
        prefs["lock_direct_period_search"] = True
    if "em lote" in t or "volume grande" in t or "alto volume" in t:
        prefs["prefer_batch_operations"] = True
    _save_capability_cache()


def _remember_request(user_text: str) -> None:
    mem = _memory_bucket()
    recent = mem.setdefault("recent_requests", [])
    recent.append({"text": user_text, "timestamp": datetime.now(timezone.utc).isoformat()})
    mem["recent_requests"] = _trim_list(recent, 30)
    _save_capability_cache()


def _remember_turn(user_text: str, assistant_text: str) -> None:
    mem = _memory_bucket()
    turns = mem.setdefault("recent_turns", [])
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user": user_text,
        "assistant": assistant_text,
    }
    turns.append(entry)
    mem["recent_turns"] = _trim_list(turns, 20)
    history = mem.setdefault("searchable_history", [])
    history.append({"kind": "turn", **entry})
    mem["searchable_history"] = _trim_list(history, 300)
    _save_capability_cache()


def _summarize_result_artifacts(plan: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    artifact: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": plan.get("mode"),
        "object_type": plan.get("object_type"),
        "related_object_type": plan.get("related_object_type"),
    }
    for key in [
        "created_samples",
        "created_meetings",
        "created_tasks",
        "created_deals",
        "repaired_samples",
        "repaired_associations",
        "meeting_template",
        "task_template",
        "create_properties",
    ]:
        if key in result and result.get(key):
            artifact[key] = result.get(key)
    if result.get("result"):
        nested = result.get("result") or {}
        for key in [
            "created_samples",
            "created_meetings",
            "created_tasks",
            "created_deals",
            "repaired_samples",
            "repaired_associations",
            "meeting_template",
            "task_template",
            "create_properties",
        ]:
            if key in nested and nested.get(key):
                artifact[key] = nested.get(key)
    return artifact


def _extract_entities_from_artifact(artifact: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    entities: dict[str, list[dict[str, Any]]] = {}
    for sample_key, entity_key, id_field in [
        ("created_samples", None, None),
        ("repaired_samples", None, None),
    ]:
        for item in artifact.get(sample_key) or []:
            if not isinstance(item, dict):
                continue
            for key, value in item.items():
                if key.endswith("_id") and value:
                    bucket = entities.setdefault(key, [])
                    bucket.append({"id": str(value), "source": sample_key, "item": item})
    for count_key, singular in [
        ("created_meetings", "meeting"),
        ("created_tasks", "task"),
        ("created_deals", "deal"),
        ("repaired_associations", "association"),
    ]:
        if artifact.get(count_key):
            entities.setdefault(f"last_{singular}_event", []).append({"value": artifact.get(count_key), "source": count_key})
    return entities


def _remember_operation_artifacts(plan: dict[str, Any], result: dict[str, Any]) -> None:
    mem = _memory_bucket()
    artifact = _summarize_result_artifacts(plan, result)
    artifacts = mem.setdefault("recent_operation_artifacts", [])
    artifacts.append(artifact)
    mem["recent_operation_artifacts"] = _trim_list(artifacts, 20)
    entity_memory = mem.setdefault("entity_memory", {})
    extracted = _extract_entities_from_artifact(artifact)
    for key, rows in extracted.items():
        current = entity_memory.setdefault(key, [])
        current.extend(rows)
        entity_memory[key] = _trim_list(current, 20)
    history = mem.setdefault("searchable_history", [])
    history.append({"kind": "artifact", **artifact})
    mem["searchable_history"] = _trim_list(history, 300)
    _save_capability_cache()


def _remember_execution_outcome(user_text: str, plan: dict[str, Any], result: dict[str, Any], success: bool) -> None:
    mem = _memory_bucket()
    bucket_name = "successful_patterns" if success else "failed_patterns"
    bucket = mem.setdefault(bucket_name, [])
    bucket.append(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request": user_text,
            "mode": plan.get("mode"),
            "object_type": plan.get("object_type"),
            "related_object_type": plan.get("related_object_type"),
            "reason": plan.get("reason"),
            "summary": {
                "ok": bool(result.get("ok")),
                "message": result.get("message"),
                "errors": result.get("errors"),
                "status_code": result.get("status_code"),
            },
        }
    )
    mem[bucket_name] = _trim_list(bucket, 40)
    history = mem.setdefault("searchable_history", [])
    history.append({"kind": "pattern", "success": success, **bucket[-1]})
    mem["searchable_history"] = _trim_list(history, 300)
    execution_memory = mem.setdefault("execution_memory", {})
    execution_memory["last_intent"] = {
        "request": user_text,
        "mode": plan.get("mode"),
        "object_type": plan.get("object_type"),
        "related_object_type": plan.get("related_object_type"),
        "success": success,
    }
    key = "last_successful_operation" if success else "last_failed_operation"
    execution_memory[key] = bucket[-1]
    _save_capability_cache()


def _remember_structured_execution(
    user_text: str,
    *,
    operation_name: str,
    operation_result: dict[str, Any],
    mode: str,
    object_type: Optional[str] = None,
    related_object_type: Optional[str] = None,
    constraints: Optional[list[str]] = None,
) -> None:
    pseudo_plan = {
        "mode": mode,
        "object_type": object_type,
        "related_object_type": related_object_type,
        "reason": operation_name,
    }
    _remember_operation_artifacts(pseudo_plan, operation_result)
    success = bool(operation_result.get("ok", True)) and not operation_result.get("errors")
    _remember_execution_outcome(user_text, pseudo_plan, operation_result, success=success)
    mem = _memory_bucket()
    execution_memory = mem.setdefault("execution_memory", {})
    extracted = _extract_entities_from_artifact(_summarize_result_artifacts(pseudo_plan, operation_result))
    if extracted:
        execution_memory["last_entities"] = extracted
    if constraints:
        execution_memory["last_constraints"] = _trim_list((execution_memory.get("last_constraints") or []) + constraints, 20)
    owner_email = operation_result.get("owner_email")
    owner_id = operation_result.get("owner_id")
    if owner_email:
        execution_memory["last_owner_email"] = owner_email
    if owner_id:
        execution_memory["last_owner_id"] = owner_id
    _save_capability_cache()


def _build_structured_memory_payload(user_text: str) -> dict[str, Any]:
    mem = _memory_bucket()
    memory_search = _search_memory(user_text)
    return {
        "preferences": mem.get("user_preferences") or {},
        "recent_requests": (mem.get("recent_requests") or [])[-5:],
        "recent_turns": (mem.get("recent_turns") or [])[-4:],
        "recent_operation_artifacts": (mem.get("recent_operation_artifacts") or [])[-4:],
        "execution_memory": mem.get("execution_memory") or {},
        "recent_success_patterns": (mem.get("successful_patterns") or [])[-4:],
        "recent_failed_patterns": (mem.get("failed_patterns") or [])[-4:],
        "relevant_memory_matches": memory_search.get("matches") or [],
    }


def _recent_entity_ids_for_object(object_type: str) -> list[str]:
    artifacts = (_memory_bucket().get("recent_operation_artifacts") or [])[-8:]
    found: list[str] = []
    singular = object_type[:-1] if object_type.endswith("s") else object_type
    expected_key = f"{singular}_id"
    for artifact in reversed(artifacts):
        for sample in artifact.get("created_samples") or []:
            if not isinstance(sample, dict):
                continue
            if sample.get("created_object_type") == object_type and sample.get("created_id"):
                found.append(str(sample.get("created_id")))
            if sample.get("target_object_type") == object_type and sample.get("target_id"):
                found.append(str(sample.get("target_id")))
            if sample.get(expected_key):
                found.append(str(sample.get(expected_key)))
        for sample in artifact.get("repaired_samples") or []:
            if not isinstance(sample, dict):
                continue
            if sample.get("from_id") and object_type == artifact.get("object_type"):
                found.append(str(sample.get("from_id")))
            if sample.get("to_id") and object_type == artifact.get("related_object_type"):
                found.append(str(sample.get("to_id")))
    deduped: list[str] = []
    for item in found:
        if item and item not in deduped:
            deduped.append(item)
    return deduped[:20]


def _search_memory(user_text: str) -> dict[str, Any]:
    mem = _memory_bucket()
    history = mem.get("searchable_history") or []
    query = _normalize_text(user_text)
    tokens = [tok for tok in re.split(r"\s+", query) if tok and len(tok) > 2]
    days_hint = _extract_time_hint_days(user_text)
    cutoff = None
    if days_hint:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_hint)
    scored: list[tuple[int, dict[str, Any]]] = []
    for item in history:
        ts = str(item.get("timestamp") or "")
        if cutoff:
            dt = _parse_hubspot_dt(ts)
            if dt is None or dt < cutoff:
                continue
        text_blob = _normalize_text(json.dumps(item, ensure_ascii=False))
        score = sum(1 for tok in tokens if tok in text_blob)
        if score > 0:
            scored.append((score, item))
    scored.sort(key=lambda x: x[0], reverse=True)
    return {
        "query": user_text,
        "time_hint_days": days_hint,
        "matches": [item for _, item in scored[:15]],
        "entity_memory": mem.get("entity_memory") or {},
    }


def _build_memory_context(user_text: str) -> str:
    payload = _build_structured_memory_payload(user_text)
    lines = ["MEMORIA OPERACIONAL DA LIVIA:"]
    if payload.get("preferences"):
        lines.append(f"- preferencias_usuario={json.dumps(payload.get('preferences') or {}, ensure_ascii=False)}")
    if payload.get("execution_memory"):
        lines.append(f"- memoria_execucao={json.dumps(payload.get('execution_memory') or {}, ensure_ascii=False)}")
    if payload.get("recent_operation_artifacts"):
        lines.append(f"- artefatos_operacionais_recentes={json.dumps(payload.get('recent_operation_artifacts') or [], ensure_ascii=False)}")
    if payload.get("recent_failed_patterns"):
        lines.append(f"- falhas_recentes_semelhantes={json.dumps(payload.get('recent_failed_patterns') or [], ensure_ascii=False)}")
    if payload.get("recent_success_patterns"):
        lines.append(f"- padroes_recentes_bem_sucedidos={json.dumps(payload.get('recent_success_patterns') or [], ensure_ascii=False)}")
    if payload.get("relevant_memory_matches"):
        lines.append(f"- memoria_relevante_encontrada={json.dumps(payload.get('relevant_memory_matches') or [], ensure_ascii=False)}")
    if payload.get("recent_turns"):
        lines.append(f"- ultimos_turnos={json.dumps(payload.get('recent_turns') or [], ensure_ascii=False)}")
    lines.append(f"- pedido_atual={user_text}")
    return "\n".join(lines)


def _object_config(object_type: str) -> dict[str, Any]:
    return HUBSPOT_OBJECT_CONFIGS.get(object_type, {"date_property": "createdate", "default_properties": [], "supports_pipelines": False})


def _mentioned_objects(user_text: str) -> list[str]:
    t = _normalize_text(user_text)
    mapping = {
        "contacts": ["contato", "contatos", "contact", "contacts"],
        "leads": ["lead", "leads"],
        "deals": ["negocio", "negocios", "deal", "deals"],
        "tasks": ["tarefa", "tarefas", "task", "tasks"],
        "meetings": ["reuniao", "reunioes", "reunião", "meeting", "meetings"],
    }
    found: list[str] = []
    for obj, needles in mapping.items():
        if any(n in t for n in needles):
            found.append(obj)
    return found or ["contacts", "leads", "deals"]


def _matches_large_volume_lead_analysis_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    if "lead" not in t:
        return False
    if _contains_explicit_action_request(t):
        return False
    return any(word in t for word in ["analise", "analisar", "analisa", "diagnostique", "diagnosticar", "resuma", "resumir"])


def _matches_deal_operational_analysis_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    if not any(word in t for word in ["negocio", "negocios", "deal", "deals"]):
        return False
    if _contains_explicit_action_request(t):
        return False
    analysis_words = ["analise", "analisar", "analisa", "diagnostique", "diagnosticar", "resuma", "resumir"]
    issue_words = ["sem tarefa", "sem reuniao", "sem reunião", "gargalo", "parado", "pipeline", "stage", "etapa"]
    return any(word in t for word in analysis_words) or any(word in t for word in issue_words)


def _matches_task_operational_analysis_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    if not any(word in t for word in ["tarefa", "tarefas", "task", "tasks"]):
        return False
    if _contains_explicit_action_request(t):
        return False
    return any(word in t for word in ["vencida", "vencidas", "atrasada", "atrasadas", "owner", "proprietario", "analise", "analisar", "resuma"])


def _matches_meeting_operational_analysis_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    if not any(word in t for word in ["reuniao", "reunião", "meeting", "meetings"]):
        return False
    if _contains_explicit_action_request(t):
        return False
    return any(word in t for word in ["sem", "por owner", "por proprietario", "owner", "analise", "analisar", "resuma", "agenda"])


_ACTION_REQUEST_HINTS = [
    "altere", "alterar", "mude", "mudar", "associe", "associar", "vincule", "vincular",
    "adicione", "adicionar", "crie", "criar", "gere", "gerar", "ponha", "coloque",
    "atualize", "atualizar", "agende", "agendar", "atribua", "atribuir", "movimente",
    "mova", "marque", "registrar", "registre", "preencha", "preencher",
]


def _contains_explicit_action_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    return any(word in t for word in _ACTION_REQUEST_HINTS)


def _split_operational_request(user_text: str) -> list[str]:  # [CORRIGIDO] AVISO 5 — vírgula em nomes próprios
    text = re.sub(r"\b(veja tambem|veja também|alem disso|além disso|depois disso|tambem|também)\b", "|", user_text, flags=re.IGNORECASE)
    text = re.sub(r"[;\n]+", "|", text)
    text = re.sub(
        r",\s*(?=(associe|associar|crie|criar|adicione|adicionar|altere|alterar|mude|mudar|coloque|ponha|agende|agendar|mov[a-z]+)\b)",
        "|",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\s+e\s+(?=(associe|associar|crie|criar|adicione|adicionar|altere|alterar|mude|mudar|coloque|ponha|agende|agendar|mov[a-z]+)\b)",
        "|",
        text,
        flags=re.IGNORECASE,
    )
    parts = [part.strip(" .:-") for part in text.split("|")]
    cleaned: list[str] = []
    object_context = ""
    for part in parts:
        if len(part) < 8:
            continue
        norm = _normalize_text(part)
        has_object = any(token in norm for token in ["reuniao", "reunião", "meeting", "lead", "contato", "contact", "negocio", "deal"])
        has_action = _contains_explicit_action_request(norm)
        if has_object and not has_action:
            object_context = part
            continue
        if has_action and not has_object and object_context:
            cleaned.append(f"{object_context}. {part}")
            object_context = ""
            continue
        if has_object and has_action:
            cleaned.append(part)
            object_context = part
    deduped: list[str] = []
    for part in cleaned:
        if part not in deduped:
            deduped.append(part)
    return deduped[:6]


def _build_period_range_from_text(user_text: str) -> tuple[int, int, str]:
    now = _local_now()
    text = _normalize_text(user_text)
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_yesterday = start_today - timedelta(days=1)
    start_of_week = start_today - timedelta(days=start_today.weekday())

    if "ontem" in text and "hoje" in text:
        start = start_yesterday
        end = now
        label = "ontem e hoje"
    elif "ontem" in text:
        start = start_yesterday
        end = start_today
        label = "ontem"
    elif "hoje" in text:
        start = start_today
        end = now
        label = "hoje"
    elif "essa semana" in text or "esta semana" in text:
        start = start_of_week
        end = now
        label = "esta semana"
    elif "ultima semana" in text or "ultimos 7 dias" in text or "ultimos sete dias" in text:
        start = now - timedelta(days=7)
        end = now
        label = "ultimos 7 dias"
    elif "este mes" in text or "esse mes" in text:
        start = start_today.replace(day=1)
        end = now
        label = "este mes"
    else:
        start = now - timedelta(days=30)
        end = now
        label = "ultimos 30 dias"

    return (
        int(start.astimezone(timezone.utc).timestamp() * 1000),
        int(end.astimezone(timezone.utc).timestamp() * 1000),
        label,
    )


def _parse_explicit_date_range(user_text: str) -> Optional[tuple[int, int, str]]:
    """
    Parseia datas explícitas como:
    - "13/04/2024"
    - "13-04-2024"
    - "de 01/04/2024 a 13/04/2024"
    Retorna (start_ms, end_ms, label) em epoch ms UTC, usando o dia inteiro no fuso local.
    """
    raw = user_text or ""
    # aceita / ou -
    matches = re.findall(r"\b(\d{2})[/-](\d{2})[/-](\d{4})\b", raw)
    if not matches:
        return None

    def _to_local_dt(d: str, m: str, y: str) -> datetime:
        day = int(d)
        month = int(m)
        year = int(y)
        # usa o fuso local padrão da aplicação
        base = _local_now().tzinfo or timezone.utc
        return datetime(year, month, day, 0, 0, 0, 0, tzinfo=base)

    # normaliza: 1 data => dia inteiro; 2+ datas => intervalo [primeira, última]
    start_dt = _to_local_dt(*matches[0])
    end_dt = _to_local_dt(*matches[-1]).replace(hour=23, minute=59, second=59, microsecond=999000)

    start_ms = int(start_dt.astimezone(timezone.utc).timestamp() * 1000)
    end_ms = int(end_dt.astimezone(timezone.utc).timestamp() * 1000)
    if len(matches) == 1:
        label = f"{matches[0][0]}/{matches[0][1]}/{matches[0][2]}"
    else:
        label = f"{matches[0][0]}/{matches[0][1]}/{matches[0][2]} a {matches[-1][0]}/{matches[-1][1]}/{matches[-1][2]}"
    return start_ms, end_ms, label


def _matches_contact_owner_assignment_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    has_contact = "contato" in t or "contatos" in t
    has_owner = "proprietario" in t or "owner" in t
    wants_assign = any(word in t for word in ["adicione", "adicionar", "coloque", "defina", "atribua", "atribuir"])
    missing_owner = ("sem proprietario" in t) or ("sem owner" in t)
    return has_contact and has_owner and wants_assign and missing_owner


def _matches_contact_note_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    if "contato" not in t and "contatos" not in t:
        return False
    note_kw = any(
        k in t
        for k in [
            "observacao",
            "observação",
            "nota",
            "anotacao",
            "anotação",
            "comentario",
            "comentário",
        ]
    )
    action_kw = any(
        k in t
        for k in [
            "adicione",
            "adicionar",
            "anote",
            "anotar",
            "grave",
            "gravar",
            "coloque",
            "colocar",
            "escreva",
            "escrever",
            "deixe",
            "deixar",
            "registre",
            "registrar",
        ]
    )
    return note_kw and action_kw


def _extract_contact_name_between_contato_and_que(user_text: str) -> Optional[str]:
    m = re.search(r"(?i)\bcontato\s+(.+?)\s+\bque\b\s+", user_text or "")
    if not m:
        return None
    name = (m.group(1) or "").strip()
    name = re.sub(r"(?i)^(o|a|os|as)\s+", "", name).strip()
    return name or None


def _extract_note_body_for_contact_request(user_text: str) -> str:
    raw = (user_text or "").strip()
    m = re.search(r"(?i)\bque\s+(.+)$", raw)
    if m:
        return m.group(1).strip().strip('"').strip("'")
    m2 = re.search(r"(?i)\b(dizendo|informando|constando)\s+que\s+(.+)$", raw)
    if m2:
        return m2.group(2).strip().strip('"').strip("'")
    if ":" in raw:
        tail = raw.split(":", 1)[1].strip()
        if len(tail) >= 3:
            return tail
    return ""


def _html_escape_simple(text: str) -> str:
    return (
        (text or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _get_note_to_contact_association_type_id(access_token: str) -> int:
    cached = _cache_get("associations", "note_to_contact_type_id")
    if cached is not None:
        try:
            return int(cached)
        except Exception:
            pass
    candidates_eps = [
        "/crm/v4/associations/notes/contacts/labels",
        "/crm/associations/v4/notes/contacts/labels",
        "/crm/associations/2026-03/notes/contacts/labels",
    ]
    for endpoint in candidates_eps:
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="GET")
        if not res.get("ok"):
            continue
        data = res.get("response_json") or {}
        rows = data.get("results") or []
        preferred: list[tuple[int, str]] = []
        fallback: list[tuple[int, str]] = []
        for item in rows:
            row = item or {}
            type_id = row.get("typeId", row.get("id"))
            if type_id is None:
                continue
            name = str(row.get("name") or "").strip().lower()
            label = str(row.get("label") or "").strip().lower()
            category = str(row.get("category") or "").strip().upper()
            blob = f"{name} {label}"
            tid = int(type_id)
            if "note_to_contact" in name or "note_to_contact" in label or "note" in name and "contact" in name:
                preferred.append((tid, blob))
            if category == "HUBSPOT_DEFINED":
                fallback.append((tid, blob))
        if preferred:
            tid = preferred[0][0]
            _cache_put("associations", "note_to_contact_type_id", tid)
            return tid
        if fallback:
            tid = fallback[0][0]
            _cache_put("associations", "note_to_contact_type_id", tid)
            return tid
    raise RuntimeError("Nao consegui descobrir associationTypeId para associar notes->contacts.")


def _execute_add_contact_note(access_token: str, user_text: str) -> dict[str, Any]:
    note_body = _extract_note_body_for_contact_request(user_text)
    if len(note_body.strip()) < 2:
        return {
            "ok": False,
            "deterministic_response": (
                "Nao consegui extrair o texto da observacao. "
                "Reescreva no formato: '... no contato NOME COMPLETO que TEXTO DA OBSERVACAO'."
            ),
        }

    email = _extract_email(user_text)
    if email:
        cid = _lookup_related_record_by_email(access_token, "contacts", email)
        if cid:
            tool_res = hubspot_create_note_for_contact_tool(access_token, contact_id=cid, note_body=note_body)
            if not tool_res.get("ok"):
                err = tool_res.get("error") or tool_res.get("response_text") or "erro desconhecido"
                return {
                    "ok": False,
                    "deterministic_response": f"Encontrei o contato pelo email ({email}), mas falhei ao criar a nota: {str(err)[:1500]}",
                    **{k: v for k, v in tool_res.items() if k != "ok"},
                }
            note_id = tool_res.get("note_id")
            det = (
                f"Pronto: criei uma nota no contato com email **{email}** (ID {cid}).\n"
                f"ID da nota: {note_id}\n"
                f"Texto: {note_body}"
            )
            return {
                "ok": True,
                "deterministic_response": det,
                "contact_id": cid,
                "contact_name": email,
                "note_id": note_id,
                "association_type_id": tool_res.get("association_type_id"),
            }

    explicit = _extract_contact_name_between_contato_and_que(user_text)
    phrase_candidates: list[str] = []
    if explicit:
        phrase_candidates.append(explicit)
    for phrase in _extract_possible_name_phrases(user_text):
        if phrase not in phrase_candidates:
            phrase_candidates.append(phrase)

    scored_rows: list[tuple[int, dict[str, Any], str]] = []
    for phrase in phrase_candidates[:8]:
        for row in _search_name_candidates(access_token, "contacts", phrase, limit=12):
            scored_rows.append((int(row.get("score") or 0), row, phrase))
    scored_rows.sort(key=lambda x: x[0], reverse=True)
    if not scored_rows:
        return {
            "ok": False,
            "deterministic_response": "Nao encontrei nenhum contato no HubSpot com base no nome informado. Confirme o nome exatamente como esta no CRM (ou envie o email).",
        }

    top_score = int(scored_rows[0][0])
    top_matches = [(r, ph) for sc, r, ph in scored_rows if int(sc) == top_score][:5]
    if top_score < 5:
        preview = [{"id": r.get("id"), "name": r.get("name"), "email": r.get("email"), "score": r.get("score")} for r, _ in top_matches]
        return {
            "ok": False,
            "deterministic_response": (
                "Encontrei candidatos, mas o nome nao bateu com confianca suficiente. "
                f"Candidatos: {json.dumps(preview, ensure_ascii=False)}. "
                "Envie o email do contato ou o ID do HubSpot."
            ),
            "candidates": preview,
        }
    if len(top_matches) >= 2:
        ids = {str(a[0].get("id")) for a in top_matches}
        if len(ids) > 1:
            preview = [{"id": r.get("id"), "name": r.get("name"), "email": r.get("email")} for r, _ in top_matches]
            return {
                "ok": False,
                "deterministic_response": (
                    "Ha mais de um contato possivel para esse nome. "
                    f"Escolha um ID: {json.dumps(preview, ensure_ascii=False)}"
                ),
                "candidates": preview,
            }

    best_row = top_matches[0][0]
    contact_id = str(best_row.get("id") or "").strip()
    contact_name = str(best_row.get("name") or "").strip()
    if not contact_id:
        return {"ok": False, "deterministic_response": "Resolucao do contato falhou (id vazio)."}

    tool_res = hubspot_create_note_for_contact_tool(access_token, contact_id=contact_id, note_body=note_body)
    if not tool_res.get("ok"):
        err = tool_res.get("error") or tool_res.get("response_text") or "erro desconhecido"
        return {
            "ok": False,
            "deterministic_response": f"Nao consegui criar a nota no HubSpot: {str(err)[:1500]}",
            **{k: v for k, v in tool_res.items() if k != "ok"},
        }
    note_id = tool_res.get("note_id")
    det = (
        f"Pronto: criei uma nota no registro do contato **{contact_name}** (ID {contact_id}).\n"
        f"ID da nota: {note_id}\n"
        f"Texto: {note_body}"
    )
    return {
        "ok": True,
        "deterministic_response": det,
        "contact_id": contact_id,
        "contact_name": contact_name,
        "note_id": note_id,
        "association_type_id": tool_res.get("association_type_id"),
    }


def _matches_stale_leads_without_contact_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    return "lead" in t and "sem contato" in t and _extract_day_count(t) is not None


def _extract_requested_lead_stage_label(user_text: str) -> Optional[str]:
    t = _normalize_text(user_text)
    if "novo lead" in t or "novo_lead" in t:
        return "novo lead"
    if "novo" in t and "lead" in t:
        return "novo lead"
    return None


def _execute_stale_leads_without_contact(access_token: str, user_text: str) -> dict[str, Any]:
    day_count = _extract_day_count(user_text or "")
    if day_count is None:
        day_count = 5
    cutoff = _local_now().astimezone(timezone.utc) - timedelta(days=int(day_count))
    cutoff_ms = int(cutoff.timestamp() * 1000)

    cfg = _object_config("leads")
    created_prop = str(cfg.get("date_property") or "hs_createdate")
    stage_label = _extract_requested_lead_stage_label(user_text)
    stage_props = _resolve_lead_stage_properties(access_token, stage_label) if stage_label else None
    stage_id = (stage_props or {}).get("hs_pipeline_stage")
    pipeline_id = (stage_props or {}).get("hs_pipeline")

    filters: list[dict[str, Any]] = []
    if stage_id:
        filters.append({"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": str(stage_id)})
    if pipeline_id:
        filters.append({"propertyName": "hs_pipeline", "operator": "EQ", "value": str(pipeline_id)})

    props_for_search = list(
        dict.fromkeys(
            (cfg.get("default_properties") or [])
            + [
                created_prop,
                "hs_lead_name",
                "hs_pipeline_stage",
                "hs_pipeline",
                "hs_primary_contact_id",
                "hs_associated_contact_email",
                "hs_associated_contact_firstname",
                "hs_associated_contact_lastname",
                "hs_last_engagement_timestamp",
                "hs_last_activity_date",
            ]
        )
    )

    leads, incomplete_due_to_cap = _search_objects(
        access_token,
        object_type="leads",
        properties=props_for_search,
        filter_groups=[{"filters": filters}] if filters else [],
        sorts=[{"propertyName": created_prop, "direction": "DESCENDING"}],
        per_page=200,
        max_records=10000,
        with_meta=True,
    )

    missing_primary_any_age: list[dict[str, Any]] = []
    missing_primary_stale: list[dict[str, Any]] = []
    assoc_disagreements = 0
    lead_ids = [str((lead or {}).get("id")) for lead in leads if (lead or {}).get("id") is not None]
    assoc = _batch_read_associations(access_token, from_object_type="leads", to_object_type="contacts", record_ids=lead_ids) if lead_ids else {}

    for lead in leads:
        lead_id = str((lead or {}).get("id") or "")
        if not lead_id:
            continue
        props = (lead or {}).get("properties") or {}
        if not _lead_missing_primary_contact(props):
            continue

        assoc_ids = assoc.get(lead_id, []) if assoc else []
        if assoc_ids:
            assoc_disagreements += 1

        created_ms = _parse_hubspot_epoch_ms(props.get(created_prop))
        eng_ms = _parse_hubspot_epoch_ms(props.get("hs_last_engagement_timestamp")) or _parse_hubspot_epoch_ms(
            props.get("hs_last_activity_date")
        )
        staleness_ms = None
        for candidate in (eng_ms, created_ms):
            if candidate is None:
                continue
            staleness_ms = candidate if staleness_ms is None else max(int(staleness_ms), int(candidate))

        name = _safe_prop(props, "hs_lead_name", default=f"Lead {lead_id}")
        created_at = _parse_hubspot_dt(str(props.get(created_prop) or ""))
        stage = _safe_prop(props, "hs_pipeline_stage")
        pipe = _safe_prop(props, "hs_pipeline")
        row = {
            "id": lead_id,
            "name": name,
            "pipeline": pipe,
            "stage": stage,
            "created_at": created_at.isoformat() if created_at else "",
            "staleness_basis_ms": staleness_ms,
            "has_lead_to_contact_associations": bool(assoc_ids),
        }
        missing_primary_any_age.append(row)
        if staleness_ms is None or int(staleness_ms) < int(cutoff_ms):
            missing_primary_stale.append(row)

    definition_lines = [
        "Definicoes usadas nesta verificacao:",
        "- 'Sem contato' aqui significa **sem contato primario no Lead** (campo `hs_primary_contact_id` vazio + sinais espelhados `hs_associated_contact_*` vazios).",
        f"- 'Ha mais de {int(day_count)} dias' aqui significa: **max(`hs_last_engagement_timestamp` ou `hs_last_activity_date`, `{created_prop}`) < cutoff** (UTC).",
        "Se voce quis dizer outra metrica (ex.: apenas idade do lead por criacao), diga qual campo do HubSpot deve prevalecer.",
    ]
    if assoc_disagreements:
        definition_lines.append(
            f"- Aviso: encontrei {assoc_disagreements} caso(s) onde existe associacao Lead->Contato via API de associacoes, "
            "mas o campo `hs_primary_contact_id` ainda aparece vazio. Eu mantive esses registros na lista por aderir ao board/campo primario."
        )

    cap_note = ""
    if incomplete_due_to_cap:
        cap_note = (
            f"\n\nAviso: a busca parou no limite configurado ({len(leads)} leads retornados). "
            "Se o funil for maior que isso, o numero abaixo pode ser subcontagem."
        )

    summary_lines = [
        f"Janela pedida: ultimos {int(day_count)} dias (cutoff UTC: {cutoff.isoformat()}).",
        f"Leads retornados pela busca (com os filtros de etapa/funil aplicados): {len(leads)}.",
        f"Sem contato primario (qualquer idade): {len(missing_primary_any_age)}.",
        f"Sem contato primario e 'atrasados' > {int(day_count)} dias (pela regra acima): {len(missing_primary_stale)}.",
    ]

    sample_lines = [f"- {row.get('name')} (lead {row.get('id')})" for row in missing_primary_stale[:25]]
    deterministic_response = (
        "\n".join(summary_lines)
        + "\n\n"
        + "\n".join(definition_lines)
        + "\n\n"
        + ("Amostra (ate 25):\n" + "\n".join(sample_lines) if sample_lines else "Amostra: (nenhum caso na janela pedida)")
        + cap_note
    )

    return {
        "ok": True,
        "deterministic_response": deterministic_response,
        "query_days": day_count,
        "cutoff_utc": cutoff.isoformat(),
        "requested_stage_label": stage_label,
        "resolved_stage_id": stage_id,
        "resolved_pipeline_id": pipeline_id,
        "staleness_fields": ["hs_last_engagement_timestamp", "hs_last_activity_date", created_prop],
        "searched_leads": len(leads),
        "incomplete_due_to_cap": bool(incomplete_due_to_cap),
        "missing_primary_contact_any_age": len(missing_primary_any_age),
        "missing_primary_contact_stale_window": len(missing_primary_stale),
        "association_disagreements_primary_empty_but_assoc_exists": assoc_disagreements,
        "samples_stale_window": missing_primary_stale[:25],
    }


def _get_primary_lead_contact_association_type_id(access_token: str) -> int:
    candidates = [
        "/crm/v4/associations/leads/contacts/labels",
        "/crm/associations/2026-03/leads/contacts/labels",
    ]
    for endpoint in candidates:
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="GET")
        if not res.get("ok"):
            continue
        data = res.get("response_json") or {}
        for item in data.get("results") or []:
            row = item or {}
            label = str(row.get("label") or "").strip().lower()
            assoc_id = row.get("typeId", row.get("id"))
            if assoc_id is None:
                continue
            if label == "primary":
                return int(assoc_id)
            if label == "lead with primary contact":
                return int(assoc_id)
    raise RuntimeError("Nao consegui descobrir o associationTypeId de lead_to_primary_contact.")


def _matches_direct_contact_lead_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    has_contacts = "contato" in t
    has_leads = "lead" in t
    has_period = ("hoje" in t) or ("ontem" in t)
    has_action = any(word in t for word in ["crie", "criar", "associe", "associar", "adicione", "gerar", "gere", "pegue", "busque", "procure"])
    has_missing = any(
        phrase in t
        for phrase in [
            "sem lead",
            "nao tenham",
            "não tenham",
            "nao tem lead",
            "não tem lead",
            "nao tem leads associados",
            "não tem leads associados",
            "sem leads associados",
        ]
    )
    has_named_targets = any(tok in t for tok in [" edson", " wagner", " maria", " rodrigues"])
    return has_contacts and has_leads and has_period and has_action and (has_missing or has_named_targets)


def _search_objects(
    access_token: str,
    *,
    object_type: str,
    properties: list[str],
    filter_groups: Optional[list[dict[str, Any]]] = None,
    sorts: Optional[list[str]] = None,
    per_page: int = 100,
    max_records: int = 5000,
    with_meta: bool = False,
) -> Any:
    cfg = _object_config(object_type)
    default_properties = cfg.get("default_properties") or []
    final_properties = properties or default_properties
    endpoint = f"/crm/v3/objects/{object_type}/search"
    body: dict[str, Any] = {
        "filterGroups": filter_groups or [],
        "sorts": sorts or [],
        "properties": final_properties,
        "limit": max(1, min(200, per_page)),
    }
    out: list[dict[str, Any]] = []
    after: Optional[str] = None
    page = 0
    incomplete_due_to_cap = False
    while True:
        page += 1
        if after:
            body["after"] = after
        else:
            body.pop("after", None)
        if page == 1 and os.getenv("HUBSPOT_DEBUG_SEARCH") == "1":
            try:
                _status(f"DEBUG search payload {object_type}: {json.dumps(body, ensure_ascii=False)}")
            except Exception:
                pass
        _status(f"Buscando pagina {page} de {object_type}...")
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="POST", json_payload=body)
        if not res.get("ok"):
            if res.get("status_code") == 400 and page == 1:
                resolved = _resolve_search_payload_with_capabilities(access_token, object_type, body)
                if resolved != body:
                    body = resolved
                    _status(f"Payload de busca de {object_type} ajustado dinamicamente a partir das capabilities.")
                    continue
                # Meetings: Search CRM as vezes falha com sorts; tenta novamente sem ordenação.
                if object_type == "meetings" and (body.get("sorts") or []):
                    body = json.loads(json.dumps(body))
                    body["sorts"] = []
                    _status("Retry: busca de meetings sem sorts apos HTTP 400.")
                    continue
            raise RuntimeError(f"Falha ao buscar {object_type}: HTTP {res.get('status_code')} - {res.get('response_text')}")
        data = res.get("response_json") or {}
        results = data.get("results") or []
        nxt = ((data.get("paging") or {}).get("next") or {}).get("after")
        remaining = max_records - len(out)
        if remaining <= 0:
            incomplete_due_to_cap = bool(nxt)
            break
        if len(results) <= remaining:
            out.extend(results)
            if len(out) >= max_records and nxt:
                incomplete_due_to_cap = True
                break
            if not nxt:
                break
            after = str(nxt)
            continue

        out.extend(results[:remaining])
        incomplete_due_to_cap = True
        break
    if with_meta:
        return out, incomplete_due_to_cap
    return out


def _search_contacts_in_period(access_token: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    return _search_objects(
        access_token,
        object_type="contacts",
        properties=["firstname", "lastname", "email", "phone", "mobilephone", "createdate", "hs_lead_status"],
        filter_groups=[
            {
                "filters": [
                    {
                        "propertyName": "createdate",
                        "operator": "BETWEEN",
                        "value": str(start_ms),
                        "highValue": str(end_ms),
                    }
                ]
            }
        ],
        sorts=["-createdate"],
        per_page=100,
        max_records=5000,
    )


def _extract_requested_contact_tokens(user_text: str) -> list[str]:
    text = _normalize_text(user_text)
    words = [w for w in re.findall(r"[a-zA-Z]+", text) if len(w) >= 4 and w not in _NAME_STOPWORDS]
    deduped: list[str] = []
    for word in words:
        if word not in deduped:
            deduped.append(word)
    return deduped[:20]


def _filter_contacts_by_requested_names(contacts: list[dict[str, Any]], user_text: str) -> list[dict[str, Any]]:
    requested_tokens = _extract_requested_contact_tokens(user_text)
    if not requested_tokens:
        return contacts
    scored: list[tuple[int, dict[str, Any]]] = []
    for contact in contacts:
        props = (contact or {}).get("properties") or {}
        full_name = _make_contact_full_name(props)
        name_tokens = [tok for tok in _normalize_text(full_name).split() if len(tok) >= 4]
        overlap = [tok for tok in requested_tokens if tok in name_tokens]
        score = len(overlap)
        if score > 0:
            if name_tokens and name_tokens[0] in requested_tokens:
                score += 2
            scored.append((score, contact))
    if not scored:
        return contacts
    best_score = max(score for score, _ in scored)
    filtered = [contact for score, contact in scored if score == best_score or score >= 2]
    deduped: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for contact in filtered:
        cid = str((contact or {}).get("id") or "")
        if cid and cid not in seen_ids:
            seen_ids.add(cid)
            deduped.append(contact)
    return deduped or contacts


def _resolve_lead_stage_properties(access_token: str, desired_label: str) -> Optional[dict[str, str]]:
    desired = _normalize_text(desired_label)
    res = hubspot_universal_api_call(access_token, endpoint="/crm/v3/pipelines/leads", method="GET")
    if not res.get("ok"):
        return None
    for pipe in (res.get("response_json") or {}).get("results") or []:
        pipeline_id = str((pipe or {}).get("id") or "")
        for stage in (pipe or {}).get("stages") or []:
            stage_id = str((stage or {}).get("id") or "")
            label = _normalize_text(str((stage or {}).get("label") or ""))
            if desired == label or desired in label or label in desired:
                return {"hs_pipeline": pipeline_id, "hs_pipeline_stage": stage_id}
    return None


def _requested_lead_stage_update(access_token: str, user_text: str) -> Optional[dict[str, str]]:
    t = _normalize_text(user_text)
    if "conectado" in t:
        return _resolve_lead_stage_properties(access_token, "conectado")
    return None


def _batch_read_associations(
    access_token: str,
    *,
    from_object_type: str,
    to_object_type: str,
    record_ids: list[str],
) -> dict[str, list[str]]:
    mapping = {rid: [] for rid in record_ids}
    endpoint = f"/crm/associations/2026-03/{from_object_type}/{to_object_type}/batch/read"
    for idx, chunk in enumerate(_chunked(record_ids, 1000), start=1):
        _status(f"Verificando associacoes {from_object_type}->{to_object_type} no lote {idx}...")
        payload = {"inputs": [{"id": cid} for cid in chunk]}
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="POST", json_payload=payload)
        if not res.get("ok"):
            raise RuntimeError(f"Falha ao ler associacoes: HTTP {res.get('status_code')} - {res.get('response_text')}")
        data = res.get("response_json") or {}
        for item in data.get("results") or []:
            from_obj = item.get("from") or {}
            from_id = str(from_obj.get("id") or "")
            tos = item.get("to") or []
            mapping[from_id] = [str(to.get("toObjectId")) for to in tos if (to or {}).get("toObjectId") is not None]
    return mapping


def _read_contact_lead_associations(access_token: str, contact_ids: list[str]) -> dict[str, list[str]]:
    return _batch_read_associations(
        access_token,
        from_object_type="contacts",
        to_object_type="leads",
        record_ids=contact_ids,
    )


def _top_counts(counter: Counter[str], limit: int = 10) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, count in counter.most_common(limit):
        rows.append({"value": key, "count": count})
    return rows


def _safe_prop(props: dict[str, Any], key: str, default: str = "(vazio)") -> str:
    val = props.get(key)
    if val is None:
        return default
    text = str(val).strip()
    return text if text else default


def _parse_hubspot_dt(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _parse_hubspot_epoch_ms(value: Any) -> Optional[int]:
    """
    HubSpot costuma devolver datetime como string ISO, mas algumas propriedades podem vir como epoch ms (int/str).
    """
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ms = int(value)
        return ms if ms > 0 else None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        ms = int(text)
        return ms if ms > 0 else None
    dt = _parse_hubspot_dt(text)
    if not dt:
        return None
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def _hs_prop_is_present(props: dict[str, Any], key: str) -> bool:
    val = props.get(key)
    if val is None:
        return False
    text = str(val).strip().lower()
    if not text:
        return False
    if text in {"null", "none", "undefined"}:
        return False
    if text in {"0", "false", "no"}:
        return False
    return True


def _lead_missing_primary_contact(props: dict[str, Any]) -> bool:
    """
    "Sem contato" no sentido operacional do board: sem contato primario associado ao Lead.
    Preferimos hs_primary_contact_id (oficial) e usamos campos espelhados como fallback leve.
    """
    if _hs_prop_is_present(props, "hs_primary_contact_id"):
        return False
    if _hs_prop_is_present(props, "hs_associated_contact_email"):
        return False
    if _hs_prop_is_present(props, "hs_associated_contact_firstname") or _hs_prop_is_present(props, "hs_associated_contact_lastname"):
        return False
    return True


def _fetch_object_metadata_snapshot(access_token: str, object_type: str) -> dict[str, Any]:  # [CORRIGIDO] AVISO 4 — TTL do cache
    cached = _cache_get("objects", object_type)
    if cached:
        cached_at_str = cached.get("_cached_at")
        if cached_at_str:
            try:
                cached_at = datetime.fromisoformat(str(cached_at_str))
                age_hours = (datetime.now(timezone.utc) - cached_at).total_seconds() / 3600
                if age_hours < 6:
                    return cached
            except Exception:
                pass
        else:
            return cached
    cfg = _object_config(object_type)
    props_res = hubspot_universal_api_call(access_token, endpoint=f"/crm/v3/properties/{object_type}", method="GET")
    props = []
    prop_rows: list[dict[str, Any]] = []
    if props_res.get("ok"):
        for row in (props_res.get("response_json") or {}).get("results") or []:
            name = (row or {}).get("name")
            if name:
                props.append(str(name))
                prop_rows.append(
                    {
                        "name": str(name),
                        "label": str((row or {}).get("label") or ""),
                        "type": str((row or {}).get("type") or ""),
                        "fieldType": str((row or {}).get("fieldType") or ""),
                    }
                )
    pipelines = []
    if cfg.get("supports_pipelines"):
        pipe_res = hubspot_universal_api_call(access_token, endpoint=f"/crm/v3/pipelines/{object_type}", method="GET")
        if pipe_res.get("ok"):
            for row in (pipe_res.get("response_json") or {}).get("results") or []:
                pipelines.append(
                    {
                        "id": str((row or {}).get("id") or ""),
                        "label": str((row or {}).get("label") or ""),
                        "stage_count": len((row or {}).get("stages") or []),
                    }
                )
    snapshot = {
        "object_type": object_type,
        "date_property": cfg.get("date_property"),
        "property_count": len(props),
        "sample_properties": props[:40],
        "properties": prop_rows[:400],
        "pipelines": pipelines[:20],
        "_cached_at": datetime.now(timezone.utc).isoformat(),
    }
    _cache_put("objects", object_type, snapshot)
    return snapshot


def _discover_association_labels(access_token: str, from_object_type: str, to_object_type: str) -> list[dict[str, Any]]:
    cache_key = f"{from_object_type}->{to_object_type}"
    cached = _cache_get("associations", cache_key)
    if cached:
        return cached
    endpoints = [
        f"/crm/v4/associations/{from_object_type}/{to_object_type}/labels",
        f"/crm/associations/2026-03/{from_object_type}/{to_object_type}/labels",
    ]
    labels: list[dict[str, Any]] = []
    for endpoint in endpoints:
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="GET")
        if not res.get("ok"):
            continue
        for row in (res.get("response_json") or {}).get("results") or []:
            labels.append(
                {
                    "typeId": (row or {}).get("typeId", (row or {}).get("id")),
                    "label": str((row or {}).get("label") or ""),
                    "category": str((row or {}).get("category") or ""),
                }
            )
        if labels:
            break
    _cache_put("associations", cache_key, labels)
    return labels


def _resolve_property_name(access_token: str, object_type: str, aliases: list[str]) -> Optional[str]:
    snapshot = _fetch_object_metadata_snapshot(access_token, object_type)
    rows = snapshot.get("properties") or []
    normalized_aliases = [_normalize_text(a) for a in aliases]
    for alias in normalized_aliases:
        for row in rows:
            name = _normalize_text(str((row or {}).get("name") or ""))
            label = _normalize_text(str((row or {}).get("label") or ""))
            if alias == name or alias == label:
                return str((row or {}).get("name") or "")
    for alias in normalized_aliases:
        for row in rows:
            name = _normalize_text(str((row or {}).get("name") or ""))
            label = _normalize_text(str((row or {}).get("label") or ""))
            if alias in name or alias in label:
                return str((row or {}).get("name") or "")
    return None


def _resolve_best_date_property(access_token: str, object_type: str) -> str:
    snapshot = _fetch_object_metadata_snapshot(access_token, object_type)
    candidates = [
        snapshot.get("date_property"),
        _resolve_property_name(access_token, object_type, ["createdate", "data de criacao", "creation date", "hs_createdate"]),
        _resolve_property_name(access_token, object_type, ["hs_timestamp", "activity date", "data da atividade"]),
    ]
    for candidate in candidates:
        if candidate:
            return str(candidate)
    return "createdate"


def _resolve_search_payload_with_capabilities(access_token: str, object_type: str, body: dict[str, Any]) -> dict[str, Any]:
    patched = json.loads(json.dumps(body))
    best_date_prop = _resolve_best_date_property(access_token, object_type)

    # [CORRIGIDO] HTTP 400 — normaliza sorts: sempre lista de strings (ex.: ["createdate"] ou ["-createdate"])
    raw_sorts = patched.get("sorts") or []
    normalized_sorts: list[str] = []
    for sort_item in raw_sorts:
        if isinstance(sort_item, str):
            normalized_sorts.append(sort_item)
        elif isinstance(sort_item, dict) and sort_item.get("propertyName"):
            prop = str(sort_item.get("propertyName") or "").strip()
            if not prop:
                continue
            direction = str(sort_item.get("direction") or "").upper().strip()
            normalized_sorts.append(f"-{prop}" if direction == "DESCENDING" else prop)
    patched["sorts"] = [s for s in normalized_sorts if isinstance(s, str) and s.strip()]

    # [CORRIGIDO] HTTP 400 — normaliza filterGroups: envolve filtros soltos sem wrapper de grupo
    raw_groups = patched.get("filterGroups") or []
    fixed_groups = []
    for group in raw_groups:
        if isinstance(group, dict) and "filters" in group:
            fixed_groups.append(group)
        elif isinstance(group, dict) and "propertyName" in group:
            fixed_groups.append({"filters": [group]})
    patched["filterGroups"] = fixed_groups

    fixed_sorts: list[str] = []
    for sort in patched.get("sorts") or []:
        if not isinstance(sort, str):
            continue
        raw = sort.strip()
        if not raw:
            continue
        desc = raw.startswith("-")
        prop = raw[1:] if desc else raw
        if prop in {"createdate", "hs_createdate", "hs_timestamp", "hs_meeting_start_time"}:
            prop = best_date_prop
        fixed_sorts.append(f"-{prop}" if desc else prop)
    patched["sorts"] = fixed_sorts
    for group in patched.get("filterGroups") or []:
        for flt in (group or {}).get("filters") or []:
            property_name = str((flt or {}).get("propertyName") or "")
            if property_name in {"createdate", "hs_createdate", "hs_timestamp", "hs_meeting_start_time"}:
                flt["propertyName"] = best_date_prop
            if property_name and property_name not in {row.get("name") for row in (_fetch_object_metadata_snapshot(access_token, object_type).get("properties") or [])}:
                resolved = _resolve_property_name(access_token, object_type, [property_name])
                if resolved:
                    flt["propertyName"] = resolved
    return patched


def _resolve_relative_time_token(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip().upper()
    now_local = _local_now()
    now_utc = now_local.astimezone(timezone.utc)

    # [CORRIGIDO] tokens de limite de dia — meia-noite local correta (nunca ponto flutuante no dia)
    def _day_start_ms(days_ago: int) -> str:
        d = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_ago)
        return str(int(d.astimezone(timezone.utc).timestamp() * 1000))

    def _day_end_ms(days_ago: int) -> str:
        d = now_local.replace(hour=23, minute=59, second=59, microsecond=999000) - timedelta(days=days_ago)
        return str(int(d.astimezone(timezone.utc).timestamp() * 1000))

    if text in ("YESTERDAY_START", "ONTEM_START"):
        return _day_start_ms(1)
    if text in ("YESTERDAY_END", "ONTEM_END"):
        return _day_end_ms(1)
    if text in ("TODAY_START", "HOJE_START"):
        return _day_start_ms(0)
    if text in ("TODAY_END", "HOJE_END"):
        return _day_end_ms(0)

    if text == "NOW":
        return str(int(now_utc.timestamp() * 1000))

    # NOW-XD: usa meia-noite do dia correto, não ponto flutuante
    m = re.fullmatch(r"NOW-(\d+)D", text)
    if m:
        days = int(m.group(1))
        # converte para início do dia (meia-noite local) X dias atrás — evita corte no meio do dia
        return _day_start_ms(days)

    m = re.fullmatch(r"NOW\+(\d+)D", text)
    if m:
        days = int(m.group(1))
        dt = now_utc + timedelta(days=days)
        return str(int(dt.timestamp() * 1000))
    if text == "__NOW_PLUS_1_DAY__":
        dt = now_utc + timedelta(days=1)
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return value


def _template_replace(text: str, props: dict[str, Any], row_id: str) -> str:
    def _repl(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        if key == "id":
            return row_id
        return str(props.get(key) or "")

    return re.sub(r"\{\{\s*([^}]+?)\s*\}\}", _repl, text or "")


def _resolve_template_payload(template: dict[str, Any], props: dict[str, Any], row_id: str) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for key, value in (template or {}).items():
        if isinstance(value, str):
            value = _template_replace(value, props, row_id)
            value = _resolve_relative_time_token(value)
        resolved[key] = value
    return resolved


def _row_candidate_emails(props: dict[str, Any]) -> list[str]:
    candidates = [
        str(props.get("email") or "").strip().lower(),
        str(props.get("hs_associated_contact_email") or "").strip().lower(),
    ]
    return [c for c in candidates if c]


def _lookup_related_record_by_email(access_token: str, object_type: str, email: str) -> Optional[str]:
    if object_type == "companies":
        return None
    email_prop = _resolve_property_name(access_token, object_type, ["email", "e-mail", "hs_associated_contact_email"])
    if not email_prop:
        return None
    rows = _search_objects(
        access_token,
        object_type=object_type,
        properties=[email_prop],
        filter_groups=[{"filters": [{"propertyName": email_prop, "operator": "EQ", "value": email}]}],
        per_page=5,
        max_records=5,
    )
    for row in rows:
        row_id = (row or {}).get("id")
        if row_id is not None:
            return str(row_id)
    return None


def _search_name_candidates(access_token: str, object_type: str, phrase: str, limit: int = 5) -> list[dict[str, Any]]:
    properties = list(_object_config(object_type).get("default_properties") or [])
    if object_type == "leads":
        properties = list(dict.fromkeys(properties + ["hs_lead_name", "hs_associated_contact_email", "hubspot_owner_id"]))
    if object_type == "contacts":
        properties = list(dict.fromkeys(properties + ["firstname", "lastname", "email", "hubspot_owner_id"]))
    tokens = [tok for tok in _normalize_text(phrase).split() if tok]
    candidate_rows: list[dict[str, Any]] = []
    search_variants = [phrase]
    if tokens:
        search_variants.append(tokens[0])
    seen_ids: set[str] = set()
    for query in search_variants:
        body = {"query": query, "properties": properties, "limit": 50}
        res = hubspot_universal_api_call(access_token, endpoint=f"/crm/v3/objects/{object_type}/search", method="POST", json_payload=body)
        if not res.get("ok"):
            continue
        for row in (res.get("response_json") or {}).get("results") or []:
            row_id = str((row or {}).get("id") or "")
            if row_id and row_id not in seen_ids:
                seen_ids.add(row_id)
                candidate_rows.append(row)
    if not candidate_rows:
        candidate_rows = _search_objects(
            access_token,
            object_type=object_type,
            properties=properties,
            per_page=100,
            max_records=120,
        )
    scored: list[tuple[int, dict[str, Any]]] = []
    for row in candidate_rows:
        row_id = str((row or {}).get("id") or "")
        props = (row or {}).get("properties") or {}
        if object_type == "leads":
            label = _safe_prop(props, "hs_lead_name", default=f"Lead {row_id}")
            aux = _safe_prop(props, "hs_associated_contact_email", default="")
        else:
            label = _make_contact_full_name(props)
            aux = _safe_prop(props, "email", default="")
        score = _score_name_match(phrase, f"{label} {aux}")
        if score > 0:
            scored.append(
                (
                    score,
                    {
                        "id": row_id,
                        "name": label,
                        "email": aux,
                        "object_type": object_type,
                        "score": score,
                    },
                )
            )
    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:limit]]


def _build_name_candidate_context(access_token: str, user_text: str) -> str:
    phrases = _extract_possible_name_phrases(user_text)
    if not phrases:
        return ""
    lines = ["CANDIDATOS POR NOME PARCIAL:"]
    for phrase in phrases[:1]:
        lead_candidates = _search_name_candidates(access_token, "leads", phrase, limit=5)
        contact_candidates = _search_name_candidates(access_token, "contacts", phrase, limit=5)
        if lead_candidates or contact_candidates:
            lines.append(f"- frase={phrase}")
            if lead_candidates:
                lines.append(f"  leads={json.dumps(lead_candidates, ensure_ascii=False)}")
            if contact_candidates:
                lines.append(f"  contatos={json.dumps(contact_candidates, ensure_ascii=False)}")
    return "\n".join(lines)


def _extract_post_association_filters(search_body: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    patched = json.loads(json.dumps(search_body or {}))
    assoc_filters: list[dict[str, Any]] = []
    for group in patched.get("filterGroups") or []:
        new_filters = []
        for flt in (group or {}).get("filters") or []:
            prop = str((flt or {}).get("propertyName") or "")
            if prop.startswith("associations."):
                assoc_filters.append(
                    {
                        "to_object_type": prop.split(".", 1)[1],
                        "operator": str((flt or {}).get("operator") or ""),
                    }
                )
                continue
            new_filters.append(flt)
        group["filters"] = new_filters
    patched["filterGroups"] = [g for g in (patched.get("filterGroups") or []) if (g or {}).get("filters")]
    return patched, assoc_filters


def _apply_post_association_filters(
    access_token: str,
    *,
    from_object_type: str,
    rows: list[dict[str, Any]],
    assoc_filters: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not assoc_filters or not rows:
        return rows
    filtered = rows
    for assoc in assoc_filters:
        to_object_type = str(assoc.get("to_object_type") or "")
        operator = str(assoc.get("operator") or "")
        ids = [str((row or {}).get("id")) for row in filtered if (row or {}).get("id") is not None]
        assoc_map = _batch_read_associations(
            access_token,
            from_object_type=from_object_type,
            to_object_type=to_object_type,
            record_ids=ids,
        )
        if operator == "NOT_HAS_PROPERTY":
            filtered = [row for row in filtered if not assoc_map.get(str(row.get("id")), [])]
        elif operator == "HAS_PROPERTY":
            filtered = [row for row in filtered if assoc_map.get(str(row.get("id")), [])]
    return filtered


def _shared_metadata_snapshot(access_token: str) -> dict[str, Any]:
    return {
        "leads": _fetch_object_metadata_snapshot(access_token, "leads"),
        "deals": _fetch_object_metadata_snapshot(access_token, "deals"),
        "tasks": _fetch_object_metadata_snapshot(access_token, "tasks"),
        "meetings": _fetch_object_metadata_snapshot(access_token, "meetings"),
    }


def _build_capability_context(access_token: str, user_text: str) -> str:
    mentioned = _mentioned_objects(user_text)
    lines = ["CAPABILITIES DESCOBERTAS DINAMICAMENTE DO HUBSPOT:"]
    for object_type in mentioned:
        snapshot = _fetch_object_metadata_snapshot(access_token, object_type)
        lines.append(f"- objeto={object_type}")
        lines.append(f"  date_property={snapshot.get('date_property')}")
        lines.append(f"  sample_properties={json.dumps(snapshot.get('sample_properties') or [], ensure_ascii=False)}")
        pipelines = snapshot.get("pipelines") or []
        if pipelines:
            lines.append(f"  pipelines={json.dumps(pipelines[:10], ensure_ascii=False)}")
    assoc_pairs = [
        ("contacts", "leads"),
        ("leads", "contacts"),
        ("leads", "deals"),
        ("deals", "tasks"),
        ("deals", "meetings"),
    ]
    for from_obj, to_obj in assoc_pairs:
        if from_obj in mentioned or to_obj in mentioned:
            labels = _discover_association_labels(access_token, from_obj, to_obj)
            if labels:
                lines.append(f"- association_labels {from_obj}->{to_obj}: {json.dumps(labels[:10], ensure_ascii=False)}")
    lines.append(
        "- Regra: antes de assumir propriedade/associationTypeId, prefira usar estes metadados descobertos. "
        "Se ainda faltar algo, use a ferramenta universal para consultar propriedades, pipelines ou labels."
    )
    candidate_context = _build_name_candidate_context(access_token, user_text)
    if candidate_context:
        lines.append("")
        lines.append(candidate_context)
    lines.append("")
    lines.append(_build_memory_context(user_text))
    return "\n".join(lines)


def anthropic_plan_hubspot_execution(api_key: str, *, model: str, user_text: str, capability_context: str) -> str:
    prompt = (
        "Voce e a Livia. Planeje internamente a execucao no HubSpot usando as capabilities descobertas.\n"
        "Retorne um plano curto e objetivo com: objeto principal, filtros, associacoes necessarias, operacao esperada, "
        "e quais endpoints/metadados devem ser consultados antes de agir. Nao fale com o usuario ainda.\n\n"
        f"Pedido:\n{user_text}\n\n"
        f"{capability_context}"
    )
    url = f"{ANTHROPIC_BASE}/v1/messages"
    body = {
        "model": model,
        "max_tokens": 600,
        "system": "Planeje operacoes HubSpot com cautela, usando metadados dinamicos antes de agir.",
        "messages": [{"role": "user", "content": prompt}],
    }
    resp = _ANTHROPIC_SESSION.post(url, headers=_anthropic_headers(api_key), json=body, timeout=(10, 30))
    if resp.status_code >= 400:
        return ""
    return _extract_text((resp.json().get("content") or []))


def _extract_json_object(text: str) -> Optional[dict[str, Any]]:  # [CORRIGIDO] AVISO 3 — diagnóstico em JSON inválido
    raw = (text or "").strip()
    if not raw:
        _status("AVISO: _extract_json_object recebeu texto vazio.", level="warning")
        return None
    fence_match = re.search(r"\{[\s\S]*\}", raw)
    if not fence_match:
        _status(f"AVISO: _extract_json_object nao encontrou JSON no texto: {raw[:200]}", level="warning")
        return None
    try:
        value = json.loads(fence_match.group(0))
    except Exception as e:
        _status(f"AVISO: _extract_json_object falhou no parse JSON: {e} | texto: {raw[:200]}", level="warning")
        return None
    return value if isinstance(value, dict) else None


def anthropic_plan_hubspot_execution_json(api_key: str, *, model: str, user_text: str, capability_context: str) -> Optional[dict[str, Any]]:
    prompt = (
        "Voce e a Livia. Gere um plano ESTRUTURADO em JSON para executar um pedido HubSpot.\n"
        "Responda SOMENTE com JSON valido.\n"
        "Schema:\n"
        "{\n"
        '  "mode": "search_analyze" | "bulk_update_by_search" | "assign_owner_by_search" | "move_pipeline_stage" | "create_tasks_for_search_results" | "schedule_meetings_for_search_results" | "close_or_complete_tasks_by_search" | "create_deals_for_search_results" | "create_and_associate" | "repair_associations_by_search" | "unsupported",\n'
        '  "object_type": "contacts|leads|deals|tasks|meetings|null",\n'
        '  "related_object_type": "contacts|leads|deals|tasks|meetings|companies|null",\n'
        '  "date_property": "nome_da_propriedade_ou_null",\n'
        '  "search_body": {"filterGroups":[{"filters":[{"propertyName":"createdate","operator":"BETWEEN","value":"ONTEM_inicio_epoch_ms","highValue":"ONTEM_fim_epoch_ms"}]}],"sorts":["-createdate"],"properties":["firstname","lastname","email"],"limit":20} | null,\n'
        '  "properties_to_update": { ... } | null,\n'
        '  "owner_email": "email ou null",\n'
        '  "task_template": { ... } | null,\n'
        '  "meeting_template": { ... } | null,\n'
        '  "create_properties": { ... } | null,\n'
        '  "analysis_goal": "texto curto ou null",\n'
        '  "reason": "texto curto"\n'
        "}\n"
        "Regras:\n"
        "- REGRA DE OURO (CRÍTICO): Este planner JSON é EXCLUSIVO para operações EM LOTE (bulk) ou buscas amplas. Se o usuário pedir uma ação SINGULAR e específica para apenas 1 pessoa/registro (ex: 'agendar UMA reunião com a Marcela', 'criar UMA tarefa para o Edson', 'anotar no contato'), você DEVE OBRIGATORIAMENTE retornar 'unsupported'. Isso forçará o sistema a ignorar o plano autônomo e usar o modo conversacional interativo, que consegue buscar os IDs corretamente antes de agir.\n"
        "- Use bulk_update_by_search apenas se o pedido for uma atualizacao em massa clara.\n"
        "- Use assign_owner_by_search quando o pedido for atribuir owner em massa por criterio de busca.\n"
        "- Use move_pipeline_stage quando o pedido for mover etapa/pipeline em massa.\n"
        "- Use create_tasks_for_search_results quando o pedido for criar tarefas para um conjunto encontrado.\n"
        "- Use schedule_meetings_for_search_results quando o pedido for agendar reunioes para um conjunto encontrado.\n"
        "- Use close_or_complete_tasks_by_search quando o pedido for concluir/fechar tarefas em massa.\n"
        "- Use create_deals_for_search_results quando o pedido for criar negocios para registros encontrados.\n"
        "- Use create_and_associate quando o pedido for criar registros relacionados e associar automaticamente.\n"
        "- Use repair_associations_by_search quando o pedido for corrigir associacoes ausentes entre objetos.\n"
        "- Use search_analyze se o pedido pedir listar, analisar, auditar, diagnosticar, resumir ou encontrar registros.\n"
        "- FORMATO OBRIGATORIO de sorts: lista de strings (ex.: [\"createdate\"] ou [\"-createdate\"] para DESC). NUNCA lista de objetos.\n"
        "- FORMATO OBRIGATORIO de filterGroups: [{\"filters\":[{\"propertyName\":\"...\",\"operator\":\"...\",\"value\":\"...\"}]}]. NUNCA lista plana de filtros.\n"
        "- Para filtros de data use operator BETWEEN com value=epoch_ms_inicio (string) e highValue=epoch_ms_fim (string).\n"
        "- O campo limit dentro de search_body deve ser um inteiro. Para pedidos como 'ultimos 3' use limit:3. Para 'ultimos 10' use limit:10.\n"
        "- Use os epoch_ms do CONTEXTO DE TEMPO fornecido para montar filtros de data. NUNCA invente timestamps.\n"
        "- Se nao souber com seguranca, use unsupported.\n"
        "- Use as capabilities e metadados reais fornecidos.\n\n"
        f"Pedido:\n{user_text}\n\n"
        f"{capability_context}"
    )
    url = f"{ANTHROPIC_BASE}/v1/messages"
    body = {
        "model": model,
        "max_tokens": 1500,
        "system": "Planeje operacoes HubSpot em JSON estrito. Sem markdown.",
        "messages": [{"role": "user", "content": prompt}],
    }
    try:
        resp = _ANTHROPIC_SESSION.post(url, headers=_anthropic_headers(api_key), json=body, timeout=(10, 45))
    except Exception:
        return None
    if resp.status_code >= 400:
        return None
    return _extract_json_object(_extract_text((resp.json().get("content") or [])))


def anthropic_repair_hubspot_execution_json(
    api_key: str,
    *,
    model: str,
    user_text: str,
    capability_context: str,
    previous_plan: dict[str, Any],
    execution_result: dict[str, Any],
) -> Optional[dict[str, Any]]:
    prompt = (
        "Voce e a Livia. Um plano HubSpot falhou ou ficou incompleto. Gere um NOVO plano JSON corrigido.\n"
        "Responda SOMENTE com JSON valido no mesmo schema anterior.\n"
        "Use o erro real da API, a memoria operacional e as capabilities da conta para corrigir a estrategia.\n"
        "Se nao houver correcao segura, retorne mode=unsupported.\n\n"
        f"Pedido:\n{user_text}\n\n"
        f"Capabilities e memoria:\n{capability_context}\n\n"
        f"Plano anterior:\n{json.dumps(previous_plan, ensure_ascii=False)}\n\n"
        f"Resultado/erro anterior:\n{json.dumps(execution_result, ensure_ascii=False)}"
    )
    url = f"{ANTHROPIC_BASE}/v1/messages"
    body = {
        "model": model,
        "max_tokens": 1500,
        "system": "Corrija planos HubSpot em JSON estrito. Sem markdown.",
        "messages": [{"role": "user", "content": prompt}],
    }
    try:
        resp = _ANTHROPIC_SESSION.post(url, headers=_anthropic_headers(api_key), json=body, timeout=(10, 45))
    except Exception:
        return None
    if resp.status_code >= 400:
        return None
    return _extract_json_object(_extract_text((resp.json().get("content") or [])))


def _normalize_execution_plan(access_token: str, plan: dict[str, Any]) -> Optional[dict[str, Any]]:
    mode = str(plan.get("mode") or "").strip()
    object_type = str(plan.get("object_type") or "").strip().lower()
    if mode not in {
        "search_analyze",
        "bulk_update_by_search",
        "assign_owner_by_search",
        "move_pipeline_stage",
        "create_tasks_for_search_results",
        "schedule_meetings_for_search_results",
        "close_or_complete_tasks_by_search",
        "create_deals_for_search_results",
        "create_and_associate",
        "repair_associations_by_search",
    }:
        return None
    if object_type not in HUBSPOT_OBJECT_CONFIGS:
        return None
    related_object_type = str(plan.get("related_object_type") or "").strip().lower()
    if related_object_type and related_object_type not in set(HUBSPOT_OBJECT_CONFIGS) | {"companies"}:
        return None
    search_body = plan.get("search_body") or {}
    if not isinstance(search_body, dict):
        return None
    normalized = json.loads(json.dumps(plan))
    normalized["object_type"] = object_type
    normalized["search_body"] = _resolve_search_payload_with_capabilities(access_token, object_type, search_body)
    for group in normalized["search_body"].get("filterGroups") or []:
        for flt in (group or {}).get("filters") or []:
            if "value" in flt:
                flt["value"] = _resolve_relative_time_token(flt.get("value"))
            if "highValue" in flt:
                flt["highValue"] = _resolve_relative_time_token(flt.get("highValue"))
    if mode == "search_analyze":
        if not normalized["search_body"].get("properties"):
            normalized["search_body"]["properties"] = list(_object_config(object_type).get("default_properties") or [])
        # [CORRIGIDO] BUG paginação infinita — preserve limit explícito; default seguro=20 (nunca 100)
        raw_limit = normalized["search_body"].get("limit")
        if raw_limit not in (None, "", 0, "0"):
            normalized["search_body"]["limit"] = max(1, min(200, int(raw_limit)))
        else:
            normalized["search_body"]["limit"] = 20
    if mode in {"bulk_update_by_search", "move_pipeline_stage"}:
        props = normalized.get("properties_to_update")
        if not isinstance(props, dict) or not props:
            return None
    if mode == "assign_owner_by_search":
        owner_email = _extract_email(str(normalized.get("owner_email") or ""))
        if not owner_email:
            return None
        owner_id = _resolve_owner_id_by_email(access_token, owner_email)
        normalized["owner_email"] = owner_email
        normalized["properties_to_update"] = {"hubspot_owner_id": owner_id}
    if mode == "create_tasks_for_search_results":
        task_template = normalized.get("task_template") or {}
        if not isinstance(task_template, dict) or not task_template.get("hs_task_subject"):
            return None
        task_template.setdefault("hs_task_status", "NOT_STARTED")
        task_template.setdefault("hs_task_priority", "NONE")
        if not task_template.get("hs_timestamp"):
            future = _local_now().astimezone(timezone.utc) + timedelta(days=1)
            task_template["hs_timestamp"] = future.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        normalized["task_template"] = task_template
    if mode == "schedule_meetings_for_search_results":
        meeting_template = normalized.get("meeting_template") or {}
        if not isinstance(meeting_template, dict):
            return None
        meeting_template.setdefault("hs_meeting_title", "Reuniao de follow-up")
        if not meeting_template.get("hs_timestamp"):
            future = _local_now().astimezone(timezone.utc) + timedelta(days=1)
            meeting_template["hs_timestamp"] = future.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        normalized["meeting_template"] = meeting_template
    if mode == "close_or_complete_tasks_by_search":
        normalized["object_type"] = "tasks"
        normalized["properties_to_update"] = normalized.get("properties_to_update") or {"hs_task_status": "COMPLETED"}
    if mode == "create_deals_for_search_results":
        create_properties = normalized.get("create_properties") or {}
        if not isinstance(create_properties, dict):
            return None
        create_properties.setdefault("pipeline", "default")
        if not create_properties.get("dealstage"):
            snapshot = _fetch_object_metadata_snapshot(access_token, "deals")
            pipes = snapshot.get("pipelines") or []
            if pipes:
                create_properties["pipeline"] = pipes[0].get("id") or create_properties.get("pipeline")
        create_properties.setdefault("dealname", "Novo negocio")
        normalized["create_properties"] = create_properties
    if mode == "create_and_associate":
        if not related_object_type:
            return None
        create_properties = normalized.get("create_properties") or {}
        if not isinstance(create_properties, dict) or not create_properties:
            return None
        normalized["related_object_type"] = related_object_type
        normalized["create_properties"] = create_properties
    if mode == "repair_associations_by_search":
        if not related_object_type:
            return None
        normalized["related_object_type"] = related_object_type
    return normalized


_WRITE_MODES = {
    "bulk_update_by_search",
    "assign_owner_by_search",
    "move_pipeline_stage",
    "create_tasks_for_search_results",
    "schedule_meetings_for_search_results",
    "close_or_complete_tasks_by_search",
    "create_deals_for_search_results",
    "create_and_associate",
    "repair_associations_by_search",
}


def _is_write_mode(mode: str) -> bool:
    return mode in _WRITE_MODES


def _looks_bulk_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    return any(
        word in t
        for word in [
            "todos",
            "todas",
            "em lote",
            "varios",
            "varias",
            "vários",
            "várias",
            "ultimos",
            "últimos",
            "contatos que",
            "leads que",
            "registros",
        ]
    )


def _request_looks_singular(user_text: str) -> bool:
    t = _normalize_text(user_text)
    if _looks_bulk_request(t):
        return False
    return any(word in t for word in [" esse ", " essa ", " o ", " a ", " minha reuniao", " meu lead", " da maria", " do edson", " do wagner"])


def _search_body_has_strong_filter(search_body: dict[str, Any]) -> bool:
    for group in search_body.get("filterGroups") or []:
        for flt in (group or {}).get("filters") or []:
            operator = str((flt or {}).get("operator") or "").upper()
            prop = str((flt or {}).get("propertyName") or "")
            value = str((flt or {}).get("value") or "")
            if operator == "EQ" and value:
                return True
            if operator == "BETWEEN" and value and flt.get("highValue"):
                return True
            if "email" in prop.lower() and value:
                return True
    query = str(search_body.get("query") or "").strip()
    return len(query) >= 8


def _validate_contact_creation_payload(create_props: dict[str, Any]) -> bool:
    email = str(create_props.get("email") or "").strip()
    firstname = str(create_props.get("firstname") or "").strip()
    lastname = str(create_props.get("lastname") or "").strip()
    return bool(email or (firstname and lastname))


def _format_candidate_rows_for_user(object_type: str, rows: list[dict[str, Any]], max_items: int = 10) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in (rows or [])[:max_items]:
        rid = str((row or {}).get("id") or "")
        props = (row or {}).get("properties") or {}
        if object_type == "contacts":
            name = _make_contact_full_name(props)
            email = str(props.get("email") or "").strip()
            phone = str(props.get("phone") or props.get("mobilephone") or "").strip()
            created = str(props.get("createdate") or "").strip()
            out.append({"id": rid, "name": name, "email": email, "phone": phone, "createdate": created})
        elif object_type == "leads":
            name = str(props.get("hs_lead_name") or rid)
            stage = str(props.get("hs_pipeline_stage") or "")
            pipe = str(props.get("hs_pipeline") or "")
            out.append({"id": rid, "name": name, "pipeline": pipe, "stage": stage})
        else:
            out.append({"id": rid, "properties": props})
    return out


def _resolve_targets_for_write_plan(access_token: str, user_text: str, plan: dict[str, Any]) -> dict[str, Any]:
    rows, search_body, assoc_filters = _search_rows_from_plan(access_token, plan, default_max_records=50)
    count = len(rows)
    mode = str(plan.get("mode") or "")
    explicit_bulk = _looks_bulk_request(user_text)
    singular = _request_looks_singular(user_text)
    strong_filter = _search_body_has_strong_filter(search_body)
    if count == 0:
        return {
            "target_mode": "empty",
            "match_count": 0,
            "resolved_ids": [],
            "rows": [],
            "search_body": search_body,
            "assoc_filters": assoc_filters,
            "reason": "Nenhum registro encontrado para executar com seguranca.",
        }
    if singular and count != 1:
        return {
            "target_mode": "ambiguous",
            "match_count": count,
            "resolved_ids": [str((row or {}).get("id") or "") for row in rows[:10]],
            "rows": rows[:10],
            "search_body": search_body,
            "assoc_filters": assoc_filters,
            "reason": "Pedido parece singular, mas a busca retornou mais de um registro.",
        }
    if mode in {"create_and_associate", "move_pipeline_stage", "repair_associations_by_search"} and count > 3 and not explicit_bulk:
        return {
            "target_mode": "ambiguous",
            "match_count": count,
            "resolved_ids": [str((row or {}).get("id") or "") for row in rows[:10]],
            "rows": rows[:10],
            "search_body": search_body,
            "assoc_filters": assoc_filters,
            "reason": "Operacao de escrita encontrou varios registros sem pedido explicito em lote.",
        }
    if not strong_filter and count > 1 and not explicit_bulk:
        return {
            "target_mode": "ambiguous",
            "match_count": count,
            "resolved_ids": [str((row or {}).get("id") or "") for row in rows[:10]],
            "rows": rows[:10],
            "search_body": search_body,
            "assoc_filters": assoc_filters,
            "reason": "Busca ampla demais para uma operacao de escrita automatica.",
        }
    return {
        "target_mode": "unique_match" if count == 1 else "bulk_filter",
        "match_count": count,
        "resolved_ids": [str((row or {}).get("id") or "") for row in rows],
        "rows": rows,
        "search_body": search_body,
        "assoc_filters": assoc_filters,
        "reason": "Alvo resolvido com seguranca.",
    }


def _prepare_safe_execution_plan(access_token: str, user_text: str, plan: dict[str, Any]) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    mode = str(plan.get("mode") or "")
    if not _is_write_mode(mode):
        return plan, None
    related_object_type = str(plan.get("related_object_type") or "")
    object_type = str(plan.get("object_type") or "")
    t = _normalize_text(user_text)
    if mode in {"bulk_update_by_search", "assign_owner_by_search", "move_pipeline_stage", "close_or_complete_tasks_by_search"} and any(
        marker in t for marker in ["criados", "criadas", "que voce criou", "que você criou", "esses", "essas", "eles", "elas", "leads criados", "contatos criados"]
    ):
        remembered_ids = _recent_entity_ids_for_object(object_type)
        if remembered_ids:
            secured = json.loads(json.dumps(plan))
            secured["_resolved_target"] = {
                "target_mode": "memory_followup",
                "match_count": len(remembered_ids),
                "resolved_ids": remembered_ids,
                "rows": [],
                "search_body": plan.get("search_body") or {},
                "assoc_filters": [],
                "reason": "Alvos recuperados da memoria operacional recente.",
            }
            secured["max_records"] = len(remembered_ids)
            return secured, None
    if mode == "create_and_associate" and object_type == "leads" and related_object_type == "contacts":
        return None, {
            "ok": False,
            "message": "Bloqueei uma criacao autonoma invertida de contatos para leads, pois isso pode gerar contatos indevidos.",
            "errors": [{"type": "unsafe_create_pair", "object_type": object_type, "related_object_type": related_object_type}],
        }
    if mode == "create_and_associate" and related_object_type == "contacts":
        create_props = dict(plan.get("create_properties") or {})
        if not _validate_contact_creation_payload(create_props):
            return None, {
                "ok": False,
                "message": "Bloqueei criacao automatica de contato sem email ou nome completo valido.",
                "errors": [{"type": "unsafe_contact_creation", "create_properties": create_props}],
            }
    resolution = _resolve_targets_for_write_plan(access_token, user_text, plan)
    if resolution.get("target_mode") in {"empty", "ambiguous"}:
        candidates = _format_candidate_rows_for_user(str(plan.get("object_type") or ""), list(resolution.get("rows") or []), max_items=10)
        return None, {
            "ok": False,
            "message": (
                str(resolution.get("reason") or "")
                + (" Eu encontrei estas opcoes (responda com 1 ou 2 IDs, ou com os numeros 1/2/3...):" if candidates else "")
            ).strip(),
            "resolution": resolution,
            "candidates": candidates,
            "errors": [{"type": resolution.get("target_mode"), "match_count": resolution.get("match_count")}],
        }
    secured = json.loads(json.dumps(plan))
    secured["_resolved_target"] = resolution
    secured["max_records"] = min(int(plan.get("max_records") or resolution.get("match_count") or 1), max(1, int(resolution.get("match_count") or 1)))
    return secured, None


def _execute_generic_search_analyze(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    object_type = str(plan.get("object_type"))
    search_body = dict(plan.get("search_body") or {})
    search_body, assoc_filters = _extract_post_association_filters(search_body)
    # [CORRIGIDO] BUG paginação infinita — max_records deve respeitar limit explícito do search_body
    explicit_limit = int(search_body.get("limit") or 0)
    max_rec = explicit_limit if explicit_limit > 0 else int(plan.get("max_records") or 100)
    results = _search_objects(
        access_token,
        object_type=object_type,
        properties=search_body.get("properties") or [],
        filter_groups=search_body.get("filterGroups") or [],
        sorts=search_body.get("sorts") or [],
        per_page=min(max_rec, 200),
        max_records=max_rec,
    )
    results = _apply_post_association_filters(
        access_token,
        from_object_type=object_type,
        rows=results,
        assoc_filters=assoc_filters,
    )
    sample_rows: list[dict[str, Any]] = []
    for row in results[:25]:
        sample_rows.append(
            {
                "id": str((row or {}).get("id") or ""),
                "properties": (row or {}).get("properties") or {},
            }
        )
    return {
        "mode": "search_analyze",
        "object_type": object_type,
        "analysis_goal": plan.get("analysis_goal"),
        "search_body": search_body,
        "post_association_filters": assoc_filters,
        "total_results": len(results),
        "sample_rows": sample_rows,
    }


def _execute_generic_bulk_update_by_search(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    resolved = plan.get("_resolved_target") or {}
    resolved_ids = [rid for rid in (resolved.get("resolved_ids") or []) if rid]
    if resolved_ids:
        return hubspot_bulk_update_tool(
            access_token,
            object_type=str(plan.get("object_type")),
            record_ids=resolved_ids,
            properties_to_update=plan.get("properties_to_update") or {},
        )
    return hubspot_bulk_update_by_search_tool(
        access_token,
        object_type=str(plan.get("object_type")),
        search_body=plan.get("search_body") or {},
        properties_to_update=plan.get("properties_to_update") or {},
        max_records=plan.get("max_records"),
    )


def _execute_generic_assign_owner_by_search(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    resolved = plan.get("_resolved_target") or {}
    resolved_ids = [rid for rid in (resolved.get("resolved_ids") or []) if rid]
    if resolved_ids:
        return hubspot_bulk_update_tool(
            access_token,
            object_type=str(plan.get("object_type")),
            record_ids=resolved_ids,
            properties_to_update=plan.get("properties_to_update") or {},
        )
    return hubspot_bulk_update_by_search_tool(
        access_token,
        object_type=str(plan.get("object_type")),
        search_body=plan.get("search_body") or {},
        properties_to_update=plan.get("properties_to_update") or {},
        max_records=plan.get("max_records"),
    )


def _execute_generic_move_pipeline_stage(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    resolved = plan.get("_resolved_target") or {}
    resolved_ids = [rid for rid in (resolved.get("resolved_ids") or []) if rid]
    if resolved_ids:
        return hubspot_bulk_update_tool(
            access_token,
            object_type=str(plan.get("object_type")),
            record_ids=resolved_ids,
            properties_to_update=plan.get("properties_to_update") or {},
        )
    return hubspot_bulk_update_by_search_tool(
        access_token,
        object_type=str(plan.get("object_type")),
        search_body=plan.get("search_body") or {},
        properties_to_update=plan.get("properties_to_update") or {},
        max_records=plan.get("max_records"),
    )


def _execute_generic_create_tasks_for_search_results(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    object_type = str(plan.get("object_type"))
    search_body = dict(plan.get("search_body") or {})
    search_body, assoc_filters = _extract_post_association_filters(search_body)
    results = _search_objects(
        access_token,
        object_type=object_type,
        properties=search_body.get("properties") or list(_object_config(object_type).get("default_properties") or []),
        filter_groups=search_body.get("filterGroups") or [],
        sorts=search_body.get("sorts") or [],
        per_page=int(search_body.get("limit") or 100),
        max_records=int(plan.get("max_records") or 500),
    )
    results = _apply_post_association_filters(
        access_token,
        from_object_type=object_type,
        rows=results,
        assoc_filters=assoc_filters,
    )
    task_template = dict(plan.get("task_template") or {})
    for key in list(task_template.keys()):
        task_template[key] = _resolve_relative_time_token(task_template[key])
    task_template.pop("association_type_id", None)
    created: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for batch in _chunked([str((row or {}).get("id")) for row in results if (row or {}).get("id") is not None], 100):
        inputs = []
        row_map = {str((row or {}).get("id")): ((row or {}).get("properties") or {}) for row in results if (row or {}).get("id") is not None}
        for record_id in batch:
            payload = {"properties": _resolve_template_payload(task_template, row_map.get(record_id, {}), record_id)}
            inputs.append(payload)
        res = hubspot_universal_api_call(access_token, endpoint="/crm/v3/objects/tasks/batch/create", method="POST", json_payload={"inputs": inputs})
        if not res.get("ok"):
            errors.append({"record_ids": batch, "status_code": res.get("status_code"), "response_text": res.get("response_text")})
            continue
        created_results = (res.get("response_json") or {}).get("results") or []
        pairs = []
        for task_row, target_id in zip(created_results, batch):
            task_id = (task_row or {}).get("id")
            if task_id is not None:
                created.append({"task_id": str(task_id), "target_object_type": object_type, "target_id": target_id})
                pairs.append({"from": {"id": str(task_id)}, "to": {"id": target_id}})
        if pairs:
            assoc_res = hubspot_universal_api_call(
                access_token,
                endpoint=f"/crm/v4/associations/tasks/{object_type}/batch/associate/default",
                method="POST",
                json_payload={"inputs": pairs},
            )
            if not assoc_res.get("ok"):
                errors.append({"record_ids": batch, "status_code": assoc_res.get("status_code"), "response_text": assoc_res.get("response_text")})
    return {
        "ok": len(errors) == 0,
        "object_type": object_type,
        "searched": len(results),
        "created_tasks": len(created),
        "task_template": task_template,
        "created_samples": created[:20],
        "errors": errors[:10],
    }


def _search_rows_from_plan(access_token: str, plan: dict[str, Any], default_max_records: int = 500) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    resolved = plan.get("_resolved_target") or {}
    resolved_rows = resolved.get("rows") or []
    if resolved_rows:
        return list(resolved_rows), dict(resolved.get("search_body") or plan.get("search_body") or {}), list(resolved.get("assoc_filters") or [])
    object_type = str(plan.get("object_type"))
    search_body = dict(plan.get("search_body") or {})
    search_body, assoc_filters = _extract_post_association_filters(search_body)
    rows = _search_objects(
        access_token,
        object_type=object_type,
        properties=search_body.get("properties") or list(_object_config(object_type).get("default_properties") or []),
        filter_groups=search_body.get("filterGroups") or [],
        sorts=search_body.get("sorts") or [],
        per_page=int(search_body.get("limit") or 100),
        max_records=int(plan.get("max_records") or default_max_records),
    )
    rows = _apply_post_association_filters(
        access_token,
        from_object_type=object_type,
        rows=rows,
        assoc_filters=assoc_filters,
    )
    return rows, search_body, assoc_filters


def _batch_associate_default(access_token: str, from_object_type: str, to_object_type: str, pairs: list[dict[str, dict[str, str]]]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for chunk in _chunked([json.dumps(p, ensure_ascii=False) for p in pairs], 100):
        inputs = [json.loads(item) for item in chunk]
        res = hubspot_universal_api_call(
            access_token,
            endpoint=f"/crm/v4/associations/{from_object_type}/{to_object_type}/batch/associate/default",
            method="POST",
            json_payload={"inputs": inputs},
        )
        if not res.get("ok"):
            errors.append({"status_code": res.get("status_code"), "response_text": res.get("response_text")})
    return errors


def _execute_generic_schedule_meetings_for_search_results(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    object_type = str(plan.get("object_type"))
    rows, search_body, assoc_filters = _search_rows_from_plan(access_token, plan)
    meeting_template = dict(plan.get("meeting_template") or {})
    for key in list(meeting_template.keys()):
        meeting_template[key] = _resolve_relative_time_token(meeting_template[key])
    created: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for batch in _chunked([str((row or {}).get("id")) for row in rows if (row or {}).get("id") is not None], 100):
        row_map = {str((row or {}).get("id")): ((row or {}).get("properties") or {}) for row in rows if (row or {}).get("id") is not None}
        inputs = [{"properties": _resolve_template_payload(meeting_template, row_map.get(record_id, {}), record_id)} for record_id in batch]
        res = hubspot_universal_api_call(access_token, endpoint="/crm/v3/objects/meetings/batch/create", method="POST", json_payload={"inputs": inputs})
        if not res.get("ok"):
            errors.append({"record_ids": batch, "status_code": res.get("status_code"), "response_text": res.get("response_text")})
            continue
        pairs = []
        for meeting_row, target_id in zip((res.get("response_json") or {}).get("results") or [], batch):
            meeting_id = (meeting_row or {}).get("id")
            if meeting_id is not None:
                created.append({"meeting_id": str(meeting_id), "target_object_type": object_type, "target_id": target_id})
                pairs.append({"from": {"id": str(meeting_id)}, "to": {"id": target_id}})
        if pairs:
            errors.extend(_batch_associate_default(access_token, "meetings", object_type, pairs))
    return {
        "ok": len(errors) == 0,
        "object_type": object_type,
        "searched": len(rows),
        "search_body": search_body,
        "post_association_filters": assoc_filters,
        "created_meetings": len(created),
        "meeting_template": meeting_template,
        "created_samples": created[:20],
        "errors": errors[:10],
    }


def _execute_generic_close_or_complete_tasks_by_search(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    resolved = plan.get("_resolved_target") or {}
    resolved_ids = [rid for rid in (resolved.get("resolved_ids") or []) if rid]
    if resolved_ids:
        return hubspot_bulk_update_tool(
            access_token,
            object_type="tasks",
            record_ids=resolved_ids,
            properties_to_update=plan.get("properties_to_update") or {"hs_task_status": "COMPLETED"},
        )
    return hubspot_bulk_update_by_search_tool(
        access_token,
        object_type="tasks",
        search_body=plan.get("search_body") or {},
        properties_to_update=plan.get("properties_to_update") or {"hs_task_status": "COMPLETED"},
        max_records=plan.get("max_records"),
    )


def _execute_generic_create_deals_for_search_results(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    object_type = str(plan.get("object_type"))
    rows, search_body, assoc_filters = _search_rows_from_plan(access_token, plan)
    template = dict(plan.get("create_properties") or {})
    created: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for batch_rows in _chunked([json.dumps(r, ensure_ascii=False) for r in rows], 100):
        inputs = []
        target_ids: list[str] = []
        for raw in batch_rows:
            row = json.loads(raw)
            row_id = str((row or {}).get("id"))
            props = (row or {}).get("properties") or {}
            deal_props = _resolve_template_payload(template, props, row_id)
            if deal_props.get("dealname") in {"Novo negocio", "", None}:
                base_name = _make_contact_full_name(props) if object_type == "contacts" else str(props.get("hs_lead_name") or props.get("dealname") or row_id)
                deal_props["dealname"] = f"{base_name} - Novo negocio"
            owner_id = props.get("hubspot_owner_id")
            if owner_id and not deal_props.get("hubspot_owner_id"):
                deal_props["hubspot_owner_id"] = owner_id
            inputs.append({"properties": deal_props})
            target_ids.append(row_id)
        res = hubspot_universal_api_call(access_token, endpoint="/crm/v3/objects/deals/batch/create", method="POST", json_payload={"inputs": inputs})
        if not res.get("ok"):
            errors.append({"target_ids": target_ids, "status_code": res.get("status_code"), "response_text": res.get("response_text")})
            continue
        pairs = []
        for deal_row, target_id in zip((res.get("response_json") or {}).get("results") or [], target_ids):
            deal_id = (deal_row or {}).get("id")
            if deal_id is not None:
                created.append({"deal_id": str(deal_id), "target_object_type": object_type, "target_id": target_id})
                pairs.append({"from": {"id": str(deal_id)}, "to": {"id": target_id}})
        if pairs:
            errors.extend(_batch_associate_default(access_token, "deals", object_type, pairs))
    return {
        "ok": len(errors) == 0,
        "object_type": object_type,
        "searched": len(rows),
        "search_body": search_body,
        "post_association_filters": assoc_filters,
        "created_deals": len(created),
        "create_properties": template,
        "created_samples": created[:20],
        "errors": errors[:10],
    }


def _execute_generic_create_and_associate(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    object_type = str(plan.get("object_type"))
    related_object_type = str(plan.get("related_object_type"))
    rows, search_body, assoc_filters = _search_rows_from_plan(access_token, plan)
    if related_object_type == "contacts":
        return {
            "ok": False,
            "object_type": object_type,
            "related_object_type": related_object_type,
            "searched": len(rows),
            "message": "Criacao autonoma de contatos foi bloqueada por seguranca.",
            "errors": [{"type": "unsafe_contact_creation_blocked"}],
        }
    template = dict(plan.get("create_properties") or {})
    created: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    endpoint = f"/crm/v3/objects/{related_object_type}/batch/create"
    for batch_rows in _chunked([json.dumps(r, ensure_ascii=False) for r in rows], 100):
        inputs = []
        target_ids: list[str] = []
        for raw in batch_rows:
            row = json.loads(raw)
            row_id = str((row or {}).get("id"))
            props = (row or {}).get("properties") or {}
            create_props = _resolve_template_payload(template, props, row_id)
            if related_object_type == "leads" and not create_props.get("hs_lead_name"):
                create_props["hs_lead_name"] = _make_contact_full_name(props)
            if related_object_type == "deals" and not create_props.get("dealname"):
                base_name = _make_contact_full_name(props) if object_type == "contacts" else str(props.get("hs_lead_name") or props.get("dealname") or row_id)
                create_props["dealname"] = f"{base_name} - Novo negocio"
            inputs.append({"properties": create_props})
            target_ids.append(row_id)
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="POST", json_payload={"inputs": inputs})
        if not res.get("ok"):
            errors.append({"target_ids": target_ids, "status_code": res.get("status_code"), "response_text": res.get("response_text")})
            continue
        pairs = []
        for created_row, target_id in zip((res.get("response_json") or {}).get("results") or [], target_ids):
            created_id = (created_row or {}).get("id")
            if created_id is not None:
                created.append({"created_id": str(created_id), "created_object_type": related_object_type, "target_object_type": object_type, "target_id": target_id})
                pairs.append({"from": {"id": str(created_id)}, "to": {"id": target_id}})
        if pairs:
            errors.extend(_batch_associate_default(access_token, related_object_type, object_type, pairs))
    return {
        "ok": len(errors) == 0,
        "object_type": object_type,
        "related_object_type": related_object_type,
        "searched": len(rows),
        "search_body": search_body,
        "post_association_filters": assoc_filters,
        "created_count": len(created),
        "create_properties": template,
        "created_samples": created[:20],
        "errors": errors[:10],
    }


def _execute_create_leads_for_contacts_with_macro(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    rows, search_body, assoc_filters = _search_rows_from_plan(access_token, plan)
    stage = None
    create_props = plan.get("create_properties") or {}
    if isinstance(create_props, dict):
        stage = create_props.get("hs_pipeline_stage")
    created: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for row in rows:
        cid = str((row or {}).get("id") or "")
        props = (row or {}).get("properties") or {}
        lead_name = _make_contact_full_name(props)
        res = hubspot_create_and_associate_lead_tool(
            access_token,
            contact_id=cid,
            lead_name=lead_name,
            pipeline_stage=str(stage) if stage else None,
        )
        if res.get("ok"):
            created.append({"lead_id": str(res.get("lead_id") or ""), "contact_id": cid, "lead_name": lead_name})
        else:
            errors.append({"contact_id": cid, "lead_name": lead_name, "error": res.get("error"), "status_code": res.get("status_code")})
    return {
        "ok": len(errors) == 0,
        "mode": "create_and_associate",
        "object_type": "contacts",
        "related_object_type": "leads",
        "searched": len(rows),
        "search_body": search_body,
        "post_association_filters": assoc_filters,
        "created_count": len(created),
        "created_samples": created[:20],
        "errors": errors[:10],
        "message": "Leads criados e associados via ferramenta macro hubspot_create_and_associate_lead.",
    }


def _lookup_related_records_by_email(access_token: str, object_type: str, email: str) -> list[str]:
    email = str(email or "").strip().lower()
    if not email:
        return []
    email_prop = "email" if object_type == "contacts" else "hs_associated_contact_email"
    rows = _search_objects(
        access_token,
        object_type=object_type,
        properties=list(dict.fromkeys((_object_config(object_type).get("default_properties") or []) + [email_prop])),
        filter_groups=[{"filters": [{"propertyName": email_prop, "operator": "EQ", "value": email}]}],
        per_page=10,
        max_records=10,
    )
    return [str((row or {}).get("id")) for row in rows if (row or {}).get("id") is not None]


def _execute_generic_repair_associations_by_search(access_token: str, plan: dict[str, Any]) -> dict[str, Any]:
    object_type = str(plan.get("object_type"))
    related_object_type = str(plan.get("related_object_type"))
    rows, search_body, assoc_filters = _search_rows_from_plan(access_token, plan)
    missing_rows = _apply_post_association_filters(
        access_token,
        from_object_type=object_type,
        rows=rows,
        assoc_filters=[{"to_object_type": related_object_type, "operator": "NOT_HAS_PROPERTY"}],
    )
    if not missing_rows:
        return {
            "ok": True,
            "object_type": object_type,
            "related_object_type": related_object_type,
            "searched": len(rows),
            "missing_associations": 0,
            "message": "Nenhuma associacao faltante encontrada para reparar automaticamente.",
        }
    repaired_pairs: list[dict[str, str]] = []
    lookup_failures: list[dict[str, Any]] = []
    pairs: list[dict[str, dict[str, str]]] = []
    for row in missing_rows:
        row_id = str((row or {}).get("id") or "")
        props = (row or {}).get("properties") or {}
        related_id = None
        for email in _row_candidate_emails(props):
            matches = _lookup_related_records_by_email(access_token, related_object_type, email)
            if len(matches) == 1:
                related_id = matches[0]
                break
            if len(matches) > 1:
                lookup_failures.append({"id": row_id, "properties": props, "reason": "multiple_email_matches", "email": email, "matches": matches[:10]})
                break
        if related_id:
            pairs.append({"from": {"id": row_id}, "to": {"id": related_id}})
            repaired_pairs.append({"from_id": row_id, "to_id": related_id})
        else:
            if not any(f.get("id") == row_id for f in lookup_failures):
                lookup_failures.append({"id": row_id, "properties": props})
    errors: list[dict[str, Any]] = []
    if pairs:
        errors.extend(_batch_associate_default(access_token, object_type, related_object_type, pairs))
    if repaired_pairs and not errors:
        return {
            "ok": True,
            "object_type": object_type,
            "related_object_type": related_object_type,
            "searched": len(rows),
            "missing_associations": len(missing_rows),
            "repaired_associations": len(repaired_pairs),
            "repaired_samples": repaired_pairs[:20],
            "lookup_failures": lookup_failures[:20],
        }
    return {
        "ok": False if lookup_failures or errors else True,
        "object_type": object_type,
        "related_object_type": related_object_type,
        "searched": len(rows),
        "missing_associations": len(missing_rows),
        "repaired_associations": len(repaired_pairs),
        "repaired_samples": repaired_pairs[:20],
        "lookup_failures": lookup_failures[:20],
        "errors": errors[:10],
        "message": "Encontrei associacoes faltantes. Reparei o que foi possivel por lookup automatico e deixei o restante sinalizado.",
    }


def _dispatch_autonomous_mode(access_token: str, plan: dict[str, Any]) -> Optional[dict[str, Any]]:
    mode = plan.get("mode")
    if mode == "search_analyze":
        return _execute_generic_search_analyze(access_token, plan)
    if mode == "bulk_update_by_search":
        return _execute_generic_bulk_update_by_search(access_token, plan)
    if mode == "assign_owner_by_search":
        return _execute_generic_assign_owner_by_search(access_token, plan)
    if mode == "move_pipeline_stage":
        return _execute_generic_move_pipeline_stage(access_token, plan)
    if mode == "create_tasks_for_search_results":
        return _execute_generic_create_tasks_for_search_results(access_token, plan)
    if mode == "schedule_meetings_for_search_results":
        return _execute_generic_schedule_meetings_for_search_results(access_token, plan)
    if mode == "close_or_complete_tasks_by_search":
        return _execute_generic_close_or_complete_tasks_by_search(access_token, plan)
    if mode == "create_deals_for_search_results":
        return _execute_generic_create_deals_for_search_results(access_token, plan)
    if mode == "create_and_associate":
        if str(plan.get("object_type")) == "contacts" and str(plan.get("related_object_type")) == "leads":
            return _execute_create_leads_for_contacts_with_macro(access_token, plan)
        return _execute_generic_create_and_associate(access_token, plan)
    if mode == "repair_associations_by_search":
        return _execute_generic_repair_associations_by_search(access_token, plan)
    return None


def _present_autonomous_result(
    api_key: str,
    *,
    model: str,
    user_text: str,
    normalized: dict[str, Any],
    raw_result: dict[str, Any],
) -> str:
    mode = str(normalized.get("mode") or "")
    operation_names = {
        "search_analyze": f"busca e analise autonoma de {normalized.get('object_type')}",
        "bulk_update_by_search": f"atualizacao autonoma em massa de {normalized.get('object_type')}",
        "assign_owner_by_search": f"atribuicao autonoma de owner em massa para {normalized.get('object_type')}",
        "move_pipeline_stage": f"movimentacao autonoma de pipeline/stage para {normalized.get('object_type')}",
        "create_tasks_for_search_results": f"criacao autonoma de tarefas para resultados de busca em {normalized.get('object_type')}",
        "schedule_meetings_for_search_results": f"agendamento autonomo de reunioes para resultados de busca em {normalized.get('object_type')}",
        "close_or_complete_tasks_by_search": "conclusao autonoma de tarefas em massa",
        "create_deals_for_search_results": f"criacao autonoma de negocios para resultados de busca em {normalized.get('object_type')}",
        "create_and_associate": f"criacao e associacao autonoma de {normalized.get('related_object_type')} para {normalized.get('object_type')}",
        "repair_associations_by_search": f"reparo autonomo de associacoes {normalized.get('object_type')}->{normalized.get('related_object_type')}",
    }
    return anthropic_present_operation_result(
        api_key,
        model=model,
        user_text=user_text,
        operation_name=operation_names.get(mode, "execucao autonoma HubSpot"),
        operation_result={"ok": bool(raw_result.get("ok", True)), "plan": normalized, "result": raw_result},
    )


def _record_completed_turn(messages: list[dict[str, Any]], user_text: str, assistant_text: str) -> list[dict[str, Any]]:
    messages.append({"role": "user", "content": user_text})
    messages.append({"role": "assistant", "content": assistant_text})
    _remember_turn(user_text, assistant_text)
    return _prune_messages_for_speed(messages)


def _try_execute_autonomous_plan(
    api_key: str,
    *,
    model: str,
    access_token: str,
    user_text: str,
    capability_context: str,
) -> Optional[str]:
    execution = _execute_autonomous_plan_raw(
        api_key,
        model=model,
        access_token=access_token,
        user_text=user_text,
        capability_context=capability_context,
    )
    if not execution:
        return None
    normalized = execution["plan"]
    raw_result = execution["result"]
    return _present_autonomous_result(
        api_key,
        model=model,
        user_text=user_text,
        normalized=normalized,
        raw_result=raw_result,
    )


def _execute_autonomous_plan_raw(
    api_key: str,
    *,
    model: str,
    access_token: str,
    user_text: str,
    capability_context: str,
) -> Optional[dict[str, Any]]:
    plan = anthropic_plan_hubspot_execution_json(
        api_key,
        model=model,
        user_text=user_text,
        capability_context=capability_context,
    )
    if not plan:
        return None
    normalized = _normalize_execution_plan(access_token, plan)
    if not normalized:
        return None
    normalized, safety_result = _prepare_safe_execution_plan(access_token, user_text, normalized)
    if safety_result is not None:
        _remember_structured_execution(
            user_text,
            operation_name="bloqueio de seguranca antes da execucao autonoma",
            operation_result=safety_result,
            mode="safety_block",
            object_type=str(plan.get("object_type") or ""),
            related_object_type=str(plan.get("related_object_type") or ""),
            constraints=["nao executar quando o alvo nao estiver claramente resolvido"],
        )
        return {"plan": plan, "result": safety_result}
    if not normalized:
        return None
    raw_result = _dispatch_autonomous_mode(access_token, normalized)
    if raw_result is None:
        return None
    _remember_operation_artifacts(normalized, raw_result)
    success = bool(raw_result.get("ok", True)) and not raw_result.get("errors")
    _remember_execution_outcome(user_text, normalized, raw_result, success=success)
    if success:
        return {"plan": normalized, "result": raw_result}
    repaired_plan = anthropic_repair_hubspot_execution_json(
        api_key,
        model=model,
        user_text=user_text,
        capability_context=capability_context,
        previous_plan=normalized,
        execution_result=raw_result,
    )
    if repaired_plan:
        repaired_normalized = _normalize_execution_plan(access_token, repaired_plan)
        if repaired_normalized:
            repaired_result = _dispatch_autonomous_mode(access_token, repaired_normalized)
            if repaired_result is not None:
                _remember_operation_artifacts(repaired_normalized, repaired_result)
                repaired_success = bool(repaired_result.get("ok", True)) and not repaired_result.get("errors")
                _remember_execution_outcome(user_text, repaired_normalized, repaired_result, success=repaired_success)
                return {"plan": repaired_normalized, "result": repaired_result}
    return {"plan": normalized, "result": raw_result}


def _present_and_remember_operation(
    api_key: str,
    *,
    model: str,
    user_text: str,
    operation_name: str,
    operation_result: dict[str, Any],
    mode: str,
    object_type: Optional[str] = None,
    related_object_type: Optional[str] = None,
    constraints: Optional[list[str]] = None,
) -> str:
    deterministic = operation_result.get("deterministic_response") if isinstance(operation_result, dict) else None
    if isinstance(deterministic, str) and deterministic.strip():
        text = deterministic.strip()
    else:
        text = anthropic_present_operation_result(
            api_key,
            model=model,
            user_text=user_text,
            operation_name=operation_name,
            operation_result=operation_result,
        )
    _remember_structured_execution(
        user_text,
        operation_name=operation_name,
        operation_result=operation_result,
        mode=mode,
        object_type=object_type,
        related_object_type=related_object_type,
        constraints=constraints,
    )
    return text


def _orchestrate_user_request(
    api_key: str,
    *,
    model: str,
    access_token: str,
    user_text: str,
    pending_owner_assignment_email_request: bool,
) -> tuple[Optional[str], bool]:
    provided_email = _extract_email(user_text)
    if pending_owner_assignment_email_request and provided_email:
        raw_result = {"ok": True, "result_text": _execute_contact_owner_assignment(access_token, provided_email), "owner_email": provided_email, "owner_id": _resolve_owner_id_by_email(access_token, provided_email)}
        text = _present_and_remember_operation(
            api_key,
            model=model,
            user_text=user_text,
            operation_name="atualizacao em lote de owner para contatos sem proprietario",
            operation_result=raw_result,
            mode="direct_owner_assignment",
            object_type="contacts",
            constraints=["reutilizar owner resolvido por email em pedidos seguintes"],
        )
        return text, False
    if pending_owner_assignment_email_request:
        return None, True
    if _matches_contact_owner_assignment_request(user_text):
        owner_email = _extract_email(user_text)
        if owner_email:
            raw_result = {"ok": True, "result_text": _execute_contact_owner_assignment(access_token, owner_email), "owner_email": owner_email, "owner_id": _resolve_owner_id_by_email(access_token, owner_email)}
            text = _present_and_remember_operation(
                api_key,
                model=model,
                user_text=user_text,
                operation_name="atualizacao em lote de owner para contatos sem proprietario",
                operation_result=raw_result,
                mode="direct_owner_assignment",
                object_type="contacts",
                constraints=["reutilizar owner resolvido por email em pedidos seguintes"],
            )
            return text, False
        needs_input = {
            "ok": False,
            "needs_user_input": True,
            "missing_field": "owner_email",
            "instruction": "Solicitar o email do owner no HubSpot para atualizar em lote os contatos sem proprietario.",
        }
        text = _present_and_remember_operation(
            api_key,
            model=model,
            user_text=user_text,
            operation_name="pedido de complemento para atualizacao em lote de owner",
            operation_result=needs_input,
            mode="owner_followup_request",
            object_type="contacts",
        )
        return text, True
    if _matches_contact_note_request(user_text):
        raw = _execute_add_contact_note(access_token, user_text)
        text = _present_and_remember_operation(
            api_key,
            model=model,
            user_text=user_text,
            operation_name="criacao de nota (timeline) em contato a partir de linguagem natural",
            operation_result=raw,
            mode="contact_timeline_note",
            object_type="contacts",
            constraints=["nunca usar bulk_update em propriedade tipo notes do contato"],
        )
        return text, False
    if len(_split_operational_request(user_text)) >= 2:
        compound_text = _try_execute_compound_operational_plan(
            api_key,
            model=model,
            access_token=access_token,
            user_text=user_text,
        )
        if compound_text:
            return compound_text, False
    if _matches_direct_contact_lead_request(user_text):
        raw_text = _execute_direct_contact_lead_flow(access_token, user_text)
        text = _present_and_remember_operation(
            api_key,
            model=model,
            user_text=user_text,
            operation_name="criacao e associacao de leads para contatos sem lead",
            operation_result={"ok": True, "result_text": raw_text},
            mode="direct_contact_lead_flow",
            object_type="contacts",
            related_object_type="leads",
            constraints=["nao criar contatos novos ao criar leads para contatos"],
        )
        return text, False
    if _matches_direct_contact_list_request(user_text):
        raw = _execute_direct_contact_list_flow(access_token, user_text)
        text = _present_and_remember_operation(
            api_key,
            model=model,
            user_text=user_text,
            operation_name="busca deterministica de contatos por periodo (sem planner)",
            operation_result=raw,
            mode="direct_contact_list",
            object_type="contacts",
            constraints=["evitar planner para consultas simples de data", "nao paginar desnecessariamente"],
        )
        return text, False
    if _matches_direct_lead_date_search_request(user_text):
        raw = _execute_direct_lead_date_search_flow(access_token, user_text)
        text = _present_and_remember_operation(
            api_key,
            model=model,
            user_text=user_text,
            operation_name="busca deterministica de leads por data explicita (sem planner)",
            operation_result=raw,
            mode="direct_lead_date_search",
            object_type="leads",
            constraints=["evitar planner para buscas amplas com data explicita", "respeitar cap de registros"],
        )
        return text, False
    if _matches_stale_leads_without_contact_request(user_text):
        raw = _execute_stale_leads_without_contact(access_token, user_text)
        text = _present_and_remember_operation(
            api_key,
            model=model,
            user_text=user_text,
            operation_name="busca deterministica de leads sem contato ha X dias (com filtro por fase quando informado)",
            operation_result=raw,
            mode="stale_leads_without_contact",
            object_type="leads",
            related_object_type="contacts",
            constraints=["nao depender do planner para filtros simples de SDR"],
        )
        return text, False
    if _matches_large_volume_lead_analysis_request(user_text):
        text = _execute_large_volume_lead_analysis(api_key, model, access_token, user_text)
        _remember_structured_execution(
            user_text,
            operation_name="analise de alto volume de leads",
            operation_result={"ok": True, "analysis_text": text},
            mode="lead_analysis",
            object_type="leads",
        )
        return text, False
    if _matches_deal_operational_analysis_request(user_text):
        text = _execute_deal_operational_analysis(api_key, model, access_token, user_text)
        _remember_structured_execution(
            user_text,
            operation_name="analise operacional de negocios",
            operation_result={"ok": True, "analysis_text": text},
            mode="deal_analysis",
            object_type="deals",
        )
        return text, False
    if _matches_task_operational_analysis_request(user_text):
        text = _execute_task_operational_analysis(api_key, model, access_token, user_text)
        _remember_structured_execution(
            user_text,
            operation_name="analise operacional de tarefas",
            operation_result={"ok": True, "analysis_text": text},
            mode="task_analysis",
            object_type="tasks",
        )
        return text, False
    if _matches_meeting_operational_analysis_request(user_text):
        text = _execute_meeting_operational_analysis(api_key, model, access_token, user_text)
        _remember_structured_execution(
            user_text,
            operation_name="analise operacional de reunioes",
            operation_result={"ok": True, "analysis_text": text},
            mode="meeting_analysis",
            object_type="meetings",
        )
        return text, False
    capability_context = _build_capability_context(access_token, user_text)
    autonomous_text = _try_execute_autonomous_plan(
        api_key,
        model=model,
        access_token=access_token,
        user_text=user_text,
        capability_context=capability_context,
    )
    if autonomous_text:
        return autonomous_text, False
    return None, False


def _try_execute_compound_operational_plan(
    api_key: str,
    *,
    model: str,
    access_token: str,
    user_text: str,
) -> Optional[str]:
    clauses = _split_operational_request(user_text)
    if len(clauses) < 2:
        return None
    _status(f"Pedido composto detectado. Vou dividir em {len(clauses)} etapas.")
    executed_steps: list[dict[str, Any]] = []
    failed_clauses: list[str] = []
    for clause in clauses:
        clause_context = _build_capability_context(access_token, clause)
        execution = _execute_autonomous_plan_raw(
            api_key,
            model=model,
            access_token=access_token,
            user_text=clause,
            capability_context=clause_context,
        )
        if not execution:
            failed_clauses.append(clause)
            continue
        executed_steps.append(
            {
                "request": clause,
                "plan": execution["plan"],
                "result": execution["result"],
            }
        )
    if not executed_steps:
        return None
    final_result = {
        "ok": len(failed_clauses) == 0 and all(bool((step.get("result") or {}).get("ok", True)) and not (step.get("result") or {}).get("errors") for step in executed_steps),
        "executed_steps": executed_steps,
        "failed_to_plan": failed_clauses,
    }
    text = anthropic_present_operation_result(
        api_key,
        model=model,
        user_text=user_text,
        operation_name="execucao composta de multiplas acoes no HubSpot",
        operation_result=final_result,
    )
    _remember_structured_execution(
        user_text,
        operation_name="execucao composta de multiplas acoes no HubSpot",
        operation_result=final_result,
        mode="compound_operation",
        constraints=["preservar contexto compartilhado entre etapas compostas"],
    )
    return text


def _resolve_owner_id_by_email(access_token: str, email: str) -> str:
    candidates = [
        ("/crm/v3/owners/", {"email": email}),
        ("/crm/v3/owners", {"email": email}),
    ]
    for endpoint, params in candidates:
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="GET", params=params)
        if not res.get("ok"):
            continue
        data = res.get("response_json") or {}
        if isinstance(data, dict) and data.get("id") is not None:
            return str(data.get("id"))
        results = data.get("results") or []
        for row in results:
            candidate_email = str((row or {}).get("email") or "").lower()
            if candidate_email == email.lower():
                owner_id = (row or {}).get("id") or (row or {}).get("userId")
                if owner_id is not None:
                    return str(owner_id)
    raise RuntimeError(f"Nao encontrei owner no HubSpot para o email {email}.")


def _execute_contact_owner_assignment(access_token: str, owner_email: str) -> str:
    _status(f"Buscando owner pelo email {owner_email}...")
    owner_id = _resolve_owner_id_by_email(access_token, owner_email)
    _status(f"Owner encontrado com id {owner_id}. Buscando contatos sem proprietario...")
    contacts = _search_objects(
        access_token,
        object_type="contacts",
        properties=["firstname", "lastname", "email", "hubspot_owner_id", "createdate"],
        filter_groups=[
            {
                "filters": [
                    {
                        "propertyName": "hubspot_owner_id",
                        "operator": "NOT_HAS_PROPERTY",
                    }
                ]
            }
        ],
        sorts=[{"propertyName": "createdate", "direction": "DESCENDING"}],
        per_page=200,
        max_records=5000,
    )
    contact_ids = [str((c or {}).get("id")) for c in contacts if (c or {}).get("id") is not None]
    if not contact_ids:
        return "Nao encontrei contatos sem proprietario."
    result = hubspot_bulk_update_tool(
        access_token,
        object_type="contacts",
        record_ids=contact_ids,
        properties_to_update={"hubspot_owner_id": owner_id},
    )
    sample_lines: list[str] = []
    for contact in contacts[:10]:
        props = contact.get("properties") or {}
        sample_lines.append(
            f"- {_make_contact_full_name(props)} ({_safe_prop(props, 'email')}) -> contato {contact.get('id')}"
        )
    return (
        f"Atualizei {result.get('updated', 0)} contatos sem proprietario para o owner {owner_email} (id {owner_id}).\n\n"
        "Amostra dos contatos atualizados:\n"
        + "\n".join(sample_lines)
    )


def _build_large_volume_lead_dataset(access_token: str, user_text: str) -> dict[str, Any]:
    start_ms, end_ms, period_label = _build_period_range_from_text(user_text)
    cfg = _object_config("leads")
    created_prop = str(cfg.get("date_property"))
    properties = list(cfg.get("default_properties") or [])
    leads = _search_objects(
        access_token,
        object_type="leads",
        properties=properties,
        filter_groups=[
            {
                "filters": [
                    {
                        "propertyName": created_prop,
                        "operator": "BETWEEN",
                        "value": str(start_ms),
                        "highValue": str(end_ms),
                    }
                ]
            }
        ],
        sorts=[{"propertyName": created_prop, "direction": "DESCENDING"}],
        per_page=200,
        max_records=5000,
    )
    lead_ids = [str((lead or {}).get("id")) for lead in leads if (lead or {}).get("id") is not None]
    contact_assoc = _batch_read_associations(
        access_token,
        from_object_type="leads",
        to_object_type="contacts",
        record_ids=lead_ids,
    ) if lead_ids else {}
    deal_assoc = _batch_read_associations(
        access_token,
        from_object_type="leads",
        to_object_type="deals",
        record_ids=lead_ids,
    ) if lead_ids else {}

    by_label: Counter[str] = Counter()
    by_pipeline: Counter[str] = Counter()
    by_stage: Counter[str] = Counter()
    by_owner: Counter[str] = Counter()
    without_contact = 0
    without_deal = 0
    stale_without_contact = 0
    samples: list[dict[str, Any]] = []
    no_contact_samples: list[dict[str, Any]] = []
    no_deal_samples: list[dict[str, Any]] = []
    stale_no_contact_samples: list[dict[str, Any]] = []
    day_count = _extract_day_count(user_text or "")
    stale_cutoff = None
    if day_count is not None:
        stale_cutoff = _local_now().astimezone(timezone.utc) - timedelta(days=day_count)

    for lead in leads:
        lead_id = str(lead.get("id"))
        props = lead.get("properties") or {}
        label = _safe_prop(props, "hs_lead_label")
        pipeline = _safe_prop(props, "hs_pipeline")
        stage = _safe_prop(props, "hs_pipeline_stage")
        owner = _safe_prop(props, "hubspot_owner_id")
        name = _safe_prop(props, "hs_lead_name", default=f"Lead {lead_id}")
        created_at = _parse_hubspot_dt(_safe_prop(props, created_prop, default=""))

        by_label[label] += 1
        by_pipeline[pipeline] += 1
        by_stage[stage] += 1
        by_owner[owner] += 1

        contact_ids = contact_assoc.get(lead_id, [])
        deal_ids = deal_assoc.get(lead_id, [])
        if not contact_ids:
            without_contact += 1
            if len(no_contact_samples) < 10:
                no_contact_samples.append({"id": lead_id, "name": name, "stage": stage, "label": label})
            if stale_cutoff is not None and created_at is not None and created_at <= stale_cutoff:
                stale_without_contact += 1
                if len(stale_no_contact_samples) < 10:
                    stale_no_contact_samples.append(
                        {
                            "id": lead_id,
                            "name": name,
                            "stage": stage,
                            "label": label,
                            "created_at": created_at.isoformat(),
                        }
                    )
        if not deal_ids:
            without_deal += 1
            if len(no_deal_samples) < 10:
                no_deal_samples.append({"id": lead_id, "name": name, "stage": stage, "label": label})

        if len(samples) < 20:
            samples.append(
                {
                    "id": lead_id,
                    "name": name,
                    "label": label,
                    "pipeline": pipeline,
                    "stage": stage,
                    "owner": owner,
                    "contact_count": len(contact_ids),
                    "deal_count": len(deal_ids),
                }
            )

    return {
        "period_label": period_label,
        "total_leads": len(leads),
        "date_property_used": created_prop,
        "metadata_snapshot": _shared_metadata_snapshot(access_token),
        "with_contact": len(leads) - without_contact,
        "without_contact": without_contact,
        "stale_without_contact_days": day_count,
        "stale_without_contact": stale_without_contact,
        "with_deal": len(leads) - without_deal,
        "without_deal": without_deal,
        "top_labels": _top_counts(by_label),
        "top_pipelines": _top_counts(by_pipeline),
        "top_stages": _top_counts(by_stage),
        "top_owners": _top_counts(by_owner),
        "samples": samples,
        "samples_without_contact": no_contact_samples,
        "samples_stale_without_contact": stale_no_contact_samples,
        "samples_without_deal": no_deal_samples,
    }


def _build_deal_operational_dataset(access_token: str, user_text: str) -> dict[str, Any]:
    start_ms, end_ms, period_label = _build_period_range_from_text(user_text)
    cfg = _object_config("deals")
    created_prop = str(cfg.get("date_property"))
    deals = _search_objects(
        access_token,
        object_type="deals",
        properties=list(cfg.get("default_properties") or []),
        filter_groups=[
            {
                "filters": [
                    {
                        "propertyName": created_prop,
                        "operator": "BETWEEN",
                        "value": str(start_ms),
                        "highValue": str(end_ms),
                    }
                ]
            }
        ],
        sorts=[{"propertyName": created_prop, "direction": "DESCENDING"}],
        per_page=200,
        max_records=5000,
    )
    deal_ids = [str((deal or {}).get("id")) for deal in deals if (deal or {}).get("id") is not None]
    task_assoc = _batch_read_associations(access_token, from_object_type="deals", to_object_type="tasks", record_ids=deal_ids) if deal_ids else {}
    meeting_assoc = _batch_read_associations(access_token, from_object_type="deals", to_object_type="meetings", record_ids=deal_ids) if deal_ids else {}

    by_pipeline: Counter[str] = Counter()
    by_stage: Counter[str] = Counter()
    by_owner: Counter[str] = Counter()
    without_task = 0
    without_meeting = 0
    no_task_samples: list[dict[str, Any]] = []
    no_meeting_samples: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []

    for deal in deals:
        deal_id = str(deal.get("id"))
        props = deal.get("properties") or {}
        name = _safe_prop(props, "dealname", default=f"Deal {deal_id}")
        pipeline = _safe_prop(props, "pipeline")
        stage = _safe_prop(props, "dealstage")
        owner = _safe_prop(props, "hubspot_owner_id")

        by_pipeline[pipeline] += 1
        by_stage[stage] += 1
        by_owner[owner] += 1

        task_ids = task_assoc.get(deal_id, [])
        meeting_ids = meeting_assoc.get(deal_id, [])
        if not task_ids:
            without_task += 1
            if len(no_task_samples) < 10:
                no_task_samples.append({"id": deal_id, "name": name, "stage": stage, "pipeline": pipeline})
        if not meeting_ids:
            without_meeting += 1
            if len(no_meeting_samples) < 10:
                no_meeting_samples.append({"id": deal_id, "name": name, "stage": stage, "pipeline": pipeline})
        if len(samples) < 20:
            samples.append(
                {
                    "id": deal_id,
                    "name": name,
                    "pipeline": pipeline,
                    "stage": stage,
                    "owner": owner,
                    "task_count": len(task_ids),
                    "meeting_count": len(meeting_ids),
                    "amount": _safe_prop(props, "amount"),
                }
            )

    return {
        "period_label": period_label,
        "total_deals": len(deals),
        "date_property_used": created_prop,
        "metadata_snapshot": _shared_metadata_snapshot(access_token),
        "without_task": without_task,
        "with_task": len(deals) - without_task,
        "without_meeting": without_meeting,
        "with_meeting": len(deals) - without_meeting,
        "top_pipelines": _top_counts(by_pipeline),
        "top_stages": _top_counts(by_stage),
        "top_owners": _top_counts(by_owner),
        "samples": samples,
        "samples_without_task": no_task_samples,
        "samples_without_meeting": no_meeting_samples,
    }


def _build_task_operational_dataset(access_token: str, user_text: str) -> dict[str, Any]:
    start_ms, end_ms, period_label = _build_period_range_from_text(user_text)
    cfg = _object_config("tasks")
    date_prop = str(cfg.get("date_property"))
    tasks = _search_objects(
        access_token,
        object_type="tasks",
        properties=list(cfg.get("default_properties") or []),
        filter_groups=[
            {
                "filters": [
                    {
                        "propertyName": date_prop,
                        "operator": "BETWEEN",
                        "value": str(start_ms),
                        "highValue": str(end_ms),
                    }
                ]
            }
        ],
        sorts=[{"propertyName": date_prop, "direction": "DESCENDING"}],
        per_page=200,
        max_records=5000,
    )
    by_status: Counter[str] = Counter()
    by_priority: Counter[str] = Counter()
    by_owner: Counter[str] = Counter()
    overdue_open = 0
    open_samples: list[dict[str, Any]] = []
    overdue_samples: list[dict[str, Any]] = []
    now_utc = _local_now().astimezone(timezone.utc)

    for task in tasks:
        task_id = str(task.get("id"))
        props = task.get("properties") or {}
        subject = _safe_prop(props, "hs_task_subject", default=f"Task {task_id}")
        status = _safe_prop(props, "hs_task_status")
        priority = _safe_prop(props, "hs_task_priority")
        owner = _safe_prop(props, "hubspot_owner_id")
        ts = _parse_hubspot_dt(_safe_prop(props, date_prop, default=""))

        by_status[status] += 1
        by_priority[priority] += 1
        by_owner[owner] += 1

        is_open = status not in {"COMPLETED", "CANCELED"}
        if is_open and len(open_samples) < 20:
            open_samples.append({"id": task_id, "subject": subject, "status": status, "priority": priority, "owner": owner})
        if is_open and ts is not None and ts < now_utc:
            overdue_open += 1
            if len(overdue_samples) < 10:
                overdue_samples.append(
                    {"id": task_id, "subject": subject, "status": status, "priority": priority, "owner": owner, "scheduled_for": ts.isoformat()}
                )

    return {
        "period_label": period_label,
        "total_tasks": len(tasks),
        "date_property_used": date_prop,
        "metadata_snapshot": _shared_metadata_snapshot(access_token),
        "overdue_open_tasks": overdue_open,
        "top_statuses": _top_counts(by_status),
        "top_priorities": _top_counts(by_priority),
        "top_owners": _top_counts(by_owner),
        "open_task_samples": open_samples,
        "overdue_task_samples": overdue_samples,
    }


def _build_meeting_operational_dataset(access_token: str, user_text: str) -> dict[str, Any]:
    start_ms, end_ms, period_label = _build_period_range_from_text(user_text)
    cfg = _object_config("meetings")
    date_prop = str(cfg.get("date_property"))
    meetings = _search_objects(
        access_token,
        object_type="meetings",
        properties=list(cfg.get("default_properties") or []),
        filter_groups=[
            {
                "filters": [
                    {
                        "propertyName": date_prop,
                        "operator": "BETWEEN",
                        "value": str(start_ms),
                        "highValue": str(end_ms),
                    }
                ]
            }
        ],
        sorts=[{"propertyName": date_prop, "direction": "DESCENDING"}],
        per_page=200,
        max_records=5000,
    )
    by_owner: Counter[str] = Counter()
    past_count = 0
    future_count = 0
    samples: list[dict[str, Any]] = []
    now_utc = _local_now().astimezone(timezone.utc)

    for meeting in meetings:
        meeting_id = str(meeting.get("id"))
        props = meeting.get("properties") or {}
        title = _safe_prop(props, "hs_meeting_title", default=f"Meeting {meeting_id}")
        owner = _safe_prop(props, "hubspot_owner_id")
        ts = _parse_hubspot_dt(_safe_prop(props, date_prop, default=""))
        by_owner[owner] += 1
        if ts is not None and ts >= now_utc:
            future_count += 1
        else:
            past_count += 1
        if len(samples) < 20:
            samples.append({"id": meeting_id, "title": title, "owner": owner, "scheduled_for": ts.isoformat() if ts else "(sem data)"})

    return {
        "period_label": period_label,
        "total_meetings": len(meetings),
        "date_property_used": date_prop,
        "metadata_snapshot": _shared_metadata_snapshot(access_token),
        "future_meetings": future_count,
        "past_meetings": past_count,
        "top_owners": _top_counts(by_owner),
        "samples": samples,
    }


def anthropic_analyze_large_dataset(api_key: str, *, model: str, user_text: str, dataset: dict[str, Any]) -> str:
    prompt = (
        "Voce e a Livia analisando um volume grande de leads do HubSpot.\n"
        "Os dados abaixo ja foram coletados e resumidos pelo sistema. Nao invente numeros, nao peca mais consultas, "
        "nao mencione limitacoes de API, e nao tente usar ferramentas.\n"
        "Entregue uma analise objetiva em pt-BR com:\n"
        "1. resumo executivo curto\n"
        "2. principais padroes encontrados\n"
        "3. riscos/oportunidades operacionais\n"
        "4. acoes recomendadas\n\n"
        f"Pedido do usuario:\n{user_text}\n\n"
        f"Dataset resumido:\n{json.dumps(dataset, ensure_ascii=False)}"
    )
    url = f"{ANTHROPIC_BASE}/v1/messages"
    body = {
        "model": model,
        "max_tokens": 1500,
        "system": "Responda de forma objetiva, analitica e operacional. Use somente os dados fornecidos.",
        "messages": [{"role": "user", "content": prompt}],
    }
    base_sleep = 1.5
    last_error: Optional[str] = None
    for i in range(6):
        attempt = i + 1
        _status(f"Analisando resumo grande de leads... tentativa {attempt}/6.")
        try:
            resp = _ANTHROPIC_SESSION.post(url, headers=_anthropic_headers(api_key), json=body, timeout=(10, 45))
        except KeyboardInterrupt:
            raise
        except (Timeout, RequestException):
            sleep_s = min(15.0, max(0.5, base_sleep * (2**min(i, 6))))
            last_error = f"Falha de rede/timeout na analise. Nova tentativa em {sleep_s:.1f}s."
            _status(last_error, level="warning")
            time.sleep(sleep_s)
            continue
        if resp.status_code < 400:
            content = (resp.json().get("content") or [])
            return _extract_text(content)
        if resp.status_code in (429, 529):
            sleep_s = min(15.0, max(0.25, base_sleep * (2**min(i, 6))))
            last_error = f"Anthropic ocupado na analise (HTTP {resp.status_code}). Nova tentativa em {sleep_s:.1f}s."
            _status(last_error, level="warning")
            time.sleep(sleep_s)
            continue
        raise RuntimeError(f"Anthropic HTTP {resp.status_code}: {resp.text[:2000]}")
    raise RuntimeError(last_error or "Anthropic indisponivel para analisar o dataset.")


def anthropic_present_operation_result(
    api_key: str,
    *,
    model: str,
    user_text: str,
    operation_name: str,
    operation_result: Any,
) -> str:
    prompt = (
        "Voce e a Livia, agente do usuario.\n"
        "O sistema Python executou uma operacao no HubSpot e te entregou o resultado bruto abaixo.\n"
        "Responda sempre como agente, em pt-BR, de forma objetiva e natural.\n"
        "Nao diga que foi o Python quem respondeu. Nao exponha detalhes tecnicos desnecessarios.\n"
        "Se houve sucesso, explique o que foi feito e o resultado.\n"
        "Se houve erro, explique o problema de forma clara e orientada a acao.\n\n"
        f"Pedido original do usuario:\n{user_text}\n\n"
        f"Operacao executada:\n{operation_name}\n\n"
        f"Resultado bruto:\n{json.dumps(operation_result, ensure_ascii=False)}"
    )
    url = f"{ANTHROPIC_BASE}/v1/messages"
    body = {
        "model": model,
        "max_tokens": 1200,
        "system": "Voce e a Livia. Toda resposta ao usuario final deve vir de voce, como agente.",
        "messages": [{"role": "user", "content": prompt}],
    }
    base_sleep = 1.5
    last_error: Optional[str] = None
    for i in range(6):
        attempt = i + 1
        _status(f"Redigindo resposta final da agente... tentativa {attempt}/6.")
        try:
            resp = _ANTHROPIC_SESSION.post(url, headers=_anthropic_headers(api_key), json=body, timeout=(10, 45))
        except KeyboardInterrupt:
            raise
        except (Timeout, RequestException):
            sleep_s = min(15.0, max(0.5, base_sleep * (2**min(i, 6))))
            last_error = f"Falha de rede/timeout ao redigir resposta. Nova tentativa em {sleep_s:.1f}s."
            _status(last_error, level="warning")
            time.sleep(sleep_s)
            continue
        if resp.status_code < 400:
            return _extract_text((resp.json().get("content") or []))
        if resp.status_code in (429, 529):
            sleep_s = min(15.0, max(0.25, base_sleep * (2**min(i, 6))))
            last_error = f"Anthropic ocupada ao redigir resposta (HTTP {resp.status_code}). Nova tentativa em {sleep_s:.1f}s."
            _status(last_error, level="warning")
            time.sleep(sleep_s)
            continue
        raise RuntimeError(f"Anthropic HTTP {resp.status_code}: {resp.text[:2000]}")
    raise RuntimeError(last_error or "Anthropic indisponivel para redigir a resposta final.")


def _execute_large_volume_lead_analysis(api_key: str, model: str, access_token: str, user_text: str) -> str:
    _status("Fluxo de analise de alto volume ativado para leads.")
    dataset = _build_large_volume_lead_dataset(access_token, user_text)
    if not dataset.get("total_leads"):
        return f"Nao encontrei leads no periodo {dataset.get('period_label') or 'informado'}."
    return anthropic_analyze_large_dataset(api_key, model=model, user_text=user_text, dataset=dataset)


def _execute_deal_operational_analysis(api_key: str, model: str, access_token: str, user_text: str) -> str:
    _status("Fluxo de analise operacional ativado para negocios.")
    dataset = _build_deal_operational_dataset(access_token, user_text)
    if not dataset.get("total_deals"):
        return f"Nao encontrei negocios no periodo {dataset.get('period_label') or 'informado'}."
    return anthropic_analyze_large_dataset(api_key, model=model, user_text=user_text, dataset=dataset)


def _execute_task_operational_analysis(api_key: str, model: str, access_token: str, user_text: str) -> str:
    _status("Fluxo de analise operacional ativado para tarefas.")
    dataset = _build_task_operational_dataset(access_token, user_text)
    if not dataset.get("total_tasks"):
        return f"Nao encontrei tarefas no periodo {dataset.get('period_label') or 'informado'}."
    return anthropic_analyze_large_dataset(api_key, model=model, user_text=user_text, dataset=dataset)


def _execute_meeting_operational_analysis(api_key: str, model: str, access_token: str, user_text: str) -> str:
    _status("Fluxo de analise operacional ativado para reunioes.")
    dataset = _build_meeting_operational_dataset(access_token, user_text)
    if not dataset.get("total_meetings"):
        return f"Nao encontrei reunioes no periodo {dataset.get('period_label') or 'informado'}."
    return anthropic_analyze_large_dataset(api_key, model=model, user_text=user_text, dataset=dataset)



def _create_lead_for_contact(access_token: str, contact: dict[str, Any], primary_assoc_type_id: int) -> str:
    props = contact.get("properties") or {}
    lead_name = _make_contact_full_name(props)
    contact_id = str(contact.get("id"))
    res = hubspot_universal_api_call(
        access_token,
        endpoint="/crm/v3/objects/leads",
        method="POST",
        json_payload={
            "properties": {"hs_lead_name": lead_name},
            "associations": [
                {
                    "to": {"id": contact_id},
                    "types": [
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": primary_assoc_type_id,
                        }
                    ],
                }
            ],
        },
    )
    if not res.get("ok"):
        raise RuntimeError(f"Falha ao criar lead '{lead_name}': HTTP {res.get('status_code')} - {res.get('response_text')}")
    lead_id = ((res.get("response_json") or {}).get("id"))
    if not lead_id:
        raise RuntimeError(f"Resposta sem ID ao criar lead '{lead_name}'.")
    return str(lead_id)


def _batch_associate_leads_to_contacts(access_token: str, pairs: list[dict[str, str]]) -> None:
    endpoint = "/crm/v4/associations/leads/contacts/batch/associate/default"
    for idx, chunk in enumerate(_chunked([json.dumps(p, ensure_ascii=False) for p in pairs], 100), start=1):
        _status(f"Associando lote {idx} de leads aos contatos...")
        inputs = [json.loads(item) for item in chunk]
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="POST", json_payload={"inputs": inputs})
        if not res.get("ok"):
            raise RuntimeError(f"Falha ao associar leads: HTTP {res.get('status_code')} - {res.get('response_text')}")


def _execute_direct_contact_lead_flow(access_token: str, user_text: str) -> str:
    now = _local_now()
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_yesterday = start_today - timedelta(days=1)

    text = (user_text or "").lower()
    if "ontem" in text and "hoje" in text:
        start = start_yesterday
        end = now
        period_label = "ontem e hoje"
    elif "ontem" in text:
        start = start_yesterday
        end = start_today
        period_label = "ontem"
    else:
        start = start_today
        end = now
        period_label = "hoje"

    start_ms = int(start.astimezone(timezone.utc).timestamp() * 1000)
    end_ms = int(end.astimezone(timezone.utc).timestamp() * 1000)

    _status(f"Fluxo direto ativado para contatos de {period_label}.")
    contacts = _search_contacts_in_period(access_token, start_ms, end_ms)
    if not contacts:
        return f"Nao encontrei contatos criados em {period_label}."
    contacts = _filter_contacts_by_requested_names(contacts, user_text)

    contact_ids = [str((c or {}).get("id")) for c in contacts if (c or {}).get("id") is not None]
    assoc_map = _read_contact_lead_associations(access_token, contact_ids)
    contacts_without_lead = [c for c in contacts if not assoc_map.get(str(c.get("id")), [])]

    if not contacts_without_lead:
        return f"Encontrei {len(contacts)} contatos em {period_label} e todos ja possuem lead associado."

    created_lines: list[str] = []
    created_lead_ids: list[str] = []
    total_missing = len(contacts_without_lead)
    primary_assoc_type_id = _get_primary_lead_contact_association_type_id(access_token)
    for idx, contact in enumerate(contacts_without_lead, start=1):
        contact_id = str(contact.get("id"))
        contact_name = _make_contact_full_name(contact.get("properties") or {})
        _status(f"Criando lead {idx}/{total_missing} para o contato {contact_name}...")
        lead_id = _create_lead_for_contact(access_token, contact, primary_assoc_type_id)
        created_lead_ids.append(lead_id)
        created_lines.append(f"- {contact_name}: lead {lead_id} associado ao contato {contact_id}")

    stage_update_summary = ""
    stage_props = _requested_lead_stage_update(access_token, user_text)
    if stage_props and created_lead_ids:
        _status(f"Movendo {len(created_lead_ids)} leads criados para a etapa solicitada...")
        move_result = hubspot_bulk_update_tool(
            access_token,
            object_type="leads",
            record_ids=created_lead_ids,
            properties_to_update=stage_props,
        )
        if move_result.get("ok"):
            stage_update_summary = f"\n\nTambem movi os {len(created_lead_ids)} leads criados para a etapa solicitada."
        else:
            stage_update_summary = "\n\nOs leads foram criados, mas nao consegui mover a etapa automaticamente."

    return (
        f"Encontrei {len(contacts)} contatos em {period_label}. "
        f"{len(contacts_without_lead)} estavam sem lead e foram processados com sucesso.\n\n"
        + "\n".join(created_lines)
        + stage_update_summary
    )


def _matches_direct_contact_list_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    if "contato" not in t and "contatos" not in t:
        return False
    wants_list = any(word in t for word in ["liste", "lista", "relacao", "relação", "todos", "todas", "me de", "me dê", "mostre"])
    has_period = any(tok in t for tok in ["ontem", "hoje", "essa semana", "ultima semana", "última semana", "ultimos 7 dias", "últimos 7 dias"])
    return wants_list and has_period and ("lead" not in t)  # evita conflitar com o fluxo de contatos->leads


def _matches_direct_lead_date_search_request(user_text: str) -> bool:
    t = _normalize_text(user_text)
    if "lead" not in t and "leads" not in t:
        return False
    # só dispara quando há data explícita (ex.: 13/04/2024) para evitar conflitos com outros fluxos
    return _parse_explicit_date_range(user_text) is not None and any(
        w in t for w in ["criado", "criados", "criada", "criadas", "no dia", "na data", "em", "dia"]
    )


def _execute_direct_lead_date_search_flow(access_token: str, user_text: str) -> dict[str, Any]:
    parsed = _parse_explicit_date_range(user_text)
    if not parsed:
        return {"ok": False, "message": "Nao encontrei uma data valida no formato DD/MM/AAAA no seu pedido."}
    start_ms, end_ms, label = parsed
    date_prop = _resolve_best_date_property(access_token, "leads")
    # busca ampla, mas com cap seguro
    results = _search_objects(
        access_token,
        object_type="leads",
        properties=["hs_lead_name", date_prop, "hs_pipeline", "hs_pipeline_stage", "hubspot_owner_id"],
        filter_groups=[
            {
                "filters": [
                    {
                        "propertyName": date_prop,
                        "operator": "BETWEEN",
                        "value": str(start_ms),
                        "highValue": str(end_ms),
                    }
                ]
            }
        ],
        sorts=[f"-{date_prop}"],
        per_page=50,
        max_records=200,
    )
    items: list[dict[str, Any]] = []
    for row in results[:80]:
        props = (row or {}).get("properties") or {}
        items.append(
            {
                "id": str((row or {}).get("id") or ""),
                "name": str(props.get("hs_lead_name") or ""),
                "createdate": str(props.get(date_prop) or ""),
                "pipeline": str(props.get("hs_pipeline") or ""),
                "stage": str(props.get("hs_pipeline_stage") or ""),
                "owner_id": str(props.get("hubspot_owner_id") or ""),
            }
        )
    return {
        "ok": True,
        "object_type": "leads",
        "date_property": date_prop,
        "period": label,
        "total_found": len(results),
        "items": items,
        "note": "Busca deterministica por data explicita (sem planner/LLM).",
    }


def _execute_direct_contact_list_flow(access_token: str, user_text: str) -> dict[str, Any]:
    now = _local_now()
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_yesterday = start_today - timedelta(days=1)
    start_week = start_today - timedelta(days=now.weekday())
    start_last7 = start_today - timedelta(days=7)

    text = _normalize_text(user_text)
    if "ontem" in text:
        start = start_yesterday
        end = start_today
        period_label = "ontem"
    elif "essa semana" in text:
        start = start_week
        end = now
        period_label = "essa semana"
    elif "ultima semana" in text or "última semana" in text or "ultimos 7 dias" in text or "últimos 7 dias" in text:
        start = start_last7
        end = now
        period_label = "últimos 7 dias"
    else:
        start = start_today
        end = now
        period_label = "hoje"

    start_ms = int(start.astimezone(timezone.utc).timestamp() * 1000)
    end_ms = int(end.astimezone(timezone.utc).timestamp() * 1000)

    contacts = _search_contacts_in_period(access_token, start_ms, end_ms)
    contacts = _filter_contacts_by_requested_names(contacts, user_text)
    # para resposta humana: cap em 50 itens
    items = []
    for row in contacts[:50]:
        props = (row or {}).get("properties") or {}
        items.append(
            {
                "id": str((row or {}).get("id") or ""),
                "name": _make_contact_full_name(props),
                "email": str(props.get("email") or ""),
                "createdate": str(props.get("createdate") or ""),
            }
        )
    return {"ok": True, "period": period_label, "total_found": len(contacts), "items": items}


def hubspot_bulk_update_tool(  # [CORRIGIDO] CRÍTICO 1 — propaga new_access_token dos lotes
    access_token: str,
    *,
    object_type: str,
    record_ids: list[str],
    properties_to_update: dict[str, Any],
) -> dict[str, Any]:
    """
    Monta o payload /crm/v3/objects/{object_type}/batch/update e envia em chunks de 100.
    Retorna resumo + detalhes por lote (com erros quando existirem).
    """
    obj = (object_type or "").strip().lower()
    if not obj:
        return {"ok": False, "error": "object_type vazio"}

    ids = [str(x).strip() for x in (record_ids or []) if str(x).strip()]
    # dedupe preservando ordem
    seen: set[str] = set()
    deduped: list[str] = []
    for rid in ids:
        if rid not in seen:
            seen.add(rid)
            deduped.append(rid)
    ids = deduped

    if not ids:
        return {"ok": True, "updated": 0, "skipped": 0, "batches": 0, "details": [], "message": "Nenhum ID fornecido."}

    props = properties_to_update or {}
    if not isinstance(props, dict) or not props:
        return {"ok": False, "error": "properties_to_update precisa ser um dict não-vazio."}

    endpoint = f"/crm/v3/objects/{obj}/batch/update"

    details: list[dict[str, Any]] = []
    updated_count = 0
    error_count = 0
    tok = access_token
    merged: dict[str, Any] = {}

    for batch_idx, batch_ids in enumerate(_chunked(ids, 100), start=1):
        _status(f"Atualizando lote {batch_idx} com {len(batch_ids)} registros em {obj}...")
        payload = {"inputs": [{"id": rid, "properties": props} for rid in batch_ids]}
        res = hubspot_universal_api_call(tok, endpoint=endpoint, method="POST", json_payload=payload)
        _hubspot_merge_new_token(merged, res)
        if res.get("new_access_token"):
            tok = str(res["new_access_token"])

        batch_ok = bool(res.get("ok"))
        if batch_ok:
            updated_count += len(batch_ids)
        else:
            error_count += len(batch_ids)

        details.append(
            {
                "batch_index": batch_idx,
                "batch_size": len(batch_ids),
                "ok": batch_ok,
                "status_code": res.get("status_code"),
                "response_text": res.get("response_text"),
                "response_json": res.get("response_json"),
            }
        )

    ok = error_count == 0
    out: dict[str, Any] = {
        "ok": ok,
        "object_type": obj,
        "endpoint": endpoint,
        "attempted": len(ids),
        "updated": updated_count,
        "failed": error_count,
        "batches": len(details),
        "properties_to_update": props,
        "details": details if error_count else details[:1],  # não explodir contexto quando tudo ok
        "message": (
            f"{updated_count} registros atualizados com sucesso."
            if ok
            else f"{updated_count} atualizados, {error_count} falharam. Veja details."
        ),
    }
    if merged.get("new_access_token"):
        out["new_access_token"] = merged["new_access_token"]
    return out


def hubspot_bulk_update_by_search_tool(  # [CORRIGIDO] CRÍTICO 1 — propaga new_access_token (busca + update)
    access_token: str,
    *,
    object_type: str,
    search_body: dict[str, Any],
    properties_to_update: dict[str, Any],
    max_records: Optional[int] = None,
) -> dict[str, Any]:
    """
    Faz search paginado para coletar IDs e depois atualiza em lote (100).
    O LLM não precisa listar IDs manualmente.
    """
    obj = (object_type or "").strip().lower()
    if not obj:
        return {"ok": False, "error": "object_type vazio"}

    props = properties_to_update or {}
    if not isinstance(props, dict) or not props:
        return {"ok": False, "error": "properties_to_update precisa ser um dict não-vazio."}

    body = dict(search_body or {})
    # Garantir limites razoáveis
    max_n = int(max_records) if isinstance(max_records, int) and max_records > 0 else 500
    per_page = int(body.get("limit") or 100)
    per_page = max(1, min(200, per_page))
    body["limit"] = per_page

    collected: list[str] = []
    after: Optional[str] = body.get("after")
    endpoint_search = f"/crm/v3/objects/{obj}/search"
    page_idx = 0
    tok = access_token
    merged: dict[str, Any] = {}

    while True:
        page_idx += 1
        if after is not None:
            body["after"] = after
        elif "after" in body:
            body.pop("after", None)

        _status(f"Buscando pagina {page_idx} de {obj} no HubSpot...")
        res = hubspot_universal_api_call(tok, endpoint=endpoint_search, method="POST", json_payload=body)
        _hubspot_merge_new_token(merged, res)
        if res.get("new_access_token"):
            tok = str(res["new_access_token"])
        if not res.get("ok"):
            fail: dict[str, Any] = {
                "ok": False,
                "phase": "search",
                "object_type": obj,
                "endpoint": endpoint_search,
                "status_code": res.get("status_code"),
                "response_text": res.get("response_text"),
                "response_json": res.get("response_json"),
            }
            if merged.get("new_access_token"):
                fail["new_access_token"] = merged["new_access_token"]
            return fail

        rj = res.get("response_json") or {}
        results = rj.get("results") or []
        for it in results:
            rid = (it or {}).get("id")
            if rid is not None:
                collected.append(str(rid))
                if len(collected) >= max_n:
                    break

        if len(collected) >= max_n:
            break

        paging = rj.get("paging") or {}
        nxt = (paging.get("next") or {}).get("after")
        if not nxt:
            break
        after = str(nxt)

    _status(f"Busca concluida: {len(collected)} registros encontrados para atualizar.")
    # Atualiza em lote com a ferramenta nativa
    upd = hubspot_bulk_update_tool(tok, object_type=obj, record_ids=collected, properties_to_update=props)
    _hubspot_merge_new_token(merged, upd)
    upd["search_collected"] = len(collected)
    upd["max_records"] = max_n
    if merged.get("new_access_token"):
        upd["new_access_token"] = merged["new_access_token"]
    return upd


def hubspot_create_and_associate_lead_tool(  # [CORRIGIDO] CRÍTICO 1 — token após GET de labels + POST
    access_token: str,
    *,
    contact_id: str,
    lead_name: str,
    pipeline_stage: Optional[str] = None,
) -> dict[str, Any]:
    try:
        cid = str(contact_id or "").strip()
        if not cid:
            return {"ok": False, "error": "contact_id vazio"}
        lname = str(lead_name or "").strip()
        if not lname:
            return {"ok": False, "error": "lead_name vazio"}
        assoc_id = _get_primary_lead_contact_association_type_id(access_token)
        tok = _HUBSPOT_ACCESS_TOKEN or access_token
        properties: dict[str, Any] = {"hs_lead_name": lname}
        if pipeline_stage:
            properties["hs_pipeline_stage"] = str(pipeline_stage)
        payload = {
            "properties": properties,
            "associations": [
                {
                    "to": {"id": cid},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": assoc_id}],
                }
            ],
        }
        res = hubspot_universal_api_call(tok, endpoint="/crm/v3/objects/leads", method="POST", json_payload=payload)
        if res.get("ok"):
            lead_id = (res.get("response_json") or {}).get("id")
            out: dict[str, Any] = {
                "ok": True,
                "lead_id": str(lead_id) if lead_id is not None else None,
                "message": f"Lead '{lname}' (ID: {lead_id}) criado e associado ao contato {cid} com sucesso.",
            }
            _hubspot_merge_new_token(out, res)
            return out
        out_fail: dict[str, Any] = {"ok": False, "error": res.get("response_text"), "status_code": res.get("status_code"), "response_json": res.get("response_json")}
        _hubspot_merge_new_token(out_fail, res)
        return out_fail
    except Exception as e:
        return {"ok": False, "error": str(e)}


def hubspot_create_note_for_contact_tool(  # [CORRIGIDO] CRÍTICO 1 — token após GET de labels + POST
    access_token: str,
    *,
    contact_id: str,
    note_body: str,
) -> dict[str, Any]:
    try:
        cid = str(contact_id or "").strip()
        body = str(note_body or "").strip()
        if not cid or not body:
            return {"ok": False, "error": "contact_id ou note_body vazio"}
        assoc_id = _get_note_to_contact_association_type_id(access_token)
        tok = _HUBSPOT_ACCESS_TOKEN or access_token
        safe_body = _html_escape_simple(body).replace("\r\n", "\n").replace("\r", "\n")
        safe_body_html = "<p>" + safe_body.replace("\n", "<br/>") + "</p>"
        ts_ms = int(time.time() * 1000)
        payload = {
            "properties": {
                "hs_note_body": safe_body_html,
                "hs_timestamp": str(ts_ms),
            },
            "associations": [
                {
                    "to": {"id": cid},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": assoc_id}],
                }
            ],
        }
        res = hubspot_universal_api_call(tok, endpoint="/crm/v3/objects/notes", method="POST", json_payload=payload)
        if not res.get("ok"):
            out_fail: dict[str, Any] = {
                "ok": False,
                "error": res.get("response_text"),
                "status_code": res.get("status_code"),
                "response_json": res.get("response_json"),
            }
            _hubspot_merge_new_token(out_fail, res)
            return out_fail
        note_id = (res.get("response_json") or {}).get("id")
        out_ok: dict[str, Any] = {
            "ok": True,
            "note_id": str(note_id) if note_id is not None else None,
            "contact_id": cid,
            "association_type_id": assoc_id,
            "message": f"Nota criada e associada ao contato {cid} com sucesso.",
        }
        _hubspot_merge_new_token(out_ok, res)
        return out_ok
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _anthropic_headers(api_key: str) -> dict[str, str]:
    return {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}


def _hubspot_headers(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def get_access_token_via_refresh() -> str:
    load_dotenv()
    client_id = os.getenv("HUBSPOT_CLIENT_ID")
    client_secret = os.getenv("HUBSPOT_CLIENT_SECRET")
    refresh_token = os.getenv("HUBSPOT_REFRESH_TOKEN")
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Faltam variáveis no .env: HUBSPOT_CLIENT_ID, HUBSPOT_CLIENT_SECRET, HUBSPOT_REFRESH_TOKEN.")
    url = f"{HUBSPOT_BASE}/oauth/v1/token"
    data = {"grant_type": "refresh_token", "client_id": client_id, "client_secret": client_secret, "refresh_token": refresh_token}
    resp = requests.post(url, data=data, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Resposta sem access_token.")
    return str(token)


def hubspot_universal_api_call(  # [CORRIGIDO] CRÍTICO 1 + CRÍTICO 2 — new_access_token + refresh não silencioso
    access_token: str,
    *,
    endpoint: str,
    method: str,
    params: Optional[dict[str, Any]] = None,
    json_payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    global _HUBSPOT_ACCESS_TOKEN
    method_u = (method or "").strip().upper()
    ep = (endpoint or "").strip()
    if not ep.startswith("/"):
        ep = "/" + ep
    url = f"{HUBSPOT_BASE}{ep}"

    tok = access_token or _HUBSPOT_ACCESS_TOKEN or ""
    refreshed = False
    first_status_code: Optional[int] = None
    refresh_error_msg = ""

    def _do_request(token_value: str) -> requests.Response:
        return requests.request(
            method_u,
            url,
            headers=_hubspot_headers(token_value),
            params=params or None,
            json=json_payload or None,
            timeout=60,
        )

    resp = _do_request(tok)
    first_status_code = int(resp.status_code)
    if resp.status_code == 401:
        # Expirou/revogou: renova e tenta 1 vez de forma transparente.
        try:
            tok = get_access_token_via_refresh()
            _HUBSPOT_ACCESS_TOKEN = tok
            refreshed = True
            resp = _do_request(tok)
        except Exception as refresh_err:
            refresh_error_msg = str(refresh_err)
    if first_status_code == 401 and not refreshed:
        return {
            "ok": False,
            "status_code": 401,
            "url": url,
            "method": method_u,
            "response_text": f"Token expirado e refresh falhou: {refresh_error_msg}",
            "response_json": None,
            "hubspot_token_refreshed": False,
            "params": params or None,
            "json_payload": json_payload or None,
        }
    text = resp.text or ""
    text_out = text[:20000] + ("...(truncado)" if len(text) > 20000 else "")
    try:
        json_out = resp.json()
    except Exception:
        json_out = None
    out: dict[str, Any] = {
        "ok": resp.status_code < 400,
        "status_code": resp.status_code,
        "url": url,
        "method": method_u,
        "params": params or None,
        "json_payload": json_payload or None,
        "hubspot_token_refreshed": refreshed,
        "response_headers": dict(resp.headers),
        "response_text": text_out,
        "response_json": json_out,
    }
    if refreshed:
        out["new_access_token"] = tok
    return out


def anthropic_list_models(api_key: str) -> list[str]:
    resp = requests.get(f"{ANTHROPIC_BASE}/v1/models", headers=_anthropic_headers(api_key), timeout=30)
    if resp.status_code >= 400:
        return []
    data = resp.json()
    return [str(m.get("id")) for m in (data.get("data") or []) if (m or {}).get("id")]


def pick_anthropic_model(api_key: str, preferred: Optional[str]) -> str:  # [CORRIGIDO] AVISO 1 — ordem de modelos + fallback
    if preferred:
        return preferred
    available = anthropic_list_models(api_key)
    if not available:
        warnings.warn("Nao foi possivel listar modelos Anthropic. Usando claude-sonnet-4-6 como fallback.")
        return "claude-sonnet-4-6"
    prefs = ["claude-sonnet-4-6", "claude-sonnet-4", "claude-haiku-4-5-20251001", "claude-haiku-4-5", "claude-opus-4-6"]
    s = set(available)
    for p in prefs:
        if p in s:
            return p
    return available[0]


def anthropic_messages(api_key: str, *, model: str, system: str, messages: list[dict[str, Any]]) -> dict[str, Any]:
    url = f"{ANTHROPIC_BASE}/v1/messages"
    body: dict[str, Any] = {
        "model": model,
        "max_tokens": 600,
        "system": system,
        "messages": messages,
        "tools": TOOLS,
        "tool_choice": {"type": "auto"},
    }
    base_sleep = 1.5
    max_attempts = 6
    last_error: Optional[str] = None
    for i in range(max_attempts):
        attempt = i + 1
        _status(f"Pensando... tentativa {attempt}/{max_attempts}.")
        try:
            # (connect timeout, read timeout)
            resp = _ANTHROPIC_SESSION.post(url, headers=_anthropic_headers(api_key), json=body, timeout=(10, 30))
        except KeyboardInterrupt:
            raise
        except (Timeout, RequestException):
            # Falha de rede/transiente: trate como overload e tente novamente.
            sleep_s = min(15.0, max(0.5, base_sleep * (2**min(i, 6))))
            last_error = f"Falha de rede/timeout ao consultar Anthropic. Nova tentativa em {sleep_s:.1f}s."
            _status(last_error, level="warning")
            try:
                time.sleep(sleep_s)
            except KeyboardInterrupt:
                raise
            continue
        if resp.status_code < 400:
            return resp.json()

        if resp.status_code in (429, 529):
            ra = resp.headers.get("retry-after")
            sleep_s = None
            if ra:
                try:
                    sleep_s = float(ra)
                except Exception:
                    sleep_s = None
            if sleep_s is None:
                sleep_s = base_sleep * (2**min(i, 6))
            sleep_s = min(15.0, max(0.25, float(sleep_s)))
            last_error = f"Anthropic ocupado (HTTP {resp.status_code}). Nova tentativa em {sleep_s:.1f}s."
            _status(last_error, level="warning")

            try:
                time.sleep(sleep_s)
            except KeyboardInterrupt:
                raise
            continue

        raise RuntimeError(f"Anthropic HTTP {resp.status_code}: {resp.text[:2000]}")
    raise RuntimeError(last_error or "Anthropic indisponivel apos varias tentativas.")


def _now_context_for_llm() -> str:
    """
    Fornece ao LLM referências de tempo para ele construir filtros createdate e timestamps sem pedir clarificação e sem errar a matemática.
    """
    from datetime import datetime, timedelta, timezone

    tz = None
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("America/Sao_Paulo")
    except Exception:
        tz = timezone.utc

    now = datetime.now(tz=tz)
    now_utc = now.astimezone(timezone.utc)
    now_ms = int(now_utc.timestamp() * 1000)

    start_of_week = (now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now.weekday()))
    sow_ms = int(start_of_week.astimezone(timezone.utc).timestamp() * 1000)

    last_7_days = now_utc - timedelta(days=7)
    last7_ms = int(last_7_days.timestamp() * 1000)

    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    sod_ms = int(start_of_day.astimezone(timezone.utc).timestamp() * 1000)

    amanha = now + timedelta(days=1)
    amanha_inicio_ms = int(
        amanha.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).timestamp() * 1000
    )
    amanha_fim_ms = int(
        amanha.replace(hour=23, minute=59, second=59, microsecond=0).astimezone(timezone.utc).timestamp() * 1000
    )

    lines = [
        "CONTEXTO DE TEMPO (use para filtros createdate e agendamentos hs_timestamp, NUNCA CALCULE DE CABEÇA):",
        f"- agora_local: {now.isoformat()}",
        f"- agora_epoch_ms: {now_ms}",
        f"- inicio_de_hoje_local(00:00) epoch_ms: {sod_ms}",
        f"- inicio_da_semana_local(seg 00:00) epoch_ms: {sow_ms}",
        f"- ultimos_7_dias_inicio epoch_ms: {last7_ms}",
        f"- AMANHA_inicio_epoch_ms: {amanha_inicio_ms}",
        f"- AMANHA_fim_epoch_ms: {amanha_fim_ms}",
    ]

    # Gera os horários (00h às 23h) de HOJE e AMANHA já convertidos para UTC ms
    for h in range(0, 24):
        hoje_h = now.replace(hour=h, minute=0, second=0, microsecond=0)
        amanha_h = amanha.replace(hour=h, minute=0, second=0, microsecond=0)
        lines.append(f"- HOJE_{h:02d}H_BRT_epoch_ms: {int(hoje_h.astimezone(timezone.utc).timestamp() * 1000)}")
        lines.append(f"- AMANHA_{h:02d}H_BRT_epoch_ms: {int(amanha_h.astimezone(timezone.utc).timestamp() * 1000)}")

    return "\n".join(lines)


def _extract_text(content: list[dict[str, Any]]) -> str:
    return "\n".join(str(b.get("text") or "") for b in content if b.get("type") == "text").strip()


def _prune_messages_for_speed(messages: list[dict[str, Any]], max_messages: int = 18) -> list[dict[str, Any]]:  # [CORRIGIDO] AVISO 6 — tool_result órfão
    """
    Mantém o histórico curto para evitar travamentos por tokens em conversas longas,
    sem quebrar o par assistant(tool_use) -> user(tool_result).
    """
    if len(messages) <= max_messages:
        return messages

    keep = messages[-max_messages:]

    # Se o primeiro item do recorte for um tool_result, garanta que o tool_use anterior fique junto.
    if keep and keep[0].get("role") == "user" and isinstance(keep[0].get("content"), list):
        content0 = keep[0]["content"]
        if any(isinstance(b, dict) and b.get("type") == "tool_result" for b in content0):
            # procura um assistant imediatamente antes no original
            idx0 = len(messages) - max_messages
            if idx0 - 1 >= 0 and messages[idx0 - 1].get("role") == "assistant":
                keep = [messages[idx0 - 1]] + keep

    while keep and keep[0].get("role") == "user":
        content0 = keep[0].get("content")
        if isinstance(content0, list) and any(isinstance(b, dict) and b.get("type") == "tool_result" for b in content0):
            keep = keep[1:]
        else:
            break

    if not keep:
        return messages[-1:] if messages else keep

    return keep


def main() -> int:  # [CORRIGIDO] CRÍTICO 1 + CRÍTICO 4 — sincroniza token HubSpot + truncagem tool_result
    load_dotenv()
    global _HUBSPOT_ACCESS_TOKEN
    _load_capability_cache()

    # Smoke test simples de busca (para validar Search do HubSpot rapidamente)
    if "--smoke-search" in sys.argv:
        token = get_access_token_via_refresh()
        _HUBSPOT_ACCESS_TOKEN = token
        _status("SMOKE: iniciando busca simples (limit=1) para contacts e leads...")
        for obj in ("contacts", "leads"):
            sort_prop = _resolve_best_date_property(token, obj)
            body = {
                "properties": list(_object_config(obj).get("default_properties") or [])[:6],
                "limit": 1,
                "sorts": [f"-{sort_prop}"],
            }
            res = hubspot_universal_api_call(token, endpoint=f"/crm/v3/objects/{obj}/search", method="POST", json_payload=body)
            if not res.get("ok"):
                print(f"SMOKE FAIL {obj}: HTTP {res.get('status_code')} - {res.get('response_text')}", file=sys.stderr)
                return 1
            results = (res.get("response_json") or {}).get("results") or []
            _status(f"SMOKE OK {obj}: retornou {len(results)} registro(s).")
        print("SMOKE SEARCH: OK")
        return 0

    if "--debug-yesterday-contacts" in sys.argv:
        token = get_access_token_via_refresh()
        _HUBSPOT_ACCESS_TOKEN = token
        # Reusa exatamente o mesmo relógio do contexto do LLM
        now_ctx = _now_context_for_llm()
        m1 = re.search(r'ONTEM_inicio_epoch_ms:\s*(\d+)', now_ctx)
        m2 = re.search(r'ONTEM_fim_epoch_ms:\s*(\d+)', now_ctx)
        if not (m1 and m2):
            print("DEBUG: não consegui extrair ONTEM_* do contexto.", file=sys.stderr)
            return 2
        start_ms = int(m1.group(1))
        end_ms = int(m2.group(1))
        _status(f"DEBUG: ontem range UTC-ms = [{start_ms}, {end_ms}]")

        # 1) Últimos 5 contatos (para inspecionar createdate real)
        recent = _search_objects(
            token,
            object_type="contacts",
            properties=["firstname", "lastname", "email", "createdate"],
            sorts=["-createdate"],
            per_page=5,
            max_records=5,
        )
        print("\nULTIMOS 5 CONTATOS (createdate):")
        for row in recent:
            props = (row or {}).get("properties") or {}
            print(f"- id={row.get('id')} createdate={props.get('createdate')} email={props.get('email')}")

        # 2) Busca "ontem" (limit 20)
        yesterday = _search_objects(
            token,
            object_type="contacts",
            properties=["firstname", "lastname", "email", "createdate"],
            filter_groups=[
                {
                    "filters": [
                        {"propertyName": "createdate", "operator": "BETWEEN", "value": str(start_ms), "highValue": str(end_ms)}
                    ]
                }
            ],
            sorts=["-createdate"],
            per_page=20,
            max_records=60,
        )
        print(f"\nCONTATOS ENCONTRADOS ONTEM: {len(yesterday)}")
        for row in yesterday[:20]:
            props = (row or {}).get("properties") or {}
            print(f"- id={row.get('id')} createdate={props.get('createdate')} email={props.get('email')}")
        return 0

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("Erro: defina ANTHROPIC_API_KEY no .env.", file=sys.stderr)
        return 2
    model = pick_anthropic_model(api_key, os.getenv("ANTHROPIC_MODEL"))
    token = get_access_token_via_refresh()
    _HUBSPOT_ACCESS_TOKEN = token

    messages: list[dict[str, Any]] = []
    pending_owner_assignment_email_request = False
    print("Chat da Livia (DUMB PIPE) iniciado.")
    print("Digite 'sair' para encerrar.\n")

    while True:
        user_text = input("Voce: ").strip()
        if not user_text:
            continue
        _learn_user_preferences(user_text)
        _remember_request(user_text)
        if user_text.lower() in {"sair", "exit", "quit"}:
            return 0
        try:
            orchestrated_text, pending_owner_assignment_email_request = _orchestrate_user_request(
                api_key,
                model=model,
                access_token=token,
                user_text=user_text,
                pending_owner_assignment_email_request=pending_owner_assignment_email_request,
            )
            if orchestrated_text:
                print("\nLivia:\n" + orchestrated_text + "\n")
                messages = _record_completed_turn(messages, user_text, orchestrated_text)
                token = _HUBSPOT_ACCESS_TOKEN or token
                continue
        except Exception as exc:
            try:
                error_text = _present_and_remember_operation(
                    api_key,
                    model=model,
                    user_text=user_text,
                    operation_name="falha operacional no orquestrador principal",
                    operation_result={"ok": False, "error": str(exc)},
                    mode="orchestrator_error",
                    constraints=["nao repetir automaticamente uma operacao apos falha sem alvo resolvido"],
                )
                print("\nLivia:\n" + error_text + "\n")
                messages = _record_completed_turn(messages, user_text, error_text)
                continue
            except Exception:
                pass
        capability_context = _build_capability_context(token, user_text)
        messages.append({"role": "user", "content": user_text})
        messages = _prune_messages_for_speed(messages)
        _status("Entendi o pedido. Vou montar o plano de execucao.")

        while True:
            dynamic_system = (
                SYSTEM_PROMPT
                + "\n\n"
                + _now_context_for_llm()
                + "\n\n"
                + capability_context
            )
            resp = anthropic_messages(api_key, model=model, system=dynamic_system, messages=messages)
            content = resp.get("content") or []
            tool_uses = [c for c in content if isinstance(c, dict) and c.get("type") == "tool_use"]
            if tool_uses:
                _status(f"Executando {len(tool_uses)} chamada(s) de ferramenta...")
                messages.append({"role": "assistant", "content": content})
                tool_results = []
                for tu in tool_uses:
                    inp = tu.get("input") or {}
                    tool_name = tu.get("name")
                    _status(f"Ferramenta: {tool_name}")
                    if tool_name == "hubspot_bulk_update_tool":
                        result = hubspot_bulk_update_tool(
                            token,
                            object_type=str(inp.get("object_type") or ""),
                            record_ids=inp.get("record_ids") or [],
                            properties_to_update=inp.get("properties_to_update") or {},
                        )
                    elif tool_name == "hubspot_bulk_update_by_search_tool":
                        result = hubspot_bulk_update_by_search_tool(
                            token,
                            object_type=str(inp.get("object_type") or ""),
                            search_body=inp.get("search_body") or {},
                            properties_to_update=inp.get("properties_to_update") or {},
                            max_records=inp.get("max_records"),
                        )
                    elif tool_name == "hubspot_create_and_associate_lead":
                        result = hubspot_create_and_associate_lead_tool(
                            token,
                            contact_id=str(inp.get("contact_id") or ""),
                            lead_name=str(inp.get("lead_name") or ""),
                            pipeline_stage=inp.get("pipeline_stage"),
                        )
                    elif tool_name == "hubspot_create_note_for_contact":
                        result = hubspot_create_note_for_contact_tool(
                            token,
                            contact_id=str(inp.get("contact_id") or ""),
                            note_body=str(inp.get("note_body") or ""),
                        )
                    elif tool_name == "hubspot_create_task_tool":
                        result = hubspot_create_task_tool(
                            token,
                            title=str(inp.get("title") or ""),
                            body=str(inp.get("body") or ""),
                            days_from_now=int(inp.get("days_from_now") or 0),
                            target_id=str(inp.get("target_id") or ""),
                            target_type=str(inp.get("target_type") or ""),
                        )
                    elif tool_name == "hubspot_create_meeting_tool":
                        result = hubspot_create_meeting_tool(
                            token,
                            contact_id=str(inp.get("contact_id") or ""),
                            title=str(inp.get("title") or ""),
                            body=str(inp.get("body") or ""),
                            start_time_ms=int(inp.get("start_time_ms") or 0),
                            duration_minutes=int(inp.get("duration_minutes") or 60),
                            location=str(inp.get("location") or ""),
                        )
                    elif tool_name == "hubspot_check_calendar_tool":
                        result = hubspot_check_calendar_tool(
                            token,
                            start_time_ms=int(inp.get("start_time_ms") or 0),
                            end_time_ms=int(inp.get("end_time_ms") or 0),
                        )
                    elif tool_name == "hubspot_list_meetings_for_contact_tool":
                        result = hubspot_list_meetings_for_contact_tool(
                            token,
                            contact_id=str(inp.get("contact_id") or ""),
                            limit=int(inp.get("limit") or 100),
                        )
                    elif tool_name == "hubspot_update_meeting_engagement_tool":
                        _active = inp.get("active")
                        _stm = inp.get("start_time_ms")
                        result = hubspot_update_meeting_engagement_tool(
                            token,
                            str(inp.get("engagement_id") or ""),
                            active=_active if _active is not None else None,
                            start_time_ms=int(_stm) if _stm is not None else None,
                            duration_minutes=int(inp.get("duration_minutes") or 60),
                            location=str(inp.get("location") or ""),
                        )
                    elif tool_name == "hubspot_associate_records_tool":
                        result = hubspot_associate_records_tool(
                            token,
                            from_object_type=str(inp.get("from_object_type") or ""),
                            from_object_id=str(inp.get("from_object_id") or ""),
                            to_object_type=str(inp.get("to_object_type") or ""),
                            to_object_id=str(inp.get("to_object_id") or ""),
                        )
                    elif tool_name == "hubspot_remove_association_tool":
                        result = hubspot_remove_association_tool(
                            token,
                            from_object_type=str(inp.get("from_object_type") or ""),
                            from_object_id=str(inp.get("from_object_id") or ""),
                            to_object_type=str(inp.get("to_object_type") or ""),
                            to_object_id=str(inp.get("to_object_id") or ""),
                        )
                    else:
                        result = hubspot_universal_api_call(
                            token,
                            endpoint=str(inp.get("endpoint") or ""),
                            method=str(inp.get("method") or ""),
                            params=inp.get("params"),
                            json_payload=inp.get("json_payload"),
                        )
                    token = _hubspot_token_sync_from_tool_result(token, result)
                    result_str = json.dumps(result, ensure_ascii=False)
                    MAX_RESULT_LENGTH = 6000
                    if len(result_str) > MAX_RESULT_LENGTH:
                        result_str = result_str[:MAX_RESULT_LENGTH] + (
                            f"... [TRUNCADO PELO SISTEMA: Limite de {MAX_RESULT_LENGTH} caracteres atingido. "
                            "Refine os filtros da busca para evitar estourar a memória.]"
                        )
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tu.get("id"),
                            "content": result_str,
                        }
                    )
                messages.append({"role": "user", "content": tool_results})
                messages = _prune_messages_for_speed(messages)
                continue

            text = _extract_text(content)
            print("\nLivia:\n" + (text if text else "(sem texto)") + "\n")
            _remember_structured_execution(
                user_text,
                operation_name="fallback conversacional com ferramentas",
                operation_result={"ok": True, "assistant_text": text if text else "(sem texto)"},
                mode="fallback_chat_tools",
            )
            _remember_turn(user_text, text if text else "(sem texto)")
            messages.append({"role": "assistant", "content": content})
            messages = _prune_messages_for_speed(messages)
            break


if __name__ == "__main__":
    raise SystemExit(main())