# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import sys
import time
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


SYSTEM_PROMPT = """Você é Lívia, uma Agente Autônoma SDR com permissões totais no HubSpot CRM v3.
Seu comportamento deve ser focado em EXECUÇÃO DIRETA, evitando verificações (GET) redundantes.

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
- Ordenação: para pegar os mais recentes, use "sorts": [{"propertyName":"createdate","direction":"DESCENDING"}]
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
  "sorts": [{"propertyName":"createdate","direction":"DESCENDING"}],
  "properties": ["firstname","lastname","email","phone","mobilephone","createdate","hs_lead_status"],
  "limit": 20
}

DOCUMENTAÇÃO OBRIGATÓRIA - CRIAÇÃO E ASSOCIAÇÃO DE LEADS:
Quando o usuário pedir para criar leads para contatos, siga ESTRITAMENTE esta sequência REST:

Fase 1: Identificação (Apenas 1 vez)
GET /crm/v3/objects/contacts (Use properties=firstname,lastname para compor o nome completo do lead, e identifique quem não tem Lead associado).

Fase 2: Execução (Faça em Lote de Chamadas)
Para CADA contato encontrado na Fase 1:

Passo A - Criar o Lead:
Ferramenta: hubspot_universal_api_call
Endpoint: /crm/v3/objects/leads
Method: POST
JSON Payload: {"properties": {"hs_lead_name": "[Nome e Sobrenome do Contato]"}}

Passo B - Associar ao Contato:
Ferramenta: hubspot_universal_api_call
Endpoint (preferencial, mais simples): /crm/v4/objects/leads/[ID_DO_LEAD_GERADO]/associations/default/contacts/[ID_DO_CONTATO]
Method: PUT

Se for associar vários pares, use batch (v4):
POST /crm/v4/associations/leads/contacts/batch/associate/default
Payload: {"inputs":[{"from":{"id":"<LEAD_ID>"},"to":{"id":"<CONTACT_ID>"}}]}

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


def hubspot_bulk_update_tool(
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

    for batch_idx, batch_ids in enumerate(_chunked(ids, 100), start=1):
        payload = {"inputs": [{"id": rid, "properties": props} for rid in batch_ids]}
        res = hubspot_universal_api_call(access_token, endpoint=endpoint, method="POST", json_payload=payload)

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
    return {
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


def hubspot_bulk_update_by_search_tool(
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

    while True:
        if after is not None:
            body["after"] = after
        elif "after" in body:
            body.pop("after", None)

        res = hubspot_universal_api_call(access_token, endpoint=endpoint_search, method="POST", json_payload=body)
        if not res.get("ok"):
            return {
                "ok": False,
                "phase": "search",
                "object_type": obj,
                "endpoint": endpoint_search,
                "status_code": res.get("status_code"),
                "response_text": res.get("response_text"),
                "response_json": res.get("response_json"),
            }

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

    # Atualiza em lote com a ferramenta nativa
    upd = hubspot_bulk_update_tool(access_token, object_type=obj, record_ids=collected, properties_to_update=props)
    upd["search_collected"] = len(collected)
    upd["max_records"] = max_n
    return upd


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


def hubspot_universal_api_call(
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
    if resp.status_code == 401:
        # Expirou/revogou: renova e tenta 1 vez de forma transparente.
        try:
            tok = get_access_token_via_refresh()
            _HUBSPOT_ACCESS_TOKEN = tok
            refreshed = True
            resp = _do_request(tok)
        except Exception:
            pass
    text = resp.text or ""
    text_out = text[:20000] + ("...(truncado)" if len(text) > 20000 else "")
    try:
        json_out = resp.json()
    except Exception:
        json_out = None
    return {
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


def anthropic_list_models(api_key: str) -> list[str]:
    resp = requests.get(f"{ANTHROPIC_BASE}/v1/models", headers=_anthropic_headers(api_key), timeout=30)
    if resp.status_code >= 400:
        return []
    data = resp.json()
    return [str(m.get("id")) for m in (data.get("data") or []) if (m or {}).get("id")]


def pick_anthropic_model(api_key: str, preferred: Optional[str]) -> str:
    if preferred:
        return preferred
    available = anthropic_list_models(api_key)
    if not available:
        return "claude-sonnet-4-6"
    prefs = ["claude-haiku-4-5-20251001", "claude-haiku-4-5", "claude-sonnet-4-6", "claude-sonnet-4", "claude-opus-4-6"]
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
    i = 0
    while True:
        try:
            # (connect timeout, read timeout)
            resp = _ANTHROPIC_SESSION.post(url, headers=_anthropic_headers(api_key), json=body, timeout=(10, 30))
        except KeyboardInterrupt:
            # Não finalize o programa ao interromper uma request "presa".
            # Apenas cancela essa tentativa e tenta novamente.
            i += 1
            continue
        except (Timeout, RequestException):
            # Falha de rede/transiente: trate como overload e tente novamente.
            sleep_s = min(15.0, max(0.5, base_sleep * (2**min(i, 6))))
            try:
                time.sleep(sleep_s)
            except KeyboardInterrupt:
                pass
            i += 1
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

            try:
                time.sleep(sleep_s)
            except KeyboardInterrupt:
                # Ctrl+C vira "pular espera e tentar novamente", não mata o programa.
                pass
            i += 1
            continue

        raise RuntimeError(f"Anthropic HTTP {resp.status_code}: {resp.text[:2000]}")


def _now_context_for_llm() -> str:
    """
    Fornece ao LLM referências de tempo para ele construir filtros createdate sem pedir clarificação.
    Não executa nenhuma lógica de HubSpot; apenas dá o 'relógio' atual.
    """
    # Tenta usar America/Sao_Paulo sem dependências externas.
    tz = None
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        tz = ZoneInfo("America/Sao_Paulo")
    except Exception:
        tz = timezone.utc

    now = datetime.now(tz=tz)
    now_utc = now.astimezone(timezone.utc)
    now_ms = int(now_utc.timestamp() * 1000)

    # semana começa na segunda 00:00 no fuso local (assumido)
    start_of_week = (now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now.weekday()))
    sow_utc = start_of_week.astimezone(timezone.utc)
    sow_ms = int(sow_utc.timestamp() * 1000)

    last_7_days = now_utc - timedelta(days=7)
    last7_ms = int(last_7_days.timestamp() * 1000)

    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    sod_ms = int(start_of_day.timestamp() * 1000)

    return (
        "CONTEXTO DE TEMPO (use para filtros createdate, sem perguntar):\n"
        f"- agora_local: {now.isoformat()}\n"
        f"- agora_utc: {now_utc.isoformat()}\n"
        f"- agora_epoch_ms: {now_ms}\n"
        f"- inicio_da_semana_local(seg 00:00) epoch_ms: {sow_ms}\n"
        f"- ultimos_7_dias_inicio epoch_ms: {last7_ms}\n"
        f"- inicio_de_hoje_local(00:00) epoch_ms: {sod_ms}\n"
    )


def _extract_text(content: list[dict[str, Any]]) -> str:
    return "\n".join(str(b.get("text") or "") for b in content if b.get("type") == "text").strip()


def _prune_messages_for_speed(messages: list[dict[str, Any]], max_messages: int = 18) -> list[dict[str, Any]]:
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

    return keep


def main() -> int:
    load_dotenv()
    global _HUBSPOT_ACCESS_TOKEN
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("Erro: defina ANTHROPIC_API_KEY no .env.", file=sys.stderr)
        return 2
    model = pick_anthropic_model(api_key, os.getenv("ANTHROPIC_MODEL"))
    token = get_access_token_via_refresh()
    _HUBSPOT_ACCESS_TOKEN = token

    messages: list[dict[str, Any]] = []
    print("Chat da Livia (DUMB PIPE) iniciado.")
    print("Digite 'sair' para encerrar.\n")

    while True:
        user_text = input("Voce: ").strip()
        if not user_text:
            continue
        if user_text.lower() in {"sair", "exit", "quit"}:
            return 0
        messages.append({"role": "user", "content": user_text})
        messages = _prune_messages_for_speed(messages)

        while True:
            resp = anthropic_messages(api_key, model=model, system=SYSTEM_PROMPT + "\n\n" + _now_context_for_llm(), messages=messages)
            content = resp.get("content") or []
            tool_uses = [c for c in content if isinstance(c, dict) and c.get("type") == "tool_use"]
            if tool_uses:
                messages.append({"role": "assistant", "content": content})
                tool_results = []
                for tu in tool_uses:
                    inp = tu.get("input") or {}
                    tool_name = tu.get("name")
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
                    else:
                        result = hubspot_universal_api_call(
                            token,
                            endpoint=str(inp.get("endpoint") or ""),
                            method=str(inp.get("method") or ""),
                            params=inp.get("params"),
                            json_payload=inp.get("json_payload"),
                        )
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tu.get("id"),
                            "content": json.dumps(
                                result,
                                ensure_ascii=False,
                            ),
                        }
                    )
                messages.append({"role": "user", "content": tool_results})
                messages = _prune_messages_for_speed(messages)
                continue

            text = _extract_text(content)
            print("\nLivia:\n" + (text if text else "(sem texto)") + "\n")
            messages.append({"role": "assistant", "content": content})
            messages = _prune_messages_for_speed(messages)
            break


if __name__ == "__main__":
    raise SystemExit(main())

