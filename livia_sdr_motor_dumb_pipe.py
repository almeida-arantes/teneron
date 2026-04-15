from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Optional

import requests
from dotenv import load_dotenv

HUBSPOT_BASE = "https://api.hubapi.com"
ANTHROPIC_BASE = "https://api.anthropic.com"

SYSTEM_PROMPT = """Você é Lívia, uma Agente Autônoma SDR com permissões totais no HubSpot CRM v3.
Seu comportamento deve ser focado em EXECUÇÃO DIRETA, evitando verificações (GET) redundantes.

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
Endpoint: /crm/v3/objects/leads/[ID_DO_LEAD_GERADO]/associations/contacts/[ID_DO_CONTATO]/lead_to_contact
Method: PUT

PROTOCOLOS DE ERRO (CRÍTICO):
Se a ferramenta retornar um erro HTTP, NÃO FAÇA UM NOVO GET. Leia o tool_result (inclui response_text cru) e ajuste POST/PUT imediatamente.
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
]


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
    method_u = (method or "").strip().upper()
    ep = (endpoint or "").strip()
    if not ep.startswith("/"):
        ep = "/" + ep
    url = f"{HUBSPOT_BASE}{ep}"
    resp = requests.request(method_u, url, headers=_hubspot_headers(access_token), params=params or None, json=json_payload or None, timeout=60)
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
    body: dict[str, Any] = {"model": model, "max_tokens": 900, "system": system, "messages": messages, "tools": TOOLS, "tool_choice": {"type": "auto"}}
    base_sleep = 1.5
    for i in range(6):
        resp = requests.post(url, headers=_anthropic_headers(api_key), json=body, timeout=90)
        if resp.status_code < 400:
            return resp.json()
        if resp.status_code in (429, 529):
            time.sleep(base_sleep * (2**i))
            continue
        raise RuntimeError(f"Anthropic HTTP {resp.status_code}: {resp.text[:2000]}")
    raise RuntimeError("Anthropic overloaded/rate-limited.")


def _extract_text(content: list[dict[str, Any]]) -> str:
    return "\n".join(str(b.get("text") or "") for b in content if b.get("type") == "text").strip()


def main() -> int:
    load_dotenv()
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("Erro: defina ANTHROPIC_API_KEY no .env.", file=sys.stderr)
        return 2
    model = pick_anthropic_model(api_key, os.getenv("ANTHROPIC_MODEL"))
    token = get_access_token_via_refresh()

    messages: list[dict[str, Any]] = []
    print("Chat da Lívia (DUMB PIPE) iniciado.")
    print("Digite 'sair' para encerrar.\n")

    while True:
        user_text = input("Você: ").strip()
        if not user_text:
            continue
        if user_text.lower() in {"sair", "exit", "quit"}:
            return 0
        messages.append({"role": "user", "content": user_text})

        while True:
            resp = anthropic_messages(api_key, model=model, system=SYSTEM_PROMPT, messages=messages)
            content = resp.get("content") or []
            tool_uses = [c for c in content if isinstance(c, dict) and c.get("type") == "tool_use"]
            if tool_uses:
                messages.append({"role": "assistant", "content": content})
                tool_results = []
                for tu in tool_uses:
                    inp = tu.get("input") or {}
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tu.get("id"),
                            "content": json.dumps(
                                hubspot_universal_api_call(
                                    token,
                                    endpoint=str(inp.get("endpoint") or ""),
                                    method=str(inp.get("method") or ""),
                                    params=inp.get("params"),
                                    json_payload=inp.get("json_payload"),
                                ),
                                ensure_ascii=False,
                            ),
                        }
                    )
                messages.append({"role": "user", "content": tool_results})
                continue

            text = _extract_text(content)
            print("\nLívia:\n" + (text if text else "(sem texto)") + "\n")
            messages.append({"role": "assistant", "content": content})
            break


if __name__ == "__main__":
    raise SystemExit(main())

