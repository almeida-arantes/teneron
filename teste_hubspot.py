from __future__ import annotations

import json
import os
from typing import Any, Iterable, Optional, Sequence

import requests
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.contacts.exceptions import ApiException as ContactsApiException


def _safe_str(value: Optional[str]) -> str:
    if value is None:
        return "-"
    value = str(value).strip()
    return value if value else "-"


def _mask(value: Optional[str]) -> str:
    if not value:
        return "-"
    v = value.strip()
    if len(v) <= 8:
        return "***"
    return f"{v[:4]}...{v[-4:]}"


def _print_table(rows: Sequence[Sequence[str]], headers: Sequence[str]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(values: Iterable[str]) -> str:
        return " | ".join(v.ljust(widths[i]) for i, v in enumerate(values))

    sep = "-+-".join("-" * w for w in widths)
    print(fmt_row(headers))
    print(sep)
    for row in rows:
        print(fmt_row(row))


def _get_access_token_via_refresh() -> str:
    load_dotenv()

    client_id = os.getenv("HUBSPOT_CLIENT_ID")
    client_secret = os.getenv("HUBSPOT_CLIENT_SECRET")
    refresh_token = os.getenv("HUBSPOT_REFRESH_TOKEN")

    missing: list[str] = []
    if not client_id:
        missing.append("HUBSPOT_CLIENT_ID")
    if not client_secret:
        missing.append("HUBSPOT_CLIENT_SECRET")
    if not refresh_token:
        missing.append("HUBSPOT_REFRESH_TOKEN")

    if missing:
        raise RuntimeError(
            "Faltam variaveis no .env para gerar access token via refresh token: "
            + ", ".join(missing)
            + ".\n"
            + f"DEBUG (mascarado) client_id={_mask(client_id)} client_secret={_mask(client_secret)} refresh_token={_mask(refresh_token)}"
        )

    url_oauth = "https://api.hubapi.com/oauth/v1/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }

    resp = requests.post(url_oauth, data=data, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Falha ao gerar access token: {resp.text}")

    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Resposta sem access_token: {resp.text}")

    return token


CONTACT_PROPERTIES_FOR_AI = [
    # identity
    "firstname",
    "lastname",
    "email",
    "phone",
    "mobilephone",
    # work
    "company",
    "jobtitle",
    "website",
    # lifecycle / lead
    "lifecyclestage",
    "hs_lead_status",
    # metadata
    "createdate",
    "lastmodifieddate",
]


def _get_associated_ids(api_client: HubSpot, contact_id: str, to_object_type: str) -> list[str]:
    """
    Busca IDs associados ao contato (ex.: companies, deals).
    Mantém simples: pega primeira página (geralmente suficiente para triagem/IA).
    """
    try:
        resp = api_client.crm.contacts.associations_api.get_all(contact_id, to_object_type)
    except Exception:
        return []

    results = getattr(resp, "results", None) or []
    ids: list[str] = []
    for r in results:
        to_obj = getattr(r, "to_object_id", None)
        if to_obj is not None:
            ids.append(str(to_obj))
    return ids


def _discover_lead_object_types(api_client: HubSpot) -> list[str]:
    """
    Descobre o(s) object type(s) que parecem ser "Lead" no portal.

    Observação:
    - Em muitos portais, "Lead" não é um objeto separado (é só status/propriedade).
    - Se você tiver um objeto customizado Lead, ele aparece nos Schemas como "fullyQualifiedName"
      (ex.: 'p123456_leads' ou similar).
    """
    try:
        schemas_resp = api_client.crm.schemas.core_api.get_all()
    except Exception:
        return []

    schemas = getattr(schemas_resp, "results", None) or []
    out: list[str] = []
    for s in schemas or []:
        fqn = getattr(s, "fully_qualified_name", None) or getattr(s, "fullyQualifiedName", None)
        name = (getattr(s, "name", None) or "").strip()
        labels = getattr(s, "labels", None)
        singular = ""
        plural = ""
        if labels:
            singular = (getattr(labels, "singular", None) or "").strip()
            plural = (getattr(labels, "plural", None) or "").strip()

        hay = " ".join([name, singular, plural, str(fqn or "")]).lower()
        if "lead" in hay:
            if fqn:
                out.append(str(fqn))
            elif name:
                out.append(name)

    # Também tentar o objeto padrão 'leads' caso exista no portal
    if "leads" not in out:
        out.append("leads")

    # dedupe preservando ordem
    seen: set[str] = set()
    deduped: list[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


def _get_associated_ids_from_object(access_token: str, object_type: str, object_id: str, to_object_type: str) -> list[str]:
    """
    Busca IDs associados partindo de qualquer objeto (contact/custom/etc).

    A lib python tem clients v3 por objeto, mas para custom objects nem sempre vem tipado.
    Então, aqui usamos a chamada HTTP direta (v4 associations) com o mesmo access token.
    """
    base_url = "https://api.hubapi.com"
    url = f"{base_url}/crm/v4/objects/{object_type}/{object_id}/associations/{to_object_type}"

    headers = _hubspot_headers(access_token)
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code >= 400:
            return []
        data = resp.json() or {}
        results = data.get("results") or []
        ids: list[str] = []
        for r in results:
            to_obj = r.get("toObjectId")
            if to_obj is not None:
                ids.append(str(to_obj))
        return ids
    except Exception:
        return []


def _hubspot_headers(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def _get_all_property_names(access_token: str, object_type: str) -> list[str]:
    """
    Lista TODOS os nomes de propriedades existentes para o objeto.
    """
    url = f"https://api.hubapi.com/crm/v3/properties/{object_type}"
    try:
        resp = requests.get(url, headers=_hubspot_headers(access_token), timeout=30)
        if resp.status_code >= 400:
            return []
        data = resp.json() or {}
        results = data.get("results") or []
        names: list[str] = []
        for r in results:
            n = r.get("name")
            if n:
                names.append(str(n))
        return names
    except Exception:
        return []


def _batch_read_objects(
    access_token: str,
    object_type: str,
    ids: list[str],
    properties: list[str],
) -> dict[str, dict[str, Any]]:
    """
    Faz batch read e retorna {id: properties_dict}.

    Retorna só propriedades que a API devolveu para cada objeto.
    """
    if not ids:
        return {}

    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/batch/read"
    out: dict[str, dict[str, Any]] = {}

    # A API de batch aceita até 100 inputs; manter conservador.
    chunk_size = 100
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        payload = {
            "properties": properties,
            "inputs": [{"id": _safe_str(x)} for x in chunk],
        }
        try:
            resp = requests.post(url, headers=_hubspot_headers(access_token), json=payload, timeout=60)
            if resp.status_code >= 400:
                continue
            data = resp.json() or {}
            results = data.get("results") or []
            for r in results:
                rid = str(r.get("id")) if r.get("id") is not None else None
                props = r.get("properties") or {}
                if rid:
                    out[rid] = props
        except Exception:
            continue

    return out


def _filter_filled_properties(props: dict[str, Any]) -> dict[str, Any]:
    """
    Mantém somente propriedades preenchidas (não-null e não-vazias).
    """
    filled: dict[str, Any] = {}
    for k, v in (props or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        filled[k] = v
    return filled


def _pick_first(props: dict[str, Any], keys: list[str]) -> Optional[str]:
    for k in keys:
        v = props.get(k)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return str(v)
    return None


def main() -> int:
    try:
        access_token = _get_access_token_via_refresh()
    except Exception as exc:
        print(f"Erro ao gerar token de acesso: {exc}")
        return 1

    api_client = HubSpot(access_token=access_token)
    lead_object_types = _discover_lead_object_types(api_client)

    # Para IA: HubSpot só retorna as propriedades pedidas aqui.
    # Ajuste/expanda esta lista conforme a necessidade do seu agente.
    properties = CONTACT_PROPERTIES_FOR_AI

    try:
        # Search API permite ordenar e pegar os mais recentes.
        search_req = {
            "filterGroups": [],
            "sorts": ["-createdate"],
            "properties": properties,
            "limit": 20,
            "after": 0,
        }
        page = api_client.crm.contacts.search_api.do_search(public_object_search_request=search_req)
    except ContactsApiException as exc:
        status = getattr(exc, "status", None)
        reason = getattr(exc, "reason", None)
        body = getattr(exc, "body", None)
        print("Falha ao buscar contatos no HubSpot (CRM Contacts).")
        print(f"HTTP status: {_safe_str(status)}")
        print(f"Reason: {_safe_str(reason)}")
        if body:
            print(f"Body: {body}")
        return 1

    results = getattr(page, "results", None) or []
    if not results:
        print("Conexao OK, mas nenhum contato foi retornado.")
        return 0

    rows: list[list[str]] = []
    export: list[dict[str, Any]] = []

    # Cache de nomes de propriedades por tipo de objeto
    properties_cache: dict[str, list[str]] = {}

    # Cache de detalhes por (tipo, id) - somente preenchidas
    object_details_cache_filled: dict[tuple[str, str], dict[str, Any]] = {}

    for c in results:
        props = getattr(c, "properties", {}) or {}
        contact_id = _safe_str(getattr(c, "id", None))

        associated_companies = _get_associated_ids(api_client, contact_id, "companies")
        associated_deals = _get_associated_ids(api_client, contact_id, "deals")

        # Se existir um objeto Lead no portal, tenta: Contato -> Lead -> Deal
        associated_leads: list[str] = []
        lead_to_deals: dict[str, list[str]] = {}
        for lead_type in lead_object_types:
            # primeiro tenta via SDK (quando 'leads' for reconhecido), senão via HTTP direto
            lead_ids = _get_associated_ids(api_client, contact_id, lead_type) or _get_associated_ids_from_object(
                access_token, "contacts", contact_id, lead_type
            )
            for lead_id in lead_ids:
                associated_leads.append(lead_id)
                deals_from_lead = _get_associated_ids_from_object(access_token, lead_type, lead_id, "deals")
                if deals_from_lead:
                    lead_to_deals[lead_id] = deals_from_lead

        # Expandir: buscar TODAS as propriedades preenchidas do Lead e do Deal
        lead_ids_unique = sorted(set(associated_leads))
        deal_ids_unique = sorted(
            set(associated_deals)
            | {d for deals in lead_to_deals.values() for d in (deals or [])}
        )

        # Ajuda a IA a entender o grafo: de onde veio cada Deal
        deal_sources: dict[str, Any] = {}
        for did in associated_deals:
            deal_sources[str(did)] = {"via": "contact"}
        for lid, deals in (lead_to_deals or {}).items():
            for did in deals or []:
                deal_sources[str(did)] = {"via": "lead", "lead_id": str(lid)}

        leads_details: dict[str, Any] = {}
        for lead_type in lead_object_types:
            # Para cada tipo possível de Lead, só buscamos detalhes dos IDs que existirem
            if not lead_ids_unique:
                continue

            if lead_type not in properties_cache:
                properties_cache[lead_type] = _get_all_property_names(access_token, lead_type)

            all_names = properties_cache.get(lead_type) or []
            if not all_names:
                continue

            missing_ids = [
                lid for lid in lead_ids_unique if (lead_type, lid) not in object_details_cache_filled
            ]
            if missing_ids:
                batch = _batch_read_objects(access_token, lead_type, missing_ids, all_names)
                for lid, lprops in batch.items():
                    object_details_cache_filled[(lead_type, lid)] = _filter_filled_properties(lprops)

            # anexar somente os que existem nesse tipo
            for lid in lead_ids_unique:
                key = (lead_type, lid)
                if key in object_details_cache_filled:
                    leads_details.setdefault(lead_type, {})[lid] = object_details_cache_filled.get(key, {})

        deals_details: dict[str, Any] = {}
        if deal_ids_unique:
            deal_type = "deals"
            if deal_type not in properties_cache:
                properties_cache[deal_type] = _get_all_property_names(access_token, deal_type)

            deal_prop_names = properties_cache.get(deal_type) or []
            if deal_prop_names:
                missing_deal_ids = [did for did in deal_ids_unique if (deal_type, did) not in object_details_cache_filled]
                if missing_deal_ids:
                    batch = _batch_read_objects(access_token, deal_type, missing_deal_ids, deal_prop_names)
                    for did, dprops in batch.items():
                        object_details_cache_filled[(deal_type, did)] = _filter_filled_properties(dprops)

                for did in deal_ids_unique:
                    key = (deal_type, did)
                    if key in object_details_cache_filled:
                        deals_details[did] = object_details_cache_filled.get(key, {})

        # Resumo "vendedor": campos-chave prontos pra IA consumir rapidamente (a partir das preenchidas)
        lead_summaries: list[dict[str, Any]] = []
        for lead_type, by_id in (leads_details or {}).items():
            for lid, lraw in (by_id or {}).items():
                lead_name = _pick_first(
                    lraw,
                    [
                        "hs_lead_name",
                        "hs_lead_name_calculated",
                        "name",
                        "leadname",
                        "subject",
                    ],
                )
                lead_stage = _pick_first(lraw, ["hs_pipeline_stage", "pipeline_stage", "stage"])
                lead_pipeline = _pick_first(lraw, ["hs_pipeline", "pipeline"])
                lead_label = _pick_first(lraw, ["hs_lead_label", "label"])
                lead_source = _pick_first(lraw, ["hs_lead_source", "lead_source", "source"])
                lead_summaries.append(
                    {
                        "object_type": lead_type,
                        "id": lid,
                        "name": lead_name,
                        "pipeline": lead_pipeline,
                        "stage": lead_stage,
                        "label": lead_label,
                        "source": lead_source,
                    }
                )

        deal_summaries: list[dict[str, Any]] = []
        for did, draw in (deals_details or {}).items():
            deal_name = _pick_first(draw, ["dealname", "hs_deal_name", "name"])
            deal_stage = _pick_first(draw, ["dealstage", "hs_pipeline_stage", "stage"])
            deal_pipeline = _pick_first(draw, ["pipeline", "hs_pipeline"])
            amount = _pick_first(draw, ["amount", "hs_amount"])
            close_date = _pick_first(draw, ["closedate", "hs_closedate"])
            deal_summaries.append(
                {
                    "id": did,
                    "name": deal_name,
                    "pipeline": deal_pipeline,
                    "stage": deal_stage,
                    "amount": amount,
                    "closedate": close_date,
                    "source": deal_sources.get(str(did), {}),
                }
            )

        export.append(
            {
                "id": contact_id,
                "properties": props,
                "associations": {
                    "companies": associated_companies,
                    "deals": associated_deals,
                    "leads": sorted(set(associated_leads)),
                    "lead_to_deals": lead_to_deals,
                },
                "expanded": {
                    # Para a IA: dados preenchidos dos registros associados
                    "leads": leads_details,
                    "deals": deals_details,
                    "deal_sources": deal_sources,
                    # Para a IA: "schema" (lista de propriedades existentes) para saber o que está vazio
                    "available_properties": {
                        "leads": {lt: properties_cache.get(lt, []) for lt in lead_object_types},
                        "deals": properties_cache.get("deals", []),
                    },
                    "ai_summary": {
                        "lead_records": lead_summaries,
                        "deal_records": deal_summaries,
                    },
                },
            }
        )
        rows.append(
            [
                _safe_str(props.get("firstname")),
                _safe_str(props.get("lastname")),
                _safe_str(props.get("email")),
                _safe_str(props.get("phone")),
                _safe_str(props.get("hs_lead_status")),
                str(len(sorted(set(associated_leads)))),
                str(len(deal_ids_unique)),
            ]
        )

    print("")
    print("HubSpot CRM - 20 contatos mais recentes")
    print("")
    _print_table(rows, headers=["Firstname", "Lastname", "Email", "Phone", "Lead status", "Leads", "Deals"])
    print("")

    out_path = "hubspot_contacts_sample.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    print(f"Amostra completa (properties + associations) salva em: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())