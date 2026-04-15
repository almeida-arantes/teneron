"""
Agente SDR (pré-vendas): transforma a análise do Supervisor em texto para WhatsApp.
Regras de negócio via REST do Supabase, com cache em memória (TTL 1 h).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from pydantic_ai import Agent

from supervisor import supervisor_agent

load_dotenv()

_CACHE_TTL = timedelta(hours=1)
_cache_regras: str = ""
_ultima_atualizacao: datetime | None = None


def _log_cinza(mensagem: str) -> None:
    """Aviso discreto no terminal (ANSI cinza) para depuração do cache."""
    print(f"\033[90m{mensagem}\033[0m")


def _montar_system_prompt_com_regras(corpo_regras: str) -> str:
    corpo = corpo_regras.strip() if corpo_regras else ""
    if not corpo:
        corpo = (
            "(Nenhum conteúdo retornado em regras_escritorio.conteudo — "
            "não afirme políticas concretas do escritório até haver cadastro.)"
        )
    return (
        "=== REGRAS DE NEGÓCIO DO ESCRITÓRIO (FONTE DE VERDADE ABSOLUTA) ===\n"
        "Os trechos abaixo foram carregados diretamente do cadastro do escritório (API Supabase). "
        "Trate como fato: não invente nomes, preços, áreas de atuação ou políticas além do que consta aqui. "
        "Se o pedido do cliente conflitar com alguma regra, cumpra a regra e responda com empatia no tom de WhatsApp.\n\n"
        f"{corpo}"
    )


def buscar_regras_escritorio() -> str:
    """Busca a coluna `conteudo` na tabela `regras_escritorio` pela API REST do Supabase (sem cache)."""
    base = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not base or not key:
        raise RuntimeError("Defina SUPABASE_URL e SUPABASE_KEY no ambiente (ex.: arquivo .env).")

    url = f"{base.rstrip('/')}/rest/v1/regras_escritorio?select=conteudo"
    response = requests.get(
        url,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        },
        timeout=30,
    )
    response.raise_for_status()

    payload = json.loads(response.text)
    if not isinstance(payload, list):
        raise ValueError(f"Resposta inesperada da API (esperado lista JSON): {type(payload).__name__}")

    partes: list[str] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        texto = row.get("conteudo")
        if isinstance(texto, str) and texto.strip():
            partes.append(texto.strip())

    return "\n\n".join(partes)


sdr_agent = Agent(
    supervisor_agent.model,
    output_type=str,
    instructions=(
        "Você é o SDR (pré-vendas) do escritório. "
        "Você recebe a análise interna do Supervisor em JSON (status, classificacao_do_cliente, resumo_da_mensagem) "
        "e, quando fornecida, a mensagem original do cliente. "
        "O system_prompt contém as regras oficiais vindas do banco (cache em memória de até 1 hora): "
        "elas têm prioridade absoluta sobre qualquer suposição sua. "
        "Redija UMA mensagem de WhatsApp em português do Brasil: tom amigável, acolhedor e comercial, "
        "sem juridiquês excessivo. Convide ao próximo passo quando fizer sentido, sem prometer resultado jurídico "
        "específico nem substituir consulta formal. "
        "Saída: somente o texto final, pronto para colar no WhatsApp — sem JSON, sem títulos, sem aspas envolvendo o texto todo."
    ),
)


@sdr_agent.system_prompt(dynamic=True)
def _system_prompt_regras_escritorio() -> str:
    """Monta o system prompt; evita HTTP ao Supabase enquanto o cache TTL for válido."""
    global _cache_regras, _ultima_atualizacao

    agora = datetime.now(timezone.utc)
    cache_valido = (
        _cache_regras != ""
        and _ultima_atualizacao is not None
        and (agora - _ultima_atualizacao) < _CACHE_TTL
    )

    if cache_valido:
        _log_cinza("Usando regras do cache...")
        return _cache_regras

    _log_cinza("Buscando regras no Supabase...")
    corpo = buscar_regras_escritorio()
    _cache_regras = _montar_system_prompt_com_regras(corpo)
    _ultima_atualizacao = agora
    return _cache_regras
