"""
Agente Supervisor com pydantic_ai: classifica e resume mensagens de clientes.
"""

from __future__ import annotations

import os
import sys
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel
from pydantic_ai import Agent

load_dotenv()

# Contas atuais da Anthropic costumam expor só a linha Claude 4.x; modelos 3.x (ex.: claude-3-7-sonnet-*)
# não aparecem em GET /v1/models e retornam 404. O default abaixo existe na API para essa geração.
# Opcional: SUPERVISOR_MODEL no .env (ex.: anthropic:claude-haiku-4-5-20251001).
_DEFAULT_MODELO = "anthropic:claude-sonnet-4-6"


class AnaliseSupervisor(BaseModel):
    """Saída estruturada da análise do Supervisor."""

    status: str
    classificacao_do_cliente: Literal["novo lead", "cliente antigo", "duvida juridica"]
    resumo_da_mensagem: str


supervisor_agent = Agent(
    os.getenv("SUPERVISOR_MODEL", _DEFAULT_MODELO),
    output_type=AnaliseSupervisor,
    instructions=(
        "Você é o Supervisor de um escritório de advocacia. "
        "Leia a mensagem do cliente e preencha o schema: "
        "status (breve, ex.: recebido ou analisado); "
        "classificacao_do_cliente deve ser exatamente uma destas strings: "
        "'novo lead', 'cliente antigo' ou 'duvida juridica'; "
        "resumo_da_mensagem com um resumo claro e objetivo do pedido."
    ),
)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    while True:
        texto = input("Mensagem do cliente: ")
        resultado = supervisor_agent.run_sync(texto)
        analise: AnaliseSupervisor = resultado.output
        print(analise.model_dump_json(indent=2, ensure_ascii=False))
        print()
