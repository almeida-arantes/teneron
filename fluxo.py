"""
Testador do fluxo: cliente → Supervisor → SDR (mensagem WhatsApp).
"""

from __future__ import annotations

import sys

from dotenv import load_dotenv

load_dotenv()

from atendente import sdr_agent
from supervisor import AnaliseSupervisor, supervisor_agent


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    while True:
        texto = input("Mensagem do cliente: ").strip()
        resultado_supervisor = supervisor_agent.run_sync(texto)
        analise: AnaliseSupervisor = resultado_supervisor.output

        prompt_sdr = (
            "Análise do Supervisor (JSON):\n"
            f"{analise.model_dump_json(ensure_ascii=False)}\n\n"
            "Mensagem original do cliente:\n"
            f"{texto}"
        )
        resultado_sdr = sdr_agent.run_sync(prompt_sdr)
        mensagem_whatsapp: str = resultado_sdr.output

        print()
        print(mensagem_whatsapp)
        print()
