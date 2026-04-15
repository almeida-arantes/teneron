"""
Servidor HTTP para webhooks da Evolution API (WhatsApp).
Peça separada do fluxo interativo (fluxo.py / atendente.py).
"""

from __future__ import annotations

import json

import uvicorn
from fastapi import FastAPI, Request
from starlette.responses import Response

app = FastAPI(title="Banca IA Webhooks", version="0.1.0")

# ANSI: destaque no terminal (Windows Terminal / consoles com suporte a cores)
_COR = "\033[1;36m"  # ciano brilhante
_COR_CONTEUDO = "\033[1;33m"  # amarelo brilhante para o payload
_RESET = "\033[0m"


@app.post("/webhook")
async def webhook_evolution(request: Request) -> Response:
    """Recebe JSON genérico do webhook; log colorido e 200 OK."""
    try:
        corpo = await request.json()
    except Exception:
        raw = await request.body()
        corpo = raw.decode("utf-8", errors="replace")

    if isinstance(corpo, (dict, list)):
        conteudo = json.dumps(corpo, ensure_ascii=False, indent=2)
    else:
        conteudo = str(corpo)

    print(f"{_COR}Chegou um webhook:{_RESET} {_COR_CONTEUDO}[{conteudo}]{_RESET}")

    return Response(status_code=200)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
