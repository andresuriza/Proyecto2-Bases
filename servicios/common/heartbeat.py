"""Publicador de latidos de salud en Valkey: `salud:{capa}` cada 5 s (EX 15).

NO debe importarse desde registro (regla de aislamiento: registro no usa
Valkey). El estado de registro y mongo lo publica el gateway por él.
"""
import asyncio
import json
import logging
import time
from typing import Awaitable, Callable

import redis.asyncio as aioredis

log = logging.getLogger("heartbeat")
TTL_S = 15
PERIODO_S = 5


async def publicar(r, capa: str, estado: str, detalle: str = "") -> None:
    await r.set(f"salud:{capa}",
                json.dumps({"estado": estado, "detalle": detalle, "ts": int(time.time() * 1000)}),
                ex=TTL_S)


async def bucle_latidos(r, capa: str,
                        sonda: Callable[[], Awaitable[tuple[str, str]]] | None = None,
                        extra: Callable[[], Awaitable[None]] | None = None) -> None:
    """sonda() -> (estado, detalle); si falta se publica 'ok'. Nunca lanza."""
    while True:
        try:
            estado, detalle = await sonda() if sonda else ("ok", "")
            await publicar(r, capa, estado, detalle)
            if extra:
                await extra()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # Valkey caído: se reintenta en el siguiente ciclo
            log.warning("latido %s falló: %s", capa, e)
        await asyncio.sleep(PERIODO_S)
