"""Logging con X-Request-ID y middleware ASGI que lo genera/propaga."""
import contextvars
import logging
import sys
import uuid

request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


class _FiltroRid(logging.Filter):
    def filter(self, record):
        record.request_id = request_id_ctx.get()
        return True


def setup_logging(servicio: str, nivel: str = "INFO") -> logging.Logger:
    h = logging.StreamHandler(sys.stdout)
    h.addFilter(_FiltroRid())
    h.setFormatter(logging.Formatter(
        f"%(asctime)s %(levelname)s {servicio} rid=%(request_id)s %(message)s"))
    raiz = logging.getLogger()
    raiz.handlers = [h]
    raiz.setLevel(nivel)
    return logging.getLogger(servicio)


class RequestIDMiddleware:
    """Toma X-Request-ID de la petición (o genera uno), lo deja en el contexto
    de logging y lo devuelve en la respuesta."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        rid = None
        for k, v in scope["headers"]:
            if k == b"x-request-id":
                rid = v.decode()
                break
        rid = rid or uuid.uuid4().hex
        token = request_id_ctx.set(rid)

        async def send_rid(msg):
            if msg["type"] == "http.response.start":
                msg.setdefault("headers", []).append((b"x-request-id", rid.encode()))
            await send(msg)

        try:
            await self.app(scope, receive, send_rid)
        finally:
            request_id_ctx.reset(token)
