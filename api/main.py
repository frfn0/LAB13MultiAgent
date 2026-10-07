"""Запуск API: python -m api.main

Приложение доступно на http://127.0.0.1:8000, документация OpenAPI -
на /docs.
"""

from __future__ import annotations

import logging
import sys

import uvicorn

from api.app import app
from orchestrator.config import get_settings

LOG_DIR = "logs"


def setup_logging(level: str, log_file: str) -> None:
    """Логи API идут в консоль и, если задан LOG_FILE, в файл."""
    from pathlib import Path

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        target = Path(log_file)
        target.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(target, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
        handlers=handlers,
        force=True,
    )
    logging.getLogger("nats").setLevel(logging.WARNING)


def main() -> int:
    """Запускает сервер API."""
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_file)

    logging.info("API поднимается на %s:%s", settings.api_host, settings.api_port)
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        access_log=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
