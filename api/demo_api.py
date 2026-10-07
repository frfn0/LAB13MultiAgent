"""Демонстрация REST API из задания 8.

Скрипт поднимает агентов и настоящий сервер API отдельным процессом,
обращается к нему по HTTP и печатает ответы. Проверяется весь путь:
HTTP -> API -> оркестратор -> агенты -> ответ.

Запуск:

    docker compose up -d
    python -m api.demo_api
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import httpx

from orchestrator.harness import AgentPool, build_agents

AGENTS = ("classifier", "knowledge", "responder", "escalation")
PORT = int(os.environ.get("API_PORT", "8010"))
BASE_URL = f"http://127.0.0.1:{PORT}"
LOG_FILE = "logs/api.log"

TICKETS = [
    "Не могу оплатить заказ, карта не проходит",
    "Когда придёт курьер, если заказ оформлен вчера?",
    "Хочу обсудить условия по моему тарифу, оператор не переключает",
]

failures = 0


def start_api() -> subprocess.Popen[bytes]:
    """Запускает сервер API отдельным процессом.

    Returns:
        Процесс сервера.
    """
    env = dict(os.environ)
    env["API_PORT"] = str(PORT)
    env["LOG_FILE"] = LOG_FILE
    env["LOG_LEVEL"] = "INFO"
    # Короткий таймаут: в проверке отказа агента иначе пришлось бы ждать
    # три попытки по пять секунд.
    env["TASK_TIMEOUT"] = "1.0"

    process = subprocess.Popen(
        [sys.executable, "-m", "api.main"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"  запущен API, порт {PORT}, pid {process.pid}")
    return process


def wait_ready(timeout: float = 20.0) -> bool:
    """Ждёт, пока API начнёт отвечать.

    Args:
        timeout: сколько ждать, секунды.

    Returns:
        True, если API ответил.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{BASE_URL}/health", timeout=2.0)
            if response.status_code == 200:
                return True
        except httpx.HTTPError:
            time.sleep(0.3)
    return False


def show(title: str, response: httpx.Response, fields: tuple[str, ...] = ()) -> bool:
    """Печатает ответ API и проверяет код.

    Args:
        title: заголовок.
        response: ответ HTTP.
        fields: какие поля тела показать. Пусто - показать всё.

    Returns:
        True, если код ответа 2xx.
    """
    print(f"  {title}")
    print(
        f"    HTTP {response.status_code}  X-Process-Time-Ms: "
        f"{response.headers.get('X-Process-Time-Ms', '-')}"
    )

    body = response.json()
    if fields:
        for name in fields:
            value = body.get(name)
            if isinstance(value, str):
                value = value[:70] + "..." if len(value) > 70 else value or "-"
            print(f"    {name:16} {value}")
    elif isinstance(body, dict):
        print(f"    {json.dumps(body, ensure_ascii=False)[:300]}")
    else:
        print(f"    {str(body)[:300]}")

    ok = 200 <= response.status_code < 300
    if not ok:
        print("    ОШИБКА: неожиданный код ответа")
    print()
    return ok


def main() -> int:
    """Прогоняет сценарий проверки API."""
    global failures

    print("Сборка агентов")
    build_agents(AGENTS)

    print()
    print("Запуск агентов")
    pool = AgentPool()
    for name in AGENTS:
        pool.start(name, f"{name}-api")
    time.sleep(1.5)

    api = None
    try:
        print()
        print("Запуск сервера API")
        api = start_api()
        if not wait_ready():
            print("API не поднялся, смотрите лог logs/api.log")
            return 1

        # Агенты публикуют метрики по таймеру, поэтому перед первой проверкой
        # ждём снимок: иначе в /health будет пустой список агентов.
        print("  ждём первый снимок метрик агентов")
        time.sleep(2.5)

        print()
        print("=" * 78)
        print("1. Состояние сервиса")
        print("=" * 78)
        with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
            if not show(
                "GET /health",
                client.get("/health"),
                ("status", "nats_connected", "agents", "agents_list"),
            ):
                failures += 1

            print("=" * 78)
            print("2. Синхронная обработка обращений")
            print("=" * 78)
            for text in TICKETS:
                response = client.post("/api/tickets", json={"text": text})
                if not show(
                    f"POST /api/tickets: {text[:40]}",
                    response,
                    (
                        "ticket_id",
                        "category",
                        "priority",
                        "article_title",
                        "confidence",
                        "answer_type",
                        "escalated",
                        "assigned_to",
                        "duration_ms",
                    ),
                ):
                    failures += 1

            print("=" * 78)
            print("3. Асинхронное принятие обращения")
            print("=" * 78)
            response = client.post("/api/tickets/async", json={"text": TICKETS[0]})
            accepted = show(
                "POST /api/tickets/async",
                response,
                ("ticket_id", "status", "message"),
            )
            failures += 0 if accepted else 1

            ticket_id = response.json().get("ticket_id", "")
            final = None
            for _ in range(40):
                final = client.get(f"/api/tickets/{ticket_id}")
                if final.json().get("status") != "processing":
                    break
                time.sleep(0.2)

            if final is not None:
                if not show(
                    f"GET /api/tickets/{ticket_id}",
                    final,
                    ("status", "category", "answer_type", "duration_ms"),
                ):
                    failures += 1

            print("=" * 78)
            print("4. Список обращений и метрики агентов")
            print("=" * 78)
            show(
                "GET /api/tickets?limit=5",
                client.get("/api/tickets", params={"limit": 5}),
                ("count",),
            )
            # Снимки метрик приходят по таймеру агентов, поэтому перед
            # запросом ждём: иначе в таблице будут счётчики из начала прогона.
            print("  ждём свежий снимок метрик агентов")
            time.sleep(2.5)
            agents = client.get("/api/agents")
            print("  GET /api/agents")
            print(f"    HTTP {agents.status_code}")
            if agents.status_code == 200:
                payload = agents.json()
                print(f"    агентов подключено   {payload['count']}")
                for snapshot in payload["items"]:
                    print(
                        f"    {snapshot['agent']:12} instance={snapshot['instance']:18} "
                        f"обработано={snapshot['processed']}"
                    )
                print(f"    счётчики оркестратора {payload['orchestrator']}")
            else:
                print("    ОШИБКА: неожиданный код ответа")
                failures += 1
            print()

            print("=" * 78)
            print("5. Некорректный запрос отклоняется")
            print("=" * 78)
            response = client.post("/api/tickets", json={"text": "ой"})
            print("  POST /api/tickets с текстом короче 5 символов")
            print(f"    HTTP {response.status_code}")
            if response.status_code == 422:
                print("    тело: описание ошибок валидации")
            else:
                print(f"    ОШИБКА: ожидался 422, получен {response.status_code}")
                failures += 1
            print()

            response = client.post(
                "/api/tickets", json={"text": "нормальный текст", "лишнее": 1}
            )
            print("  POST /api/tickets с лишним полем")
            print(
                f"    HTTP {response.status_code} (ожидался 422: лишние поля запрещены)"
            )
            failures += 0 if response.status_code == 422 else 1
            print()

            print("=" * 78)
            print("6. Агент не отвечает - API отвечает 504")
            print("=" * 78)
            print("  Перезапускаем агент-ответчик с FAULT_DROP=1")
            pool.stop("responder-responder-api")
            pool.start("responder", "responder-drop", {"FAULT_DROP": "1"})
            time.sleep(1.0)

            response = client.post("/api/tickets", json={"text": TICKETS[0]})
            print("  POST /api/tickets при неработающем агенте")
            print(f"    HTTP {response.status_code} (ожидался 504)")
            body = response.json()
            print(f"    error: {str(body.get('error'))[:90]}")
            if response.status_code != 504:
                failures += 1
                print("    ОШИБКА: ожидался 504")
            print()
    finally:
        pool.stop_all()
        if api is not None:
            api.terminate()
            try:
                api.wait(timeout=10)
            except subprocess.TimeoutExpired:
                api.kill()

    print("=" * 78)
    print(f"Проверок не пройдено: {failures}")
    print("=" * 78)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
