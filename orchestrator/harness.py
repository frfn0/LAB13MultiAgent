"""Общие мелочи для запуска агентов из демонстраций.

Демонстрации заданий 6 и 7 сами собирают агентов, поднимают их и
останавливают. Эта логика вынесена сюда, чтобы не дублировать её в каждом
сценарии.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

BIN_DIR = Path(os.environ.get("AGENT_BIN_DIR", ".task6-bin"))
LOG_DIR = Path("logs")
SUFFIX = ".exe" if os.name == "nt" else ""


def build_agents(names: tuple[str, ...] | list[str]) -> None:
    """Собирает агентов в отдельный каталог.

    Args:
        names: имена агентов, например ("classifier", "knowledge").
    """
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    for name in names:
        print(f"  сборка {name} -> {BIN_DIR / f'{name}{SUFFIX}'}")
        subprocess.run(
            ["go", "build", "-o", str(BIN_DIR / f"{name}{SUFFIX}"), f"./agents/{name}"],
            check=True,
        )


def agent_env(
    name: str, instance: str, extra: dict[str, str] | None = None
) -> dict[str, str]:
    """Окружение для запуска экземпляра агента.

    Из FAULT_* переменных, оставшихся в окружении процесса, чистимся:
    иначе сбой, включённый одной фазой, утечёт в следующую.

    Args:
        name: имя агента.
        instance: идентификатор экземпляра.
        extra: дополнительные переменные, например FAULT_* из задания 6.

    Returns:
        Копия окружения процесса с настройками агента.
    """
    env = dict(os.environ)
    env["INSTANCE_ID"] = instance
    env["LOG_FILE"] = str(LOG_DIR / f"{name}-{instance}.log")
    env["LOG_LEVEL"] = "INFO"
    env["METRICS_INTERVAL"] = "2s"
    for key in ("FAULT_FAIL_ATTEMPTS", "FAULT_DELAY_MS", "FAULT_DROP"):
        env.pop(key, None)
    env.update(extra or {})
    return env


def start_agent(
    name: str,
    instance: str,
    extra: dict[str, str] | None = None,
) -> subprocess.Popen[bytes]:
    """Запускает экземпляр агента в фоне.

    Args:
        name: имя агента.
        instance: идентификатор экземпляра.
        extra: дополнительные переменные окружения.

    Returns:
        Запущенный процесс.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(
        [str(BIN_DIR / f"{name}{SUFFIX}")],
        env=agent_env(name, instance, extra),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"  запущен {name} instance={instance} pid={process.pid} {extra or ''}")
    return process


def stop_agent(process: subprocess.Popen[bytes] | None) -> None:
    """Останавливает агента и дожидается завершения.

    Args:
        process: процесс агента. None пропускается.
    """
    if process is None:
        return

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


@dataclass
class AgentPool:
    """Процессы агентов, которые нужно остановить в конце сценария."""

    processes: dict[str, subprocess.Popen[bytes]] = field(default_factory=dict)

    def start(
        self, name: str, instance: str, extra: dict[str, str] | None = None
    ) -> None:
        """Запускает экземпляр и запоминает его под ключом «имя-экземпляр»."""
        key = f"{name}-{instance}"
        self.processes[key] = start_agent(name, instance, extra)

    def stop(self, key: str) -> None:
        """Останавливает экземпляр по ключу."""
        stop_agent(self.processes.pop(key, None))

    def get(self, key: str) -> subprocess.Popen[bytes] | None:
        """Процесс экземпляра по ключу."""
        return self.processes.get(key)

    def stop_all(self) -> None:
        """Останавливает все оставшиеся экземпляры."""
        for key in list(self.processes):
            self.stop(key)
