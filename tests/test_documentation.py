"""Проверки соответствия документации архитектуры коду.

Документация расходится с кодом незаметно: тема переименуется, компонент
переезжает, а описание остаётся прежним. Эти тесты проверяют, что темы,
компоненты и упомянутые файлы существуют на самом деле, а диаграммы
написаны на языке, который понимает GitHub.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from orchestrator import messages

ARCHITECTURE = Path("docs") / "architecture.md"

# Типы диаграмм, которые GitHub рисует через Mermaid.
MERMAID_TYPES = ("flowchart", "graph", "sequenceDiagram")


@pytest.fixture(scope="module")
def doc() -> str:
    """Текст документации архитектуры."""
    assert ARCHITECTURE.exists(), f"нет файла {ARCHITECTURE}"
    return ARCHITECTURE.read_text(encoding="utf-8")


class TestSubjects:
    """Все темы NATS должны быть описаны."""

    @pytest.mark.parametrize(
        "subject",
        [
            messages.SUBJECT_CLASSIFY,
            messages.SUBJECT_KNOWLEDGE,
            messages.SUBJECT_ANSWER,
            messages.SUBJECT_ESCALATE,
            messages.SUBJECT_RESULT,
            messages.SUBJECT_METRICS,
        ],
    )
    def test_subject_documented(self, doc: str, subject: str) -> None:
        """Тема из контракта упомянута в документации."""
        assert subject in doc, f"тема {subject} не описана"


class TestComponents:
    """Все компоненты должны быть описаны."""

    @pytest.mark.parametrize(
        "component",
        [
            "api/app.py",
            "api/state.py",
            "api/schemas.py",
            "api/main.py",
            "orchestrator/client.py",
            "orchestrator/pipeline.py",
            "orchestrator/retry.py",
            "orchestrator/metrics.py",
            "orchestrator/config.py",
            "orchestrator/messages.py",
            "pkg/agent/agent.go",
            "pkg/messages/messages.go",
            "agents/classifier",
            "agents/knowledge",
            "agents/responder",
            "agents/escalation",
            "knowledge_base/articles.json",
            "docker-compose.yml",
        ],
    )
    def test_component_documented(self, doc: str, component: str) -> None:
        """Компонент из кода упомянут в документации."""
        assert component in doc, f"компонент {component} не описан"


class TestReferencedFilesExist:
    """Упомянутые файлы должны существовать."""

    @pytest.mark.parametrize(
        "path",
        [
            "api/app.py",
            "api/state.py",
            "api/schemas.py",
            "api/main.py",
            "api/demo_api.py",
            "orchestrator/client.py",
            "orchestrator/pipeline.py",
            "orchestrator/retry.py",
            "orchestrator/metrics.py",
            "orchestrator/config.py",
            "orchestrator/messages.py",
            "orchestrator/harness.py",
            "orchestrator/demo_pipeline.py",
            "orchestrator/demo_metrics.py",
            "orchestrator/demo_retry.py",
            "orchestrator/demo_instances.py",
            "pkg/agent/agent.go",
            "pkg/messages/messages.go",
            "knowledge_base/articles.json",
            "docker-compose.yml",
        ],
    )
    def test_file_exists(self, path: str) -> None:
        """Файл, на который ссылается документация, есть в репозитории."""
        assert Path(path).exists(), f"файл {path} не найден"


class TestEnvironmentDefaults:
    """Значения по умолчанию в документации должны совпадать с кодом."""

    @pytest.mark.parametrize(
        ("name", "default"),
        [
            ("TASK_TIMEOUT", "5"),
            ("MAX_ATTEMPTS", "3"),
            ("RETRY_DELAY", "0.5"),
            ("RETRY_BACKOFF", "2.0"),
            ("RETRY_MAX_DELAY", "5.0"),
            ("RETRY_JITTER", "0.1"),
            ("API_PORT", "8000"),
            ("METRICS_INTERVAL", "10s"),
            ("MIN_CONFIDENCE", "0.25"),
        ],
    )
    def test_default_matches(self, doc: str, name: str, default: str) -> None:
        """В документации указано то же значение, что и в коде.

        Ищется строка таблицы настроек, а не первое упоминание имени:
        вне таблицы переменная встречается в тексте описания компонентов,
        где значения по умолчанию нет.
        """
        row = re.search(rf"^\|\s*`{name}`\s*\|([^\n]*)$", doc, re.MULTILINE)
        assert row is not None, f"переменная {name} не описана в таблице настроек"
        assert f"`{default}`" in row.group(1), (
            f"для {name} в документации указано не `{default}`: {row.group(0).strip()}"
        )

    def test_attempts_setting_matches_code(self, doc: str) -> None:
        """Значение MAX_ATTEMPTS совпадает с настройками по умолчанию."""
        from orchestrator.config import get_settings

        assert f"`{get_settings().max_attempts}`" in doc


class TestDiagrams:
    """Проверки диаграмм в Mermaid."""

    def test_fences_balanced(self, doc: str) -> None:
        """Блоки кода закрыты: незакрытый блок ломает разметку."""
        assert doc.count("```") % 2 == 0

    def test_diagrams_present(self, doc: str) -> None:
        """В документе есть диаграммы компонентов и последовательности."""
        mermaid_blocks = re.findall(r"```mermaid\n(.*?)```", doc, re.DOTALL)

        assert len(mermaid_blocks) >= 3, "ожидалось не меньше трёх диаграмм"

        types = [block.strip().splitlines()[0].strip() for block in mermaid_blocks]
        for diagram_type in types:
            assert any(diagram_type.startswith(kind) for kind in MERMAID_TYPES), (
                f"неизвестный тип диаграммы: {diagram_type}"
            )

    def test_components_diagram_covers_all_agents(self, doc: str) -> None:
        """Диаграмма компонентов показывает всех четырёх агентов."""
        block = re.search(r"```mermaid\n(.*?)```", doc, re.DOTALL)
        assert block is not None

        first_diagram = block.group(1)
        for agent in ("classifier", "knowledge", "responder", "escalation"):
            assert agent in first_diagram

    def test_sequence_diagram_uses_arrow_syntax(self, doc: str) -> None:
        """Диаграмма последовательности использует синтаксис Mermaid."""
        sequences = re.findall(r"```mermaid\n(sequenceDiagram.*?)```", doc, re.DOTALL)

        assert sequences, "нет диаграммы последовательности"

        for sequence in sequences:
            assert "participant " in sequence
            assert "->>" in sequence
            # Строки продолжают быть сообщениями, а не случайным текстом.
            assert sequence.count("->>") > 3


class TestStructure:
    """Проверки структуры документа."""

    @pytest.mark.parametrize(
        "section",
        [
            "## 1. Обзор",
            "## 2. Диаграмма компонентов",
            "## 6. Описание компонентов",
            "## 7. Темы NATS и сообщения",
            "## 8. Настройки",
            "## 9. Отказоустойчивость",
            "## 10. Запуск всей системы",
        ],
    )
    def test_section_present(self, doc: str, section: str) -> None:
        """Обязательный раздел документа есть."""
        assert section in doc

    def test_mentions_limitations(self, doc: str) -> None:
        """Раздел отказоустойчивости честно перечисляет ограничения."""
        assert "Чего нет и почему" in doc
        assert "JetStream" in doc
