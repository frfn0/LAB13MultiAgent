package messages

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestTaskJSONRoundTrip проверяет, что задание переживает
// преобразование в JSON и обратно без потери полей.
func TestTaskJSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := Task{
		ID:           "task-1",
		Type:         TaskAnswer,
		Ticket:       Ticket{ID: "ticket-1", Text: "не проходит оплата", Author: "client"},
		Category:     CategoryBilling,
		Priority:     4,
		Tags:         []string{"money"},
		ArticleID:    7,
		ArticleTitle: "Не проходит оплата банковской картой",
		Confidence:   0.8,
		Found:        true,
		Attempts:     2,
	}

	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("задание не сериализовано: %v", err)
	}

	var restored Task
	if err := json.Unmarshal(payload, &restored); err != nil {
		t.Fatalf("задание не разобрано: %v", err)
	}

	if restored.ID != original.ID || restored.Type != original.Type {
		t.Errorf("идентификатор или тип задания потерян: %+v", restored)
	}
	if restored.Ticket != original.Ticket {
		t.Errorf("тикет не совпадает: получено %+v, ожидалось %+v", restored.Ticket, original.Ticket)
	}
	if restored.Category != CategoryBilling || restored.Priority != 4 {
		t.Errorf("результат классификации потерян: %+v", restored)
	}
	if len(restored.Tags) != 1 || restored.Tags[0] != "money" {
		t.Errorf("теги потеряны: %v", restored.Tags)
	}
	if restored.ArticleID != 7 || !restored.Found || restored.Confidence != 0.8 {
		t.Errorf("результат поиска потерян: %+v", restored)
	}
	if restored.Attempts != 2 {
		t.Errorf("число попыток потеряно: %d", restored.Attempts)
	}
}

// TestResultOmitsEmptyFields проверяет, что незаполненные поля не
// попадают в JSON: так ответы агентов остаются компактными.
func TestResultOmitsEmptyFields(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(Result{TaskID: "task-1", Agent: "classifier", Success: true})
	if err != nil {
		t.Fatalf("результат не сериализован: %v", err)
	}

	text := string(payload)

	for _, field := range []string{"error", "article_id", "answer", "escalation_id", "instance"} {
		if strings.Contains(text, `"`+field+`"`) {
			t.Errorf("пустое поле %q попало в ответ: %s", field, text)
		}
	}
	if !strings.Contains(text, `"success":true`) {
		t.Errorf("признак успеха должен попасть в ответ: %s", text)
	}
}

// TestResultKeepsInstance проверяет, что идентификатор экземпляра
// попадает в ответ: по нему видно, кто из нескольких одинаковых агентов
// выполнил задание.
func TestResultKeepsInstance(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(Result{
		TaskID:   "task-1",
		Agent:    "classifier",
		Instance: "classifier-2",
		Success:  true,
	})
	if err != nil {
		t.Fatalf("результат не сериализован: %v", err)
	}

	if !strings.Contains(string(payload), `"instance":"classifier-2"`) {
		t.Errorf("экземпляр не попал в ответ: %s", payload)
	}
}

// TestResultParsesAgentError проверяет, что ответ с ошибкой читается
// обратно: оркестратор должен отличать сбой агента от успеха.
func TestResultParsesAgentError(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"task_id":"task-9","agent":"responder","success":false,` +
		`"error":"текст обращения пуст"}`)

	var result Result
	if err := json.Unmarshal(payload, &result); err != nil {
		t.Fatalf("ответ не разобран: %v", err)
	}

	if result.Success {
		t.Error("успех разобран неверно")
	}
	if result.Error != "текст обращения пуст" {
		t.Errorf("описание ошибки потеряно: %q", result.Error)
	}
	if result.TaskID != "task-9" {
		t.Errorf("идентификатор задания потерян: %q", result.TaskID)
	}
}

// TestMetricsJSON проверяет разбор снимка метрик агента: именно его
// оркестратор собирает из темы agent.metrics.
func TestMetricsJSON(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"agent":"knowledge","queue":"knowledge-agents",` +
		`"instance":"knowledge-1","received":5,"processed":4,"failed":1,"uptime_seconds":12}`)

	var metrics Metrics
	if err := json.Unmarshal(payload, &metrics); err != nil {
		t.Fatalf("метрики не разобраны: %v", err)
	}

	if metrics.Agent != "knowledge" || metrics.Queue != "knowledge-agents" {
		t.Errorf("агент или очередь разобраны неверно: %+v", metrics)
	}
	if metrics.Instance != "knowledge-1" {
		t.Errorf("экземпляр разобран неверно: %q", metrics.Instance)
	}
	if metrics.Received != 5 || metrics.Processed != 4 || metrics.Failed != 1 {
		t.Errorf("счётчики разобраны неверно: %+v", metrics)
	}
	if metrics.UptimeSeconds != 12 {
		t.Errorf("время работы разобрано неверно: %d", metrics.UptimeSeconds)
	}
}

// TestSubjectsAreStable проверяет темы NATS: они зафиксированы в
// agents/AGENTS.md, и смена строки сломала бы весь конвейер.
func TestSubjectsAreStable(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"классификация": SubjectClassify,
		"база знаний":   SubjectKnowledge,
		"ответ":         SubjectAnswer,
		"эскалация":     SubjectEscalate,
		"результат":     SubjectResult,
		"метрики":       SubjectMetrics,
	}

	want := map[string]string{
		"классификация": "ticket.classify",
		"база знаний":   "ticket.knowledge",
		"ответ":         "ticket.answer",
		"эскалация":     "ticket.escalate",
		"результат":     "ticket.result",
		"метрики":       "agent.metrics",
	}

	for name, got := range cases {
		if got != want[name] {
			t.Errorf("тема %s: получено %q, ожидалось %q", name, got, want[name])
		}
	}
}
