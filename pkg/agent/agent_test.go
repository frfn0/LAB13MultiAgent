package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// newTestAgent собирает агента с заглушкой обработчика и без подключения
// к NATS: проверяется логика обвязки, а не работа с брокером.
func newTestAgent(t *testing.T, handler Handler, mutate func(*Config)) (*Agent, *[]messages.Result) {
	t.Helper()

	var published []messages.Result

	cfg := Config{
		Name:            "test-agent",
		Subject:         messages.SubjectClassify,
		Queue:           "testers",
		Instance:        "test-1",
		MetricsInterval: 0,
	}
	if mutate != nil {
		mutate(&cfg)
	}

	// Обработчик оборачивается так, чтобы тест видел, с каким заданием он
	// был вызван и какой результат агент собирался опубликовать.
	wrapped := func(ctx context.Context, task messages.Task) (messages.Result, error) {
		return handler(ctx, task)
	}

	instance, err := New(cfg, wrapped)
	if err != nil {
		t.Fatalf("агент не создан: %v", err)
	}

	return instance, &published
}

// taskMessage собирает сообщение NATS с заданием.
func taskMessage(t *testing.T, task messages.Task) *nats.Msg {
	t.Helper()

	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("задание не сериализовано: %v", err)
	}

	return &nats.Msg{Subject: messages.SubjectClassify, Data: payload}
}

// okHandler - обработчик, который всегда отвечает успехом.
func okHandler(_ context.Context, task messages.Task) (messages.Result, error) {
	return messages.Result{Category: messages.CategoryBilling, Priority: 4}, nil
}

// TestNewRequiresHandler проверяет, что агент без обработчика не создаётся.
func TestNewRequiresHandler(t *testing.T) {
	t.Parallel()

	if _, err := New(Config{Name: "test-agent"}, nil); err == nil {
		t.Error("агент создан без обработчика")
	}
}

// TestLoadConfigReadsEnvironment проверяет разбор переменных окружения.
func TestLoadConfigReadsEnvironment(t *testing.T) {
	t.Setenv("INSTANCE_ID", "classifier-3")
	t.Setenv("METRICS_INTERVAL", "1500ms")
	t.Setenv("FAULT_FAIL_ATTEMPTS", "2")
	t.Setenv("FAULT_DROP", "1")
	t.Setenv("LOG_LEVEL", "DEBUG")

	cfg, err := LoadConfig("classifier", messages.SubjectClassify, "classifiers")
	if err != nil {
		t.Fatalf("настройки не прочитаны: %v", err)
	}

	if cfg.Instance != "classifier-3" {
		t.Errorf("экземпляр: получено %q, ожидалось classifier-3", cfg.Instance)
	}
	if cfg.MetricsInterval != 1500*time.Millisecond {
		t.Errorf("интервал метрик: получено %v, ожидалось 1.5s", cfg.MetricsInterval)
	}
	if !cfg.Verbose {
		t.Error("LOG_LEVEL=DEBUG должен включать подробные записи")
	}
	if cfg.Fault.FailAttempts != 2 {
		t.Errorf("число сбоев: получено %d, ожидалось 2", cfg.Fault.FailAttempts)
	}
	if !cfg.Fault.Drop {
		t.Error("FAULT_DROP=1 должен включать отказ от ответа")
	}
	if !cfg.Fault.enabled() {
		t.Error("Fault.enabled должен быть истиной при заданных сбоях")
	}
}

// TestLoadConfigRejectsBadValues проверяет, что неверные значения
// переменных приводят к ошибке, а не к молчаливым сюрпризам в работе.
func TestLoadConfigRejectsBadValues(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"интервал метрик", "METRICS_INTERVAL"},
		{"число сбоев", "FAULT_FAIL_ATTEMPTS"},
		{"задержка сбоя", "FAULT_DELAY_MS"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv(testCase.key, "не-число")

			if _, err := LoadConfig("classifier", messages.SubjectClassify, "classifiers"); err == nil {
				t.Errorf("%s: неверное значение %s принято", testCase.name, testCase.key)
			}
		})
	}
}

// TestLoadConfigRequiresNameAndSubject проверяет обязательные параметры.
func TestLoadConfigRequiresNameAndSubject(t *testing.T) {
	t.Parallel()

	if _, err := LoadConfig("", messages.SubjectClassify, "classifiers"); err == nil {
		t.Error("агент создан без имени")
	}

	if _, err := LoadConfig("classifier", "", "classifiers"); err == nil {
		t.Error("агент создан без темы подписки")
	}
}

// TestHandleMessageSuccess проверяет обычный путь: задание выполнено,
// счётчики обновились, в ответе видно имя агента и экземпляра.
func TestHandleMessageSuccess(t *testing.T) {
	t.Parallel()

	instance, _ := newTestAgent(t, okHandler, nil)

	task := messages.Task{
		ID:     "task-1",
		Type:   messages.TaskClassify,
		Ticket: messages.Ticket{ID: "ticket-1", Text: "не проходит оплата"},
	}

	instance.handleMessage(context.Background(), taskMessage(t, task))

	if got := instance.Stats()["processed"]; got != int64(1) {
		t.Errorf("обработано заданий: получено %v, ожидалось 1", got)
	}
	if got := instance.Stats()["received"]; got != int64(1) {
		t.Errorf("получено заданий: получено %v, ожидалось 1", got)
	}
	if got := instance.Stats()["failed"]; got != int64(0) {
		t.Errorf("ошибок: получено %v, ожидалось 0", got)
	}

	payload, err := instance.prepareResult(messages.Result{TaskID: "task-1", Success: true})
	if err != nil {
		t.Fatalf("результат не подготовлен: %v", err)
	}

	var result messages.Result
	if err := json.Unmarshal(payload, &result); err != nil {
		t.Fatalf("результат не разобран: %v", err)
	}
	if result.Instance != "test-1" {
		t.Errorf("экземпляр в ответе: получено %q, ожидалось test-1", result.Instance)
	}
}

// TestPrepareResultKeepsExplicitInstance проверяет, что явный экземпляр не
// перетирается: агент подставляет свой только когда поле пустое.
func TestPrepareResultKeepsExplicitInstance(t *testing.T) {
	t.Parallel()

	instance, _ := newTestAgent(t, okHandler, nil)

	payload, err := instance.prepareResult(messages.Result{Instance: "classifier-9"})
	if err != nil {
		t.Fatalf("результат не подготовлен: %v", err)
	}

	if !strings.Contains(string(payload), `"instance":"classifier-9"`) {
		t.Errorf("явный экземпляр перетёрт: %s", payload)
	}
}

// TestHandleMessageHandlerError проверяет, что ошибка обработчика
// учитывается в счётчике ошибок.
func TestHandleMessageHandlerError(t *testing.T) {
	t.Parallel()

	failing := func(_ context.Context, _ messages.Task) (messages.Result, error) {
		return messages.Result{}, ErrInjectedFault
	}

	instance, _ := newTestAgent(t, failing, nil)

	task := messages.Task{ID: "task-2", Ticket: messages.Ticket{ID: "ticket-2", Text: "текст"}}
	instance.handleMessage(context.Background(), taskMessage(t, task))

	if got := instance.Stats()["failed"]; got != int64(1) {
		t.Errorf("ошибок: получено %v, ожидалось 1", got)
	}
	if got := instance.Stats()["processed"]; got != int64(0) {
		t.Errorf("обработано заданий: получено %v, ожидалось 0", got)
	}
}

// TestHandleMessageBrokenJSON проверяет, что неразобранное задание не
// роняет агента: он считает ошибку и продолжает работу.
func TestHandleMessageBrokenJSON(t *testing.T) {
	t.Parallel()

	instance, _ := newTestAgent(t, okHandler, nil)

	instance.handleMessage(context.Background(), &nats.Msg{Data: []byte("{не json")})

	if got := instance.Stats()["failed"]; got != int64(1) {
		t.Errorf("ошибок: получено %v, ожидалось 1", got)
	}
	if got := instance.Stats()["processed"]; got != int64(0) {
		t.Errorf("обработано заданий: получено %v, ожидалось 0", got)
	}
}

// TestHandleMessageWithoutTicketID проверяет проверку обязательного поля.
func TestHandleMessageWithoutTicketID(t *testing.T) {
	t.Parallel()

	instance, _ := newTestAgent(t, okHandler, nil)

	instance.handleMessage(context.Background(), taskMessage(t, messages.Task{ID: "task-3"}))

	if got := instance.Stats()["failed"]; got != int64(1) {
		t.Errorf("ошибок: получено %v, ожидалось 1", got)
	}
}

// TestFaultFailAttempts проверяет искусственный сбой из задания 6:
// агент отвечает ошибкой на первые N заданий, дальше работает.
func TestFaultFailAttempts(t *testing.T) {
	t.Parallel()

	instance, _ := newTestAgent(t, okHandler, func(cfg *Config) {
		cfg.Fault = Fault{FailAttempts: 2}
	})

	for index := 0; index < 4; index++ {
		task := messages.Task{
			ID:     "task-fault",
			Ticket: messages.Ticket{ID: "ticket-fault", Text: "текст"},
		}
		instance.handleMessage(context.Background(), taskMessage(t, task))
	}

	stats := instance.Stats()
	if stats["failed"] != int64(2) {
		t.Errorf("ошибок: получено %v, ожидалось 2", stats["failed"])
	}
	if stats["processed"] != int64(2) {
		t.Errorf("обработано заданий: получено %v, ожидалось 2", stats["processed"])
	}
	if stats["received"] != int64(4) {
		t.Errorf("получено заданий: получено %v, ожидалось 4", stats["received"])
	}
}

// TestFaultDropKeepsSilence проверяет, что при FAULT_DROP агент принимает
// задание, но не отвечает: так проверяется таймаут на стороне
// оркестратора.
func TestFaultDropKeepsSilence(t *testing.T) {
	t.Parallel()

	instance, _ := newTestAgent(t, okHandler, func(cfg *Config) {
		cfg.Fault = Fault{Drop: true}
	})

	task := messages.Task{ID: "task-drop", Ticket: messages.Ticket{ID: "ticket-drop", Text: "текст"}}
	instance.handleMessage(context.Background(), taskMessage(t, task))

	stats := instance.Stats()
	if stats["received"] != int64(1) {
		t.Errorf("задание должно быть получено: %v", stats["received"])
	}
	if stats["processed"] != int64(0) || stats["failed"] != int64(0) {
		t.Errorf("при отказе от ответа задание не должно быть ни выполнено, "+
			"ни провалено: %+v", stats)
	}
}

// TestFaultDelay проверяет, что задержка перед ответом действительно
// растягивает обработку.
func TestFaultDelay(t *testing.T) {
	t.Parallel()

	instance, _ := newTestAgent(t, okHandler, func(cfg *Config) {
		cfg.Fault = Fault{Delay: 40 * time.Millisecond}
	})

	task := messages.Task{ID: "task-slow", Ticket: messages.Ticket{ID: "ticket-slow", Text: "текст"}}

	begin := time.Now()
	instance.handleMessage(context.Background(), taskMessage(t, task))
	elapsed := time.Since(begin)

	if elapsed < 40*time.Millisecond {
		t.Errorf("обработка заняла %v, ожидалось не меньше 40ms", elapsed)
	}
	if got := instance.Stats()["processed"]; got != int64(1) {
		t.Errorf("после задержки задание должно быть обработано: %v", got)
	}
}

// TestPublishWithoutConnection проверяет, что агент без подключения к NATS
// не падает при попытке отправить результат.
func TestPublishWithoutConnection(t *testing.T) {
	t.Parallel()

	instance, _ := newTestAgent(t, okHandler, nil)
	instance.publish(messages.Result{TaskID: "task-4", Success: true})

	if got := instance.Stats()["processed"]; got != int64(0) {
		t.Errorf("публикация без соединения не должна менять счётчики: %v", got)
	}
}

// TestNewLoggerWritesFile проверяет, что при LOG_FILE записи идут в файл.
func TestNewLoggerWritesFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "agent.log")

	logger, closeLog, err := newLogger(Config{Name: "test-agent", LogFile: path})
	if err != nil {
		t.Fatalf("логгер не создан: %v", err)
	}

	logger.Info("проверка записи в файл", "agent", "test-agent")
	closeLog()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("файл лога не прочитан: %v", err)
	}

	text := string(content)
	if !strings.Contains(text, "проверка записи в файл") {
		t.Errorf("сообщение не попало в файл: %s", text)
	}
	if !strings.Contains(text, "agent=test-agent") {
		t.Errorf("атрибут не попал в файл: %s", text)
	}
}

// TestDefaultInstanceID проверяет, что без INSTANCE_ID идентификатор
// собирается из имени хоста, а не остаётся пустым: иначе метрики
// нескольких экземпляров неразличимы.
func TestDefaultInstanceID(t *testing.T) {
	t.Setenv("INSTANCE_ID", "")
	t.Setenv("AGENT_PORT", "")

	first := defaultInstanceID()
	if first == "" {
		t.Fatal("идентификатор экземпляра не собран")
	}
	if !strings.Contains(first, ":") {
		t.Errorf("идентификатор %q не содержит порт", first)
	}

	t.Setenv("AGENT_PORT", "9001")

	if second := defaultInstanceID(); second == first {
		t.Errorf("идентификатор не изменился после задания AGENT_PORT: %q", second)
	}
}

// TestEnvOrFallback проверяет вспомогательную функцию настройки.
func TestEnvOrFallback(t *testing.T) {
	t.Setenv("LAB13_TEST_VALUE", "")

	if got := envOr("LAB13_TEST_VALUE", "запасное"); got != "запасное" {
		t.Errorf("пустая переменная: получено %q, ожидалось запасное", got)
	}

	t.Setenv("LAB13_TEST_VALUE", "своё")

	if got := envOr("LAB13_TEST_VALUE", "запасное"); got != "своё" {
		t.Errorf("заданная переменная: получено %q, ожидалось своё", got)
	}
}
