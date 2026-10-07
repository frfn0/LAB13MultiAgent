// Package agent содержит базовую обвязку агента: подключение к NATS,
// подписка на очередь, обработка заданий и публикация результатов.
//
// Каждый агент реализует только свою логику обработки Handler, а всё
// остальное - подключение, разбор JSON, счётчики, обработка ошибок и
// завершение работы - берётся отсюда.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// ErrInvalidTask - задание не прошло проверку и не может быть обработано.
var ErrInvalidTask = errors.New("задание некорректно")

// ErrInjectedFault - искусственный сбой, включённый переменными FAULT_*.
// Нужен, чтобы показать, как оркестратор повторяет задание при сбое агента.
var ErrInjectedFault = errors.New("искусственный сбой агента для проверки повторов")

// Handler обрабатывает одно задание и возвращает результат.
//
// Агент возвращает ошибку, если задание обработать нельзя. Ошибка
// попадает в Result и в лог, а задание не считается успешным.
type Handler func(ctx context.Context, task messages.Task) (messages.Result, error)

// Config - параметры агента из переменных окружения.
type Config struct {
	// Name - имя агента, попадает в логи и в поле Result.Agent.
	Name string
	// Subject - тема, на которую агент подписывается.
	Subject string
	// Queue - имя группы очереди. Несколько экземпляров одного агента
	// работают с одним Queue и делят задания между собой.
	Queue string
	// NatsURL - адрес NATS-сервера.
	NatsURL string
	// LogFile - путь к файлу лога. Пустая строка означает вывод в stderr.
	LogFile string
	// Verbose - уровень логирования DEBUG вместо INFO.
	Verbose bool
	// Instance - идентификатор процесса. Нужен, чтобы отличить
	// несколько экземпляров одного агента в метриках.
	Instance string
	// MetricsInterval - как часто публиковать счётчики. Ноль отключает
	// публикацию.
	MetricsInterval time.Duration
	// Fault - искусственные сбои агента для проверки повторов и таймаутов.
	Fault Fault
}

// Fault - искусственные сбои агента.
//
// По умолчанию агент работает без вмешательства. Переменные окружения
// FAULT_FAIL_ATTEMPTS, FAULT_DELAY_MS и FAULT_DROP включают сбои, чтобы
// можно было проверить, как оркестратор ведёт себя при сбое агента.
type Fault struct {
	// FailAttempts - сколько первых заданий агент выполнит с ошибкой.
	FailAttempts int
	// Delay - задержка перед обработкой задания.
	Delay time.Duration
	// Drop - если true, агент принимает задание, но не отвечает на него.
	// Оркестратор должен упереться в таймаут.
	Drop bool
}

// enabled сообщает, включены ли искусственные сбои.
func (f Fault) enabled() bool {
	return f.FailAttempts > 0 || f.Delay > 0 || f.Drop
}

// loadFault читает параметры искусственных сбоев из переменных окружения.
func loadFault() (Fault, error) {
	var fault Fault

	if raw := os.Getenv("FAULT_FAIL_ATTEMPTS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return Fault{}, fmt.Errorf("FAULT_FAIL_ATTEMPTS некорректен: %q", raw)
		}
		fault.FailAttempts = parsed
	}

	if raw := os.Getenv("FAULT_DELAY_MS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return Fault{}, fmt.Errorf("FAULT_DELAY_MS некорректен: %q", raw)
		}
		fault.Delay = time.Duration(parsed) * time.Millisecond
	}

	fault.Drop = os.Getenv("FAULT_DROP") == "1"

	return fault, nil
}

// LoadConfig читает настройки агента из переменных окружения.
func LoadConfig(name, subject, queue string) (Config, error) {
	if name == "" {
		return Config{}, errors.New("имя агента не задано")
	}
	if subject == "" {
		return Config{}, errors.New("тема подписки не задана")
	}

	verbose := os.Getenv("LOG_LEVEL") == "DEBUG"

	instance := envOr("INSTANCE_ID", defaultInstanceID())

	interval := 10 * time.Second
	if raw := os.Getenv("METRICS_INTERVAL"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("METRICS_INTERVAL некорректен: %w", err)
		}
		interval = parsed
	}

	fault, err := loadFault()
	if err != nil {
		return Config{}, err
	}

	return Config{
		Name:            name,
		Subject:         subject,
		Queue:           queue,
		NatsURL:         envOr("NATS_URL", nats.DefaultURL),
		LogFile:         os.Getenv("LOG_FILE"),
		Verbose:         verbose,
		Instance:        instance,
		MetricsInterval: interval,
		Fault:           fault,
	}, nil
}

// defaultInstanceID строит идентификатор процесса из имени хоста и порта
// запуска агента. Нужен, чтобы в метриках различались экземпляры одного
// агента - см. задание 7.
func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}

	return fmt.Sprintf("%s:%s", host, envOr("AGENT_PORT", "0"))
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// Stats - счётчики агента, которые можно использовать как метрики.
type Stats struct {
	// Processed - сколько заданий успешно обработано.
	Processed atomic.Int64
	// Failed - сколько заданий завершилось ошибкой.
	Failed atomic.Int64
	// Received - сколько заданий получено всего.
	Received atomic.Int64
	// StartedAt - время запуска агента.
	StartedAt time.Time
}

func (s *Stats) Snapshot() map[string]any {
	return map[string]any{
		"processed": s.Processed.Load(),
		"failed":    s.Failed.Load(),
		"received":  s.Received.Load(),
		"uptime_s":  int64(time.Since(s.StartedAt).Seconds()),
	}
}

// Agent - базовый агент.
type Agent struct {
	cfg     Config
	nc      *nats.Conn
	logger  *slog.Logger
	stats   *Stats
	handler Handler
	// closeLog закрывает файл лога при остановке агента.
	closeLog func()
	// faultHits - сколько раз сработал искусственный сбой FAULT_FAIL_ATTEMPTS.
	faultHits atomic.Int64
}

// New создаёт агента и готовит логгер.
func New(cfg Config, handler Handler) (*Agent, error) {
	if handler == nil {
		return nil, errors.New("обработчик заданий не задан")
	}

	logger, closeLog, err := newLogger(cfg)
	if err != nil {
		return nil, err
	}

	return &Agent{
		cfg:      cfg,
		logger:   logger,
		handler:  handler,
		stats:    &Stats{StartedAt: time.Now()},
		closeLog: closeLog,
	}, nil
}

// newLogger создаёт логгер. Если задан LOG_FILE, пишем в файл и в stderr,
// иначе только в stderr.
func newLogger(cfg Config) (*slog.Logger, func(), error) {
	level := slog.LevelInfo
	if cfg.Verbose {
		level = slog.LevelDebug
	}

	var writers = []any{os.Stderr}
	closers := []func(){}

	if cfg.LogFile != "" {
		file, err := os.OpenFile(cfg.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("не удалось открыть файл лога %s: %w", cfg.LogFile, err)
		}
		writers = append(writers, file)
		closers = append(closers, func() { _ = file.Close() })
	}

	handler := slog.NewTextHandler(
		ioMultiWriter(writers),
		&slog.HandlerOptions{Level: level},
	)

	closeAll := func() {
		for _, closer := range closers {
			closer()
		}
	}

	return slog.New(handler), closeAll, nil
}

// Run подключается к NATS, подписывается на очередь и обрабатывает задания
// до получения сигнала остановки.
func (a *Agent) Run(ctx context.Context) error {
	defer a.closeLog()

	opts := []nats.Option{
		nats.Name(a.cfg.Name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.Timeout(5 * time.Second),
	}

	nc, err := nats.Connect(a.cfg.NatsURL, opts...)
	if err != nil {
		return fmt.Errorf("не удалось подключиться к NATS %s: %w", a.cfg.NatsURL, err)
	}
	a.nc = nc
	defer nc.Close()

	a.logger.Info("агент запущен",
		"agent", a.cfg.Name,
		"subject", a.cfg.Subject,
		"queue", a.cfg.Queue,
		"nats", a.cfg.NatsURL,
	)

	if a.cfg.Fault.enabled() {
		a.logger.Warn("включены искусственные сбои агента",
			"fail_attempts", a.cfg.Fault.FailAttempts,
			"delay", a.cfg.Fault.Delay.String(),
			"drop", a.cfg.Fault.Drop,
		)
	}

	handler := func(msg *nats.Msg) {
		a.handleMessage(ctx, msg)
	}

	var sub *nats.Subscription
	if a.cfg.Queue != "" {
		sub, err = nc.QueueSubscribe(a.cfg.Subject, a.cfg.Queue, handler)
	} else {
		sub, err = nc.Subscribe(a.cfg.Subject, handler)
	}
	if err != nil {
		return fmt.Errorf("не удалось подписаться на %s: %w", a.cfg.Subject, err)
	}
	defer sub.Unsubscribe()

	if err := nc.Flush(); err != nil {
		return fmt.Errorf("не удалось подтвердить подписку: %w", err)
	}

	stopMetrics := a.startMetricsPublishing(ctx)

	<-ctx.Done()
	a.logger.Info("агент останавливается", "agent", a.cfg.Name)

	// Даём в работе завершиться текущим заданиям.
	if err := nc.Drain(); err != nil {
		a.logger.Warn("дренирование не завершилось", "error", err)
	}

	stopMetrics()

	a.logger.Info("итоговые счётчики", "stats", a.stats.Snapshot())
	return nil
}

// startMetricsPublishing начинает периодическую публикацию счётчиков в тему
// agent.metrics. Возвращает функцию остановки.
func (a *Agent) startMetricsPublishing(_ context.Context) func() {
	if a.cfg.MetricsInterval <= 0 {
		a.logger.Info("публикация метрик отключена", "agent", a.cfg.Name)
		return func() {}
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Отдельный контекст: остановка агента не должна обрывать последнюю
	// публикацию, она нужна после Drain.
	publisherCtx, cancel := context.WithCancel(context.Background())

	go func() {
		defer wg.Done()

		ticker := time.NewTicker(a.cfg.MetricsInterval)
		defer ticker.Stop()

		// Первый снимок уходит сразу, чтобы метрики появились не позже
		// первого тика.
		a.publishMetrics()

		for {
			select {
			case <-publisherCtx.Done():
				return
			case <-ticker.C:
				a.publishMetrics()
			}
		}
	}()

	a.logger.Info("публикация метрик запущена",
		"agent", a.cfg.Name,
		"interval", a.cfg.MetricsInterval.String(),
	)

	return func() {
		// Финальный снимок после остановки агента.
		a.publishMetrics()
		cancel()
		wg.Wait()
	}
}

// publishMetrics отправляет текущие счётчики в тему метрик.
func (a *Agent) publishMetrics() {
	if !a.connected() {
		return
	}

	payload, err := json.Marshal(a.Metrics())
	if err != nil {
		a.logger.Error("метрики не сериализованы", "error", err)
		return
	}

	if err := a.nc.Publish(messages.SubjectMetrics, payload); err != nil {
		a.logger.Error("метрики не отправлены", "error", err)
	}
}

// Metrics возвращает счётчики агента в виде структуры сообщений.
func (a *Agent) Metrics() messages.Metrics {
	return messages.Metrics{
		Agent:         a.cfg.Name,
		Queue:         a.cfg.Queue,
		Instance:      a.cfg.Instance,
		Received:      a.stats.Received.Load(),
		Processed:     a.stats.Processed.Load(),
		Failed:        a.stats.Failed.Load(),
		UptimeSeconds: int64(time.Since(a.stats.StartedAt).Seconds()),
	}
}

// connected проверяет, что соединение с NATS живо.
func (a *Agent) connected() bool {
	return a.nc != nil && !a.nc.IsClosed()
}

// handleMessage разбирает задание, вызывает обработчик и публикует результат.
func (a *Agent) handleMessage(ctx context.Context, msg *nats.Msg) {
	a.stats.Received.Add(1)

	var task messages.Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		a.stats.Failed.Add(1)
		a.logger.Error("задание не разобрано", "error", err)
		a.publishError("", fmt.Errorf("некорректный JSON: %w", err))
		return
	}

	if task.Ticket.ID == "" {
		a.stats.Failed.Add(1)
		a.logger.Error("в задании нет идентификатора тикета", "task_id", task.ID)
		a.publishError(task.ID, ErrInvalidTask)
		return
	}

	// Искусственные сбои: так проверяется поведение оркестратора при
	// сбое агента. По умолчанию выключены.
	if a.cfg.Fault.Drop {
		// Задание принято, но ответа не будет - так ведёт себя агент,
		// который завис. Оркестратор должен упереться в таймаут.
		a.logger.Warn("ответ на задание не будет отправлен",
			"task_id", task.ID,
			"ticket", task.Ticket.ID,
			"reason", "FAULT_DROP",
		)
		return
	}

	if a.cfg.Fault.Delay > 0 {
		select {
		case <-ctx.Done():
			a.logger.Warn("задание снято во время искусственной задержки",
				"task_id", task.ID,
			)
			return
		case <-time.After(a.cfg.Fault.Delay):
		}
	}

	if a.cfg.Fault.FailAttempts > 0 && a.faultHits.Load() < int64(a.cfg.Fault.FailAttempts) {
		a.faultHits.Add(1)
		a.stats.Failed.Add(1)
		a.logger.Warn("искусственный сбой агента",
			"task_id", task.ID,
			"ticket", task.Ticket.ID,
			"fail_attempts", a.cfg.Fault.FailAttempts,
		)
		a.publish(messages.Result{
			TaskID: task.ID,
			Agent:  a.cfg.Name,
			Error:  ErrInjectedFault.Error(),
		})
		return
	}

	start := time.Now()
	result, err := a.handler(ctx, task)
	elapsed := time.Since(start)

	if err != nil {
		a.stats.Failed.Add(1)
		a.logger.Error("задание не выполнено",
			"task_id", task.ID,
			"ticket", task.Ticket.ID,
			"error", err,
		)
		result = messages.Result{
			TaskID: task.ID,
			Agent:  a.cfg.Name,
			Error:  err.Error(),
		}
		a.publish(result)
		return
	}

	result.TaskID = task.ID
	result.Agent = a.cfg.Name
	result.Success = true
	a.stats.Processed.Add(1)

	a.logger.Info("задание выполнено",
		"task_id", task.ID,
		"ticket", task.Ticket.ID,
		"elapsed_ms", elapsed.Milliseconds(),
	)

	a.publish(result)
}

// publishError публикует результат с ошибкой при неизвестном task_id.
func (a *Agent) publishError(taskID string, err error) {
	a.publish(messages.Result{
		TaskID:  taskID,
		Agent:   a.cfg.Name,
		Success: false,
		Error:   err.Error(),
	})
}

// publish отправляет результат в тему ответа.
func (a *Agent) publish(result messages.Result) {
	// Экземпляр проставляется здесь, а не в обработчике: тогда и успешный
	// ответ, и ответ с ошибкой называют того, кто их отправил.
	if result.Instance == "" {
		result.Instance = a.cfg.Instance
	}

	payload, err := json.Marshal(result)
	if err != nil {
		a.logger.Error("результат не сериализован", "error", err)
		return
	}

	if err := a.nc.Publish(messages.SubjectResult, payload); err != nil {
		a.logger.Error("результат не отправлен", "error", err)
		return
	}

	if err := a.nc.Flush(); err != nil {
		a.logger.Error("отправка не подтверждена", "error", err)
	}
}

// Stats возвращает текущие счётчики агента.
func (a *Agent) Stats() map[string]any {
	return a.stats.Snapshot()
}

// SignalContext возвращает контекст, отменяемый по SIGINT или SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
