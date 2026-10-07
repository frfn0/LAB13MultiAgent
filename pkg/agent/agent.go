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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// ErrInvalidTask - задание не прошло проверку и не может быть обработано.
var ErrInvalidTask = errors.New("задание некорректно")

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
}

// LoadConfig читает настройки агента из переменных окружения.
func LoadConfig(name, subject, queue string) (Config, error) {
	if name == "" {
		return Config{}, errors.New("имя агента не задано")
	}
	if subject == "" {
		return Config{}, errors.New("тема подписки не задана")
	}

	verbose := false
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		verbose = true
	}

	return Config{
		Name:    name,
		Subject: subject,
		Queue:   queue,
		NatsURL: envOr("NATS_URL", nats.DefaultURL),
		LogFile: os.Getenv("LOG_FILE"),
		Verbose: verbose,
	}, nil
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

	<-ctx.Done()
	a.logger.Info("агент останавливается", "agent", a.cfg.Name)

	// Даём в работе завершиться текущим заданиям.
	if err := nc.Drain(); err != nil {
		a.logger.Warn("дренирование не завершилось", "error", err)
	}

	stats := a.stats.Snapshot()
	a.logger.Info("итоговые счётчики", "stats", stats)
	return nil
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
