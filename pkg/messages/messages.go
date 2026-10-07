// Package messages содержит типы сообщений, которыми обмениваются
// агенты и оркестратор.
//
// Контракт зафиксирован в agents/AGENTS.md: если изменить поля здесь,
// нужно менять и описание, и код агентов.
package messages

// Темы NATS.
const (
	// SubjectClassify - задание классификатору тикетов.
	SubjectClassify = "ticket.classify"
	// SubjectKnowledge - задание поиску по базе знаний.
	SubjectKnowledge = "ticket.knowledge"
	// SubjectAnswer - задание генератору ответа.
	SubjectAnswer = "ticket.answer"
	// SubjectEscalate - задание эскалирующему агенту.
	SubjectEscalate = "ticket.escalate"
	// SubjectResult - ответ агента с результатом задания.
	SubjectResult = "ticket.result"
)

// Категории обращений.
const (
	CategoryBilling   = "billing"
	CategoryTechnical = "technical"
	CategoryAccount   = "account"
	CategoryDelivery  = "delivery"
	CategoryOther     = "other"
)

// Ticket - исходное обращение клиента.
type Ticket struct {
	ID     string `json:"id"`
	Text   string `json:"text"`
	Author string `json:"author"`
}

// Task - задание, которое агент получает из очереди NATS.
type Task struct {
	// ID задания, генерируется оркестратором.
	ID string `json:"task_id"`
	// Type - какое действие требуется: classify, knowledge, answer, escalate.
	Type string `json:"type"`
	// Ticket - исходное обращение.
	Ticket Ticket `json:"ticket"`
	// Данные предыдущего шага конвейера.
	Category        string   `json:"category,omitempty"`
	Priority        int      `json:"priority,omitempty"`
	Tags            []string `json:"tags,omitempty"`
	ArticleID       int      `json:"article_id,omitempty"`
	ArticleTitle    string   `json:"article_title,omitempty"`
	ArticleSolution string   `json:"solution,omitempty"`
	Confidence      float64  `json:"confidence,omitempty"`
	Found           bool     `json:"found,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	// Attempts - сколько раз оркестратор уже пытался выполнить задание.
	Attempts int `json:"attempts"`
}

// Result - ответ агента, который публикуется в тему ticket.result.
type Result struct {
	TaskID string `json:"task_id"`
	// Agent - имя агента, обработавшего задание.
	Agent string `json:"agent"`
	// Success - удалось ли обработать задание.
	Success bool `json:"success"`
	// Error - описание ошибки при Success = false.
	Error string `json:"error,omitempty"`

	// Результат классификации.
	Category string   `json:"category,omitempty"`
	Priority int      `json:"priority,omitempty"`
	Tags     []string `json:"tags,omitempty"`

	// Результат поиска по базе знаний.
	ArticleID       int     `json:"article_id,omitempty"`
	ArticleTitle    string  `json:"article_title,omitempty"`
	ArticleSolution string  `json:"solution,omitempty"`
	Confidence      float64 `json:"confidence,omitempty"`
	Found           bool    `json:"found,omitempty"`

	// Результат генерации ответа.
	Answer     string `json:"answer,omitempty"`
	AnswerType string `json:"answer_type,omitempty"`

	// Результат эскалации.
	EscalationID string `json:"escalation_id,omitempty"`
	AssignedTo   string `json:"assigned_to,omitempty"`
	SLAHours     int    `json:"sla_hours,omitempty"`
	Urgent       bool   `json:"urgent,omitempty"`
}

// Типы заданий.
const (
	TaskClassify  = "classify"
	TaskKnowledge = "knowledge"
	TaskAnswer    = "answer"
	TaskEscalate  = "escalate"
)

// Типы ответов агента генерации.
const (
	AnswerResolved = "resolved"
	AnswerDeferred = "deferred"
)
