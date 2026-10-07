package main

import (
	"context"
	"errors"
	"testing"

	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// TestClassify проверяет правила классификации: категория, приоритет и тег.
func TestClassify(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		text     string
		category string
		priority int
		tag      string
	}{
		{
			name:     "ошибка сайта важнее оплаты",
			text:     "При оплате выдает ошибку 500",
			category: messages.CategoryTechnical,
			priority: 5,
			tag:      "critical",
		},
		{
			name:     "оплата",
			text:     "Не могу оплатить заказ, карта не проходит",
			category: messages.CategoryBilling,
			priority: 4,
			tag:      "money",
		},
		{
			name:     "доступ в аккаунт",
			text:     "Не могу сбросить пароль от аккаунта",
			category: messages.CategoryAccount,
			priority: 3,
			tag:      "access",
		},
		{
			name:     "ошибка важнее всего",
			text:     "Сайт лежит, белая страница",
			category: messages.CategoryTechnical,
			priority: 5,
			tag:      "critical",
		},
		{
			name:     "доставка",
			text:     "Когда придёт курьер",
			category: messages.CategoryDelivery,
			priority: 2,
			tag:      "delivery",
		},
		{
			name:     "непонятное обращение",
			text:     "Здравствуйте, у меня вопрос",
			category: messages.CategoryOther,
			priority: 1,
			tag:      "unclassified",
		},
		{
			name:     "регистр не важен",
			text:     "НЕ РАБОТАЕТ САЙТ",
			category: messages.CategoryTechnical,
			priority: 5,
			tag:      "critical",
		},
		{
			name:     "латиница",
			text:     "payment timeout",
			category: messages.CategoryTechnical,
			priority: 5,
			tag:      "critical",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			category, priority, tags := classify(testCase.text)

			if category != testCase.category {
				t.Errorf("категория: получено %q, ожидалось %q", category, testCase.category)
			}
			if priority != testCase.priority {
				t.Errorf("приоритет: получено %d, ожидалось %d", priority, testCase.priority)
			}
			if len(tags) != 1 || tags[0] != testCase.tag {
				t.Errorf("теги: получено %v, ожидалось [%s]", tags, testCase.tag)
			}
		})
	}
}

// TestHandleTaskRejectsEmptyText проверяет, что обращение без текста
// отклоняется: классифицировать нечего.
func TestHandleTaskRejectsEmptyText(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"пустой текст":        "",
		"только пробелы":      "   ",
		"перевод строки":      "\n",
		"пробелы и табуляции": " \t ",
	}

	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			task := messages.Task{Ticket: messages.Ticket{ID: "ticket-1", Text: text}}
			if _, err := handleTask(context.Background(), task); !errors.Is(err, errEmptyText) {
				t.Errorf("получена ошибка %v, ожидалась errEmptyText", err)
			}
		})
	}
}

// TestHandleTaskReturnsClassification проверяет успешный путь: в результате
// есть категория, приоритет и тег.
func TestHandleTaskReturnsClassification(t *testing.T) {
	t.Parallel()

	task := messages.Task{
		ID:     "task-1",
		Ticket: messages.Ticket{ID: "ticket-1", Text: "Не проходит оплата картой"},
	}

	result, err := handleTask(context.Background(), task)
	if err != nil {
		t.Fatalf("задание не выполнено: %v", err)
	}
	if result.Category != messages.CategoryBilling {
		t.Errorf("категория: получено %q, ожидалось billing", result.Category)
	}
	if result.Priority == 0 {
		t.Error("приоритет не задан")
	}
	if len(result.Tags) == 0 {
		t.Error("теги не заданы")
	}
	// Идентификаторы проставляет обвязка агента, а не обработчик.
	if result.TaskID != "" {
		t.Errorf("обработчик не должен проставлять task_id: %q", result.TaskID)
	}
}

// TestRulesOrderMatters проверяет, что порядок правил сохранён: первое
// совпадение задаёт категорию, иначе обращение с ошибкой оплаты ушло бы в
// billing вместо technical.
func TestRulesOrderMatters(t *testing.T) {
	t.Parallel()

	if len(rules) == 0 {
		t.Fatal("список правил пуст")
	}
	if rules[0].Category != messages.CategoryTechnical {
		t.Errorf("первым должно идти правило technical: %q", rules[0].Category)
	}
	if len(rules) != 4 {
		t.Errorf("правил должно быть четыре по числу категорий: %d", len(rules))
	}
	for index, rule := range rules {
		if len(rule.Words) == 0 {
			t.Errorf("у правила %d нет ключевых слов", index)
		}
		if rule.Priority <= 0 {
			t.Errorf("у правила %d некорректный приоритет %d", index, rule.Priority)
		}
	}
}
