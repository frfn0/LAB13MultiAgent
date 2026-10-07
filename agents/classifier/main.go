// Классификатор тикетов - агент, работающий в системе техподдержки.
//
// Задание 2 методички: агент подписывается на очередь NATS, принимает
// структурированные данные в JSON, выполняет логику обработки и публикует
// результат в общую тему ответов.
package main

import (
	"context"
	"strings"

	"github.com/frfn0/LAB13MultiAgent/pkg/agent"
	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// keywordRule - правило классификации: совпадение любого ключевого слова
// задаёт категорию, приоритет и тег.
type keywordRule struct {
	Category string
	Priority int
	Tag      string
	Words    []string
}

// rules проверены по порядку: первое совпадение задаёт категорию.
// Порядок важен - например, "не работает оплата" должно попасть в
// technical, а не в billing.
var rules = []keywordRule{
	{
		Category: messages.CategoryTechnical,
		Priority: 5,
		Tag:      "critical",
		Words: []string{
			"не работает", "неработа", "упал", "падает", "ошибка",
			"500", "503", "timeout", "таймаут", "баг", "crash",
			"не грузит", "белая страница",
		},
	},
	{
		Category: messages.CategoryBilling,
		Priority: 4,
		Tag:      "money",
		Words: []string{
			"оплата", "оплатить", "счёт", "счет", "чек", "возврат",
			"деньги", "карта", "подписка", "тариф", "списали",
		},
	},
	{
		Category: messages.CategoryAccount,
		Priority: 3,
		Tag:      "access",
		Words: []string{
			"пароль", "password", "доступ", "аккаунт", "account",
			"вход", "залогиниться", "сбросить пароль", "блокировка",
		},
	},
	{
		Category: messages.CategoryDelivery,
		Priority: 2,
		Tag:      "delivery",
		Words: []string{
			"доставка", "доставить", "курьер", "посылка", "трек",
			"заказ не пришёл", "придёт ли",
		},
	},
}

// fallbackRule - категория для обращений без совпадений.
var fallbackRule = keywordRule{
	Category: messages.CategoryOther,
	Priority: 1,
	Tag:      "unclassified",
}

// classify определяет категорию, приоритет и теги обращения.
func classify(text string) (category string, priority int, tags []string) {
	lowered := strings.ToLower(text)

	for _, rule := range rules {
		for _, word := range rule.Words {
			if strings.Contains(lowered, word) {
				return rule.Category, rule.Priority, []string{rule.Tag}
			}
		}
	}

	return fallbackRule.Category, fallbackRule.Priority, []string{fallbackRule.Tag}
}

// handleTask - логика агента классификатора.
func handleTask(_ context.Context, task messages.Task) (messages.Result, error) {
	if strings.TrimSpace(task.Ticket.Text) == "" {
		return messages.Result{}, errEmptyText
	}

	category, priority, tags := classify(task.Ticket.Text)

	return messages.Result{
		Category: category,
		Priority: priority,
		Tags:     tags,
	}, nil
}

func main() {
	cfg, err := agent.LoadConfig("classifier", messages.SubjectClassify, "classifiers")
	if err != nil {
		panic(err)
	}

	instance, err := agent.New(cfg, handleTask)
	if err != nil {
		panic(err)
	}

	ctx, stop := agent.SignalContext()
	defer stop()

	if err := instance.Run(ctx); err != nil {
		panic(err)
	}
}
