// Агент генерации ответов клиенту.
//
// Агент детерминированный: одинаковый вход даёт одинаковый ответ.
// Задание методички про LLM-агента относится к повышенной сложности и в
// варианте 2 не выполняется.
package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/frfn0/LAB13MultiAgent/pkg/agent"
	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// hintByCategory - подсказка, добавляемая к ответу в зависимости от
// категории обращения. Это правило из AGENTS.md: для billing упоминаются
// сроки возврата, для account - восстановление доступа.
var hintByCategory = map[string]string{
	messages.CategoryBilling:   "Сроки возврата денежных средств — от 3 до 10 рабочих дней в зависимости от вашего банка.",
	messages.CategoryAccount:   "Если пароль не подходит, восстановите доступ на странице входа: код придёт на почту.",
	messages.CategoryDelivery:  "Актуальный статус доставки доступен в личном кабинете в разделе «Мои заказы».",
	messages.CategoryTechnical: "Если проблема повторяется, сообщите нам версию приложения и модель устройства.",
}

// handleTask формирует ответ клиенту.
func handleTask(_ context.Context, task messages.Task) (messages.Result, error) {
	if strings.TrimSpace(task.Ticket.Text) == "" {
		return messages.Result{}, errEmptyText
	}

	if !task.Found {
		return messages.Result{
			Answer:     deferredAnswer(task),
			AnswerType: messages.AnswerDeferred,
		}, nil
	}

	return messages.Result{
		Answer:     resolvedAnswer(task),
		AnswerType: messages.AnswerResolved,
	}, nil
}

// resolvedAnswer - ответ на основе найденной статьи базы знаний.
func resolvedAnswer(task messages.Task) string {
	var builder strings.Builder

	builder.WriteString(task.ArticleSolution)

	if hint, ok := hintByCategory[task.Category]; ok {
		builder.WriteString(" ")
		builder.WriteString(hint)
	}

	builder.WriteString(fmt.Sprintf(" (статья «%s»)", task.ArticleTitle))

	return builder.String()
}

// deferredAnswer - ответ, когда подходящей статьи нет.
func deferredAnswer(task messages.Task) string {
	var builder strings.Builder

	if task.Category == messages.CategoryOther {
		builder.WriteString(
			"Опишите вопрос подробнее и оставьте контакты — старший специалист ответит в течение рабочего дня.",
		)
	} else {
		builder.WriteString(
			"Мы не нашли готового решения по вашему обращению и передали его специалисту.",
		)
	}

	if task.Priority >= 4 {
		builder.WriteString(" Обращение отмечено как срочное.")
	}

	builder.WriteString(" Номер обращения: ")
	builder.WriteString(task.Ticket.ID)

	return builder.String()
}

func main() {
	cfg, err := agent.LoadConfig("responder", messages.SubjectAnswer, "responders")
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
