// Агент эскалации.
//
// Фиксирует обращения, которые нельзя закрыть автоматически, и назначает
// ответственного с отделом и сроком реакции.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/frfn0/LAB13MultiAgent/pkg/agent"
	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// departmentByCategory - отдел, который принимает эскалацию по категории.
var departmentByCategory = map[string]string{
	messages.CategoryBilling:   "отдел-расчётов",
	messages.CategoryTechnical: "инженерная-служба",
	messages.CategoryAccount:   "служба-поддержки-аккаунтов",
	messages.CategoryDelivery:  "служба-доставки",
	messages.CategoryOther:     "старший-специалист",
}

// fallbackDepartment - отдел для неизвестной категории.
const fallbackDepartment = "старший-специалист"

// slaByPriority - срок реакции в часах в зависимости от приоритета.
var slaByPriority = map[int]int{
	5: 1,
	4: 4,
	3: 8,
}

// defaultSLAHours - срок для низких приоритетов.
const defaultSLAHours = 24

// urgentPriority - начиная с этого приоритета обращение помечается срочным.
const urgentPriority = 5

// handleTask оформляет эскалацию обращения.
func handleTask(_ context.Context, task messages.Task) (messages.Result, error) {
	if strings.TrimSpace(task.Ticket.Text) == "" {
		return messages.Result{}, errEmptyText
	}

	department, ok := departmentByCategory[task.Category]
	if !ok {
		department = fallbackDepartment
	}

	slaHours, ok := slaByPriority[task.Priority]
	if !ok {
		slaHours = defaultSLAHours
	}

	escalationID := generateID(task.Ticket.ID)

	reason := task.Reason
	if reason == "" {
		reason = "не найдено решение в базе знаний"
	}

	return messages.Result{
		EscalationID: escalationID,
		AssignedTo:   department,
		SLAHours:     slaHours,
		Urgent:       task.Priority >= urgentPriority,
		Answer: fmt.Sprintf(
			"Обращение %s передано в отдел «%s», срок реакции %d ч. Причина: %s.",
			task.Ticket.ID, department, slaHours, reason,
		),
		AnswerType: messages.AnswerDeferred,
	}, nil
}

// generateID формирует идентификатор эскалации на основе номера тикета и
// случайного суффикса. Идентификатор должен быть уникальным, а счётчик в
// агенте для этого заводить не хочется: при перезапуске агента счётчик
// сбросится и номера повторятся.
func generateID(ticketID string) string {
	buffer := make([]byte, 4)
	if _, err := rand.Read(buffer); err != nil {
		// Криптослучайные числа недоступны: возвращаем идентификатор без
		// суффикса. Лучше повторимый, чем вовсе никакой.
		return fmt.Sprintf("ESC-%s", ticketID)
	}
	return fmt.Sprintf("ESC-%s-%s", ticketID, hex.EncodeToString(buffer))
}

func main() {
	cfg, err := agent.LoadConfig("escalation", messages.SubjectEscalate, "escalations")
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
