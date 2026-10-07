package main

import (
	"context"
	"strings"
	"testing"

	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// TestResolvedAnswerUsesArticleSolution проверяет, что ответ строится на
// основе найденной статьи и ссылается на неё.
func TestResolvedAnswerUsesArticleSolution(t *testing.T) {
	t.Parallel()

	task := messages.Task{
		Ticket:          messages.Ticket{ID: "ticket-1", Text: "не проходит оплата"},
		Category:        messages.CategoryBilling,
		Priority:        4,
		Found:           true,
		ArticleTitle:    "Не проходит оплата банковской картой",
		ArticleSolution: "Проверьте срок действия карты.",
	}

	answer := resolvedAnswer(task)

	if !strings.Contains(answer, "Проверьте срок действия карты.") {
		t.Errorf("в ответе нет решения из статьи: %q", answer)
	}
	if !strings.Contains(answer, "Не проходит оплата банковской картой") {
		t.Errorf("в ответе нет ссылки на статью: %q", answer)
	}
}

// TestResolvedAnswerAddsCategoryHint проверяет, что к ответу добавляется
// подсказка по категории обращения.
func TestResolvedAnswerAddsCategoryHint(t *testing.T) {
	t.Parallel()

	task := messages.Task{
		Category:        messages.CategoryAccount,
		ArticleTitle:    "Не удаётся войти в аккаунт",
		ArticleSolution: "Сбросьте пароль.",
	}

	answer := resolvedAnswer(task)

	hint, ok := hintByCategory[messages.CategoryAccount]
	if !ok {
		t.Skip("для категории account нет подсказки")
	}
	if !strings.Contains(answer, hint) {
		t.Errorf("подсказка категории не добавлена: %q", answer)
	}
}

// TestDeferredAnswerForOtherCategory проверяет текст для нераспознанной
// категории: клиенту обещают ответ специалиста.
func TestDeferredAnswerForOtherCategory(t *testing.T) {
	t.Parallel()

	task := messages.Task{
		Ticket:   messages.Ticket{ID: "ticket-9"},
		Category: messages.CategoryOther,
		Priority: 1,
	}

	answer := deferredAnswer(task)

	if !strings.Contains(answer, "старший специалист") {
		t.Errorf("нет обещания ответа специалиста: %q", answer)
	}
	if !strings.Contains(answer, "ticket-9") {
		t.Errorf("в ответе нет номера обращения: %q", answer)
	}
	if strings.Contains(answer, "срочное") {
		t.Errorf("несрочное обращение не должно помечаться срочным: %q", answer)
	}
}

// TestDeferredAnswerMarksUrgent проверяет, что срочные обращения помечаются.
func TestDeferredAnswerMarksUrgent(t *testing.T) {
	t.Parallel()

	task := messages.Task{
		Ticket:   messages.Ticket{ID: "ticket-10"},
		Category: messages.CategoryBilling,
		Priority: 5,
	}

	if !strings.Contains(deferredAnswer(task), "срочное") {
		t.Error("обращение с приоритетом 5 должно помечаться срочным")
	}
}

// TestHandleTaskChoosesAnswerType проверяет выбор типа ответа: при найденной
// статье ответ считается решённым, без статьи - отложенным.
func TestHandleTaskChoosesAnswerType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		task messages.Task
		want string
	}{
		{
			name: "статья найдена",
			task: messages.Task{
				Ticket:          messages.Ticket{ID: "t1", Text: "оплата"},
				Category:        messages.CategoryBilling,
				Found:           true,
				ArticleTitle:    "Оплата картой",
				ArticleSolution: "Проверьте карту.",
			},
			want: messages.AnswerResolved,
		},
		{
			name: "статья не найдена",
			task: messages.Task{
				Ticket:   messages.Ticket{ID: "t2", Text: "оплата"},
				Category: messages.CategoryBilling,
				Priority: 3,
			},
			want: messages.AnswerDeferred,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result, err := handleTask(context.Background(), testCase.task)
			if err != nil {
				t.Fatalf("задание не выполнено: %v", err)
			}
			if result.AnswerType != testCase.want {
				t.Errorf("тип ответа: получено %q, ожидалось %q", result.AnswerType, testCase.want)
			}
			if strings.TrimSpace(result.Answer) == "" {
				t.Error("ответ пустой")
			}
		})
	}
}

// TestHandleTaskRejectsEmptyText проверяет, что обращение без текста
// отклоняется.
func TestHandleTaskRejectsEmptyText(t *testing.T) {
	t.Parallel()

	task := messages.Task{Ticket: messages.Ticket{ID: "t3", Text: "   "}}

	if _, err := handleTask(context.Background(), task); err == nil {
		t.Error("обращение без текста принято")
	}
}
