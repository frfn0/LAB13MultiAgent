package main

import (
	"strings"
	"testing"
)

// TestGenerateIDContainsTicketID проверяет, что номер эскалации содержит
// номер обращения: по нему можно найти исходный тикет в логах.
func TestGenerateIDContainsTicketID(t *testing.T) {
	t.Parallel()

	id := generateID("ticket-42")

	if !strings.HasPrefix(id, "ESC-ticket-42") {
		t.Errorf("номер эскалации %q не содержит номер обращения", id)
	}
}

// TestGenerateIDIsUnique проверяет, что номера не повторяются: счётчик
// сбрасывался бы при перезапуске агента, поэтому используется случайный
// суффикс.
func TestGenerateIDIsUnique(t *testing.T) {
	t.Parallel()

	const attempts = 200
	seen := make(map[string]struct{}, attempts)

	for range attempts {
		id := generateID("ticket-1")
		if _, exists := seen[id]; exists {
			t.Fatalf("номер эскалации повторился: %q", id)
		}
		seen[id] = struct{}{}
	}
}

// TestGenerateIDFormat проверяет формат номера: он используется в логах и
// в ответе клиенту, поэтому должен быть предсказуемым.
func TestGenerateIDFormat(t *testing.T) {
	t.Parallel()

	id := generateID("ticket-7")
	parts := strings.Split(id, "-")

	// ESC-ticket-7-xxxxxxxx: 4 части, суффикс из 8 шестнадцатеричных цифр.
	if len(parts) != 4 {
		t.Fatalf("неверный формат номера: %q", id)
	}
	if len(parts[3]) != 8 {
		t.Errorf("суффикс номера должен быть из 8 символов: %q", id)
	}
	for _, symbol := range parts[3] {
		isHex := (symbol >= '0' && symbol <= '9') || (symbol >= 'a' && symbol <= 'f')
		if !isHex {
			t.Errorf("суффикс содержит недопустимый символ %q: %q", symbol, id)
		}
	}
}
