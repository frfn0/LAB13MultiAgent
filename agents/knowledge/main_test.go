package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// loadTestKnowledgeBase читает настоящий файл базы знаний: тесты должны
// проверять ту же выдачу, что и работающий агент, а не выдуманные статьи.
func loadTestKnowledgeBase(t *testing.T) *KnowledgeBase {
	t.Helper()

	base, err := loadKnowledgeBase(filepath.Join("..", "..", "knowledge_base", "articles.json"))
	if err != nil {
		t.Fatalf("база знаний не загружена: %v", err)
	}

	return base
}

// TestSignificantWordsDropsShortAndServiceWords проверяет фильтрацию:
// в поиск не должны попадать слова короче трёх букв и служебные слова,
// иначе запрос совпадал бы со статьёй по слову «где» или «не».
func TestSignificantWordsDropsShortAndServiceWords(t *testing.T) {
	t.Parallel()

	words := significantWords("Я хочу узнать, где мой заказ")

	for _, word := range words {
		if len([]rune(word)) < 3 {
			t.Errorf("короткое слово попало в список значимых: %q (все слова: %v)", word, words)
		}
		if stopWords[word] {
			t.Errorf("служебное слово попало в список значимых: %q", word)
		}
	}

	if len(words) == 0 {
		t.Error("из осмысленного текста не осталось ни одного значимого слова")
	}
}

// TestSignificantWordsIgnoresPunctuation проверяет, что знаки препинания
// не мешают разбору: слова выделяются по буквам и цифрам.
func TestSignificantWordsIgnoresPunctuation(t *testing.T) {
	t.Parallel()

	withPunctuation := significantWords("Оплата, карта — не проходит!")
	withoutPunctuation := significantWords("Оплата карта не проходит")

	if len(withPunctuation) != len(withoutPunctuation) {
		t.Errorf("знаки препинания изменили разбор: %v против %v",
			withPunctuation, withoutPunctuation)
	}
}

// TestCommonPrefixLength проверяет подсчёт общего начала в символах
// Unicode: в байтах кириллица считалась бы вдвое длиннее.
func TestCommonPrefixLength(t *testing.T) {
	t.Parallel()

	cases := []struct {
		first   string
		second  string
		length  int
		comment string
	}{
		{"оплата", "оплата", 6, "одинаковые слова"},
		{"оплата", "оплатить", 5, "разные словоформы одной основы"},
		{"заказ", "заказа", 5, "существительное и его форма"},
		{"оплата", "доставка", 0, "разные понятия"},
		{"заказ", "", 0, "пустое слово"},
		{"", "заказ", 0, "пустое слово слева"},
	}

	for _, testCase := range cases {
		got := commonPrefixLength(testCase.first, testCase.second)

		if got != testCase.length {
			t.Errorf("commonPrefixLength(%q, %q) = %d, ожидалось %d (%s)",
				testCase.first, testCase.second, got, testCase.length, testCase.comment)
		}
	}
}

// TestSameWordComparesByStem проверяет сопоставление по основе: именно
// из-за него в задании 4 выдача была нулевой до исправления.
func TestSameWordComparesByStem(t *testing.T) {
	t.Parallel()

	same := [][2]string{
		{"оплатить", "оплата"},
		{"заказа", "заказ"},
		{"курьер", "курьер"},
		{"оплата", "оплату"},
		{"подписка", "подписки"},
	}

	for _, pair := range same {
		if !sameWord(pair[0], pair[1]) {
			t.Errorf("слова %q и %q должны считаться одинаковыми", pair[0], pair[1])
		}
	}

	different := [][2]string{
		{"оплата", "доставка"},
		{"заказ", "пароль"},
		{"карта", "курс"},
		{"заказ", "выдача"},
	}

	for _, pair := range different {
		if sameWord(pair[0], pair[1]) {
			t.Errorf("слова %q и %q не должны считаться одинаковыми", pair[0], pair[1])
		}
	}
}

// TestSameWordRequiresLongEnoughMatch проверяет защиту от совпадений по
// короткому началу: общее начало короче minCommonPrefix не считается
// совпадением.
func TestSameWordRequiresLongEnoughMatch(t *testing.T) {
	t.Parallel()

	if sameWord("доставка", "договор") {
		t.Error("слова с общим коротким началом не должны считаться одинаковыми")
	}
}

// TestSearchFindsArticleByStem проверяет поиск по реальной базе знаний:
// запрос со словоформой, отличной от статьи, всё равно должен её найти.
func TestSearchFindsArticle(t *testing.T) {
	t.Parallel()

	base := loadTestKnowledgeBase(t)

	cases := []struct {
		name     string
		text     string
		category string
		wantWord string
	}{
		{
			name:     "оплатить находит статью про оплату",
			text:     "Не могу оплатить заказ, карта не проходит",
			category: "billing",
			wantWord: "оплата",
		},
		{
			name:     "курьер находит статью про доставку",
			text:     "Где мой курьер и когда будет доставка",
			category: "delivery",
			wantWord: "курьер",
		},
		{
			name:     "пароль находит статью про доступ",
			text:     "Не могу сбросить пароль от аккаунта",
			category: "account",
			wantWord: "пароль",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			article, confidence, found := base.search(testCase.text, testCase.category)

			if !found {
				t.Fatalf("статья не найдена для %q (уверенность %v)", testCase.text, confidence)
			}
			if confidence <= 0 || confidence > 1 {
				t.Errorf("уверенность вне диапазона 0..1: %v", confidence)
			}
			if article.ID == 0 {
				t.Fatal("возвращена пустая статья")
			}
			if article.Category != testCase.category {
				t.Errorf("найдена статья из другой категории: №%d %q, категория %q",
					article.ID, article.Title, article.Category)
			}
			if confidence < minConfidence {
				t.Errorf("уверенность %v ниже порога %v, хотя search вернул находку",
					confidence, minConfidence)
			}
			if !strings.Contains(strings.ToLower(article.Title+article.Solution), testCase.wantWord) {
				t.Errorf("найдена статья без ожидаемого слова %q: №%d %q",
					testCase.wantWord, article.ID, article.Title)
			}
		})
	}
}

// TestHandleTaskRejectsUnrelatedText проверяет, что нерелевантный запрос
// не приводит к ложному ответу: лучше эскалация, чем неверный совет
// клиенту.
//
// Проверяется handleTask, а не голый поиск: поиск без учёта порога
// возвращает статью с оценкой ниже minConfidence, и агент отсекает её сам.
func TestHandleTaskRejectsUnrelatedText(t *testing.T) {
	base := loadTestKnowledgeBase(t)

	previous := store
	store = base
	defer func() { store = previous }()

	task := messages.Task{
		ID:       "task-1",
		Ticket:   messages.Ticket{ID: "ticket-1", Text: "квантовая физика элементарных частиц"},
		Category: messages.CategoryOther,
	}

	result, err := handleTask(context.Background(), task)
	if err != nil {
		t.Fatalf("задание не выполнено: %v", err)
	}
	if result.Found {
		t.Errorf("найдена статья для несвязанного запроса, уверенность %v", result.Confidence)
	}
	if result.Confidence >= minConfidence {
		t.Errorf("уверенность %v не должна быть выше порога %v",
			result.Confidence, minConfidence)
	}
}

// TestHandleTaskRequiresLoadedBase проверяет, что агент без базы знаний
// отвечает ошибкой, а не молча возвращает пустой результат.
func TestHandleTaskRequiresLoadedBase(t *testing.T) {
	previous := store
	store = nil
	defer func() { store = previous }()

	task := messages.Task{
		ID:     "task-2",
		Ticket: messages.Ticket{ID: "ticket-2", Text: "не проходит оплата"},
	}

	if _, err := handleTask(context.Background(), task); err == nil {
		t.Error("агент ответил успехом без загруженной базы знаний")
	}
}

// TestHandleTaskRejectsEmptyText проверяет проверку входных данных.
func TestHandleTaskRejectsEmptyText(t *testing.T) {
	previous := store
	store = loadTestKnowledgeBase(t)
	defer func() { store = previous }()

	task := messages.Task{ID: "task-3", Ticket: messages.Ticket{ID: "ticket-3", Text: "  "}}

	if _, err := handleTask(context.Background(), task); err == nil {
		t.Error("обращение без текста принято")
	}
}

// TestCategoryBoost проверяет, что совпадение категории обращения со
// статьёй повышает оценку.
func TestCategoryBoost(t *testing.T) {
	t.Parallel()

	base := loadTestKnowledgeBase(t)

	_, sameCategory, _ := base.search("Не могу оплатить заказ", "billing")
	_, otherCategory, _ := base.search("Не могу оплатить заказ", "delivery")

	if sameCategory <= otherCategory {
		t.Errorf("своя категория должна давать большую оценку: %v против %v",
			sameCategory, otherCategory)
	}
}

// TestRoundScore проверяет округление уверенности до двух знаков.
func TestRoundScore(t *testing.T) {
	t.Parallel()

	cases := map[float64]float64{
		0.0:      0.0,
		0.123456: 0.12,
		0.876543: 0.88,
		1.0:      1.0,
		0.125:    0.13,
		0.004:    0.0,
	}

	for value, want := range cases {
		if got := roundScore(value); got != want {
			t.Errorf("roundScore(%v): получено %v, ожидалось %v", value, got, want)
		}
	}
}
