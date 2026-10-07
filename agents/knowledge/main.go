// Агент поиска по базе знаний.
//
// Принимает задание с уже классифицированным обращением, подбирает статью
// базы знаний по совпадению слов и публикует результат.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/frfn0/LAB13MultiAgent/pkg/agent"
	"github.com/frfn0/LAB13MultiAgent/pkg/messages"
)

// Article - статья базы знаний.
type Article struct {
	ID       int      `json:"id"`
	Title    string   `json:"title"`
	Category string   `json:"category"`
	Keywords []string `json:"keywords"`
	Solution string   `json:"solution"`
}

// KnowledgeBase - загруженная база знаний.
type KnowledgeBase struct {
	articles []Article
}

// stopWords - служебные слова, которые не должны влиять на релевантность.
// Без такого фильтра почти каждое обращение совпадало бы со словом «не».
var stopWords = map[string]bool{
	"не": true, "но": true, "и": true, "или": true, "в": true, "на": true,
	"с": true, "к": true, "по": true, "из": true, "у": true, "за": true,
	"до": true, "от": true, "для": true, "при": true, "без": true, "что": true,
	"как": true, "мне": true, "меня": true, "мой": true, "моя": true,
	"это": true, "все": true, "весь": true, "быть": true, "есть": true,
	"the": true, "a": true, "an": true, "is": true, "to": true,
}

// significantWords возвращает значимые слова запроса в нижнем регистре.
func significantWords(text string) []string {
	fields := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})

	words := make([]string, 0, len(fields))
	for _, word := range fields {
		if len(word) < 3 || stopWords[word] {
			continue
		}
		words = append(words, word)
	}
	return words
}

// minCommonPrefix - минимальная длина общего начала двух слов, при которой
// они считаются совпадением. Нужна из-за русских окончаний: запрос
// «оплатить» должен находить статью со словом «оплата».
const minCommonPrefix = 4

// commonPrefixLength возвращает длину общего начала двух слов в символах
// Unicode, а не в байтах: иначе кириллица считалась бы вдвое длиннее.
func commonPrefixLength(first, second string) int {
	firstRunes := []rune(first)
	secondRunes := []rune(second)

	limit := len(firstRunes)
	if len(secondRunes) < limit {
		limit = len(secondRunes)
	}

	length := 0
	for length < limit && firstRunes[length] == secondRunes[length] {
		length++
	}

	return length
}

// words extracts words of the text in lower case.
func haystackWords(text string) []string {
	fields := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	return fields
}

// sameWord определяет, относятся ли два слова к одному понятию.
//
// Сравнение идёт по основе слова, а не по точному совпадению: иначе
// запрос «оплатить» не нашёл бы статью, где написано «оплата».
func sameWord(query, candidate string) bool {
	if query == candidate {
		return true
	}

	// Сравнивается общее начало, а не основы, отрезанные на одинаковое
	// число символов: у слов разной длины основы не совпадают. «оплата»
	// и «оплатить» при отрезании двух символов дают «опла» и «оплати»,
	// и запрос «оплатить» переставал находить статью про оплату.
	return commonPrefixLength(query, candidate) >= minCommonPrefix
}

// confidence считает долю значимых слов запроса, встречающихся в статье.
func (k *KnowledgeBase) confidence(article Article, words []string) float64 {
	if len(words) == 0 {
		return 0
	}

	available := haystackWords(
		article.Title + " " + article.Category + " " + article.Solution +
			" " + strings.Join(article.Keywords, " "),
	)

	matched := 0
	for _, word := range words {
		for _, candidate := range available {
			if sameWord(word, candidate) {
				matched++
				break
			}
		}
	}

	return float64(matched) / float64(len(words))
}

// search подбирает лучшую статью.
//
// Приоритет отдаётся статьям той же категории, что и у обращения:
// так запрос про оплату не получит статью про доставку, даже если в ней
// встретится слово «заказ». При равенстве оценок выбирается статья с
// меньшим номером - иначе выбор был бы невоспроизводимым.
func (k *KnowledgeBase) search(text, category string) (Article, float64, bool) {
	words := significantWords(text)
	if len(words) == 0 {
		return Article{}, 0, false
	}

	best := Article{}
	bestScore := 0.0
	found := false

	for _, article := range k.articles {
		score := k.confidence(article, words)

		if article.Category == category {
			// Совпадение категории повышает оценку: +0.2, но не выше 1.0.
			score += 0.2
			if score > 1.0 {
				score = 1.0
			}
		}

		if score > bestScore || (score == bestScore && found && article.ID < best.ID) {
			best = article
			bestScore = score
			found = true
		}
	}

	return best, bestScore, found
}

// loadKnowledgeBase читает базу знаний из JSON-файла.
func loadKnowledgeBase(path string) (*KnowledgeBase, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать базу знаний %s: %w", path, err)
	}

	var articles []Article
	if err := json.Unmarshal(data, &articles); err != nil {
		return nil, fmt.Errorf("база знаний повреждена: %w", err)
	}

	if len(articles) == 0 {
		return nil, errEmptyBase
	}

	sort.Slice(articles, func(i, j int) bool { return articles[i].ID < articles[j].ID })

	return &KnowledgeBase{articles: articles}, nil
}

// store - глобальная база знаний, загружается один раз при старте агента.
var store *KnowledgeBase

// minConfidence - порог релевантности, задаётся переменной окружения.
var minConfidence = 0.25

// handleTask ищет статью базы знаний по обращению.
func handleTask(_ context.Context, task messages.Task) (messages.Result, error) {
	if strings.TrimSpace(task.Ticket.Text) == "" {
		return messages.Result{}, errEmptyText
	}

	if store == nil {
		return messages.Result{}, errBaseNotLoaded
	}

	article, score, found := store.search(task.Ticket.Text, task.Category)

	if !found || score < minConfidence {
		return messages.Result{
			Found:      false,
			Confidence: roundScore(score),
		}, nil
	}

	return messages.Result{
		Found:           true,
		ArticleID:       article.ID,
		ArticleTitle:    article.Title,
		ArticleSolution: article.Solution,
		Confidence:      roundScore(score),
	}, nil
}

// roundScore округляет оценку до двух знаков, чтобы логи читались ровно.
func roundScore(score float64) float64 {
	return float64(int(score*100+0.5)) / 100
}

func main() {
	cfg, err := agent.LoadConfig("knowledge", messages.SubjectKnowledge, "knowledge-agents")
	if err != nil {
		panic(err)
	}

	kbPath := os.Getenv("KNOWLEDGE_BASE_PATH")
	if kbPath == "" {
		kbPath = "knowledge_base/articles.json"
	}

	store, err = loadKnowledgeBase(kbPath)
	if err != nil {
		panic(err)
	}

	if raw := os.Getenv("MIN_CONFIDENCE"); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			panic(errConfidenceParse)
		}
		minConfidence = parsed
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
