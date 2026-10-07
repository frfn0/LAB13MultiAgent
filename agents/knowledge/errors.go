package main

import "errors"

// Ошибки агента поиска по базе знаний.
var (
	errEmptyText       = errors.New("текст обращения пуст")
	errBaseNotLoaded   = errors.New("база знаний не загружена")
	errEmptyBase       = errors.New("база знаний пуста")
	errConfidenceParse = errors.New("MIN_CONFIDENCE не число")
)
