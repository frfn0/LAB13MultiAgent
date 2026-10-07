package main

import "errors"

// errEmptyText - обращение без текста: эскалировать нечего.
var errEmptyText = errors.New("текст обращения пуст")
