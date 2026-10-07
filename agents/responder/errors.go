package main

import "errors"

// errEmptyText - обращение без текста: отвечать клиенту не о чем.
var errEmptyText = errors.New("текст обращения пуст")
