package main

import "errors"

// errEmptyText - обращение без текста. Такой тикет классифицировать
// нельзя: неизвестно даже, к какой категории его отнести.
var errEmptyText = errors.New("текст обращения пуст")
