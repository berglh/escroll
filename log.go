package main

// This is the escroll logging functions

import (
	"os"
	"time"

	"github.com/wsxiaoys/terminal/color"
)

const (
	// ISO8601Milli is ISO8601 format in millis
	ISO8601Milli = "2006-01-02T15:04:05.000"
	Error        = "ERROR"
	Warn         = "WARN"
	Info         = "INFO"
	OK           = "OK"
)

func logTimestamp() string {
	t := time.Now()
	return t.Format(ISO8601Milli)
}

func Log(level, message string) {
	var c string
	switch level {
	case Error:
		c = "@r"
	case Warn:
		c = "@y"
	case OK:
		c = "@g"
	case Info:
		c = "@{|}"
	}
	color.Fprintln(os.Stderr, color.Sprintf("%s%s %s: %s", c, logTimestamp(), level, message))
	if level == Error {
		os.Exit(1)
	}
}
