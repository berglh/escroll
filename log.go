package main

// This is the escroll logging functions

import (
	"os"
	"time"

	"github.com/wsxiaoys/terminal/color"
)

// Constants for logging
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
	var t string
	switch level {
	case "Error":
		c = "@r"
		t = "string"
	case "NlnError":
		c = "@r"
		t = "newline"
		level = Error
	case "Warn":
		c = "@y"
		t = "string"
	case "NlnWarn":
		c = "@y"
		t = "newline"
		level = Warn
	case "OK":
		c = "@g"
		t = "string"
	case "NlnOK":
		c = "@g"
		t = "newline"
		level = OK
	case "Info":
		c = "@{|}"
		t = "string"
	case "NlnInfo":
		c = "@{|}"
		t = "newline"
		level = Info
	default:
		os.Exit(1)
	}
	if t == "string" {
		color.Fprintln(os.Stderr, color.Sprintf("%s%s %s: %s", c, logTimestamp(), level, message))
	}
	if t == "newline" {
		color.Fprintln(os.Stderr, color.Sprintf("\n%s%s %s: %s", c, logTimestamp(), level, message))
	}
	if level == "Error" {
		os.Exit(1)
	}
}
