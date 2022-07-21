package logger

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/term"
)

type Level int8

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	CriticalLevel
	PanicLevel
	NoLevel
	Disabled
	TraceLevel Level = -1
)

var instance zerolog.Logger

type Stringer interface {
	String() string
}

type Field struct {
	Key   string
	Value string
}

func F(k, v string) Field {
	return Field{k, v}
}
func FArr(k string, a []string) Field {
	return Field{k, strings.Join(a, ",")}
}

func init() {
	if term.IsTerminal(int(os.Stdout.Fd())) {
		output := zerolog.ConsoleWriter{Out: os.Stdout, NoColor: false}
		instance = log.Output(output)
		return
	}
	instance = log.Logger
}

func doLog(ev *zerolog.Event, msg string, fields []Field) {
	for _, f := range fields {
		ev = ev.Str(f.Key, f.Value)
	}
	ev.Msg(msg)
}

func Debug(msg string, fields ...Field) {
	doLog(instance.Debug(), msg, fields)
}

func Info(msg string, fields ...Field) {
	doLog(instance.Info(), msg, fields)
}

func Warn(msg string, fields ...Field) {
	doLog(instance.Warn(), msg, fields)
}

func Error(err error, fields ...Field) error {
	if err == nil {
		return nil
	}
	doLog(instance.Error().Err(err), "", fields)
	return err
}

func ErrorWithMsg(err error, msg string, fields ...Field) error {
	doLog(instance.Error().Err(err), msg, fields)
	return err
}
