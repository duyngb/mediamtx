package playback

import "github.com/bluenviron/mediamtx/internal/logger"

var L logger.Writer

var (
	Debug = logger.Debug
	Info  = logger.Info
	Warn  = logger.Warn
	Error = logger.Error
	Any   = logger.Error + 1

	D = logger.Debug
	I = logger.Info
	W = logger.Warn
	E = logger.Error
	A = logger.Error + 1
)

func init() {
	l, _ := logger.New(logger.Debug, []logger.Destination{logger.DestinationStdout}, "/dev/null")
	L = &lw{l}
}

type lw struct{ parent logger.Writer }

func (l *lw) Log(level logger.Level, format string, args ...interface{}) {
	l.parent.Log(level, "[playback] "+format, args...)
}
