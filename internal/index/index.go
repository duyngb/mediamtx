package index

import (
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
)

type Index interface {
	IndexPath(*conf.Path, string)
	Update(string, time.Time, int64)
	WriteIndex(string, string, time.Time, time.Time)
	PruneIndex(string, time.Time)
	FindBestOffset(string, time.Time) int64
}

var DefaultIndex Index = new(noIndex)

type noIndex struct{}

func (*noIndex) IndexPath(*conf.Path, string) {}

func (*noIndex) Update(string, time.Time, int64) {}

func (*noIndex) WriteIndex(string, string, time.Time, time.Time) {}

func (*noIndex) PruneIndex(string, time.Time) {}

func (*noIndex) FindBestOffset(string, time.Time) int64 { return 0 }
