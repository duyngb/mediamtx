package playback

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (p *Server) onHead(ctx *gin.Context) {
	pathName := ctx.Query("path")

	if !p.doAuth(ctx, pathName) {
		return
	}

	start, err := time.Parse(time.RFC3339, ctx.Query("start"))
	if err != nil {
		p.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid start: %w", err))
		return
	}

	duration, err := parseDuration(ctx.Query("duration"))
	if err != nil {
		p.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid duration: %w", err))
		return
	}

	ww := &writerWrapper{ctx: ctx, offset: 0, pass1: true}

	var m muxer

	format := ctx.Query("format")
	switch format {
	case "", "fmp4":
		m = &muxerFMP4{w: ww}

	case "mp4":
		m = &muxerMP4{w: ww}

	default:
		p.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid format: %s", format))
		return
	}

	pathConf, err := p.safeFindPathConf(pathName)
	if err != nil {
		p.writeError(ctx, http.StatusBadRequest, err)
		return
	}

	segments, err := findSegmentsInTimespan(pathConf, pathName, start, duration)
	if err != nil {
		if errors.Is(err, errNoSegmentsFound) {
			p.writeError(ctx, http.StatusNotFound, err)
		} else {
			p.writeError(ctx, http.StatusBadRequest, err)
		}
		return
	}

	// Always act as if pass1 == true
	err = seekAndMux(pathConf.RecordFormat, pathName, segments, start, duration, m)

	if p.handleError(ctx, false, err) {
		return
	}

	ctx.Header("Accept-Ranges", "bytes")
	ctx.Header("Content-Type", "video/mp4")
	ctx.Header("Content-Length", strconv.Itoa(ww.length))

}
