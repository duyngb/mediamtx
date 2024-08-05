package playback

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bluenviron/mediacommon/pkg/formats/fmp4"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/gin-gonic/gin"
)

type writerWrapper struct {
	ctx     *gin.Context
	length  int  // data length, accumulated during first pass
	skipped int  // bytes skipped during second pass
	sent    int  // byte sent
	offset  int  // offset as requested
	offset2 int  // end offset
	pass1   bool // scan pass
	written bool
}

var ErrFatal = errors.New("broken content size")

func (w *writerWrapper) writeHeaders() {
	w.written = true
	w.ctx.Header("Accept-Ranges", "bytes")
	w.ctx.Header("Content-Type", "video/mp4")
}

func (w *writerWrapper) Write(p []byte) (int, error) {
	n := len(p)

	// first pass
	if w.pass1 {
		w.length += n
		return n, nil
	}

	// second pass, write stage
	if w.skipped >= w.offset {
		if !w.written {
			w.writeHeaders()
		}

		bytesToWrite := len(p)
		if w.sent+bytesToWrite+1 > w.offset2 {
			bytesToWrite = w.offset2 + 1 - w.sent
			p = p[:bytesToWrite]
		}

		n, err := w.ctx.Writer.Write(p[:])
		if err != nil {
			return 0, err
		}

		w.sent += n
		if w.sent >= w.offset2 {
			err = io.EOF
		}

		return n, err
	}

	// second pass, skip stage
	// skip whole buffer
	if w.skipped+n <= w.offset {
		w.skipped += n
		return n, nil
	}

	// skip part of buffer
	// This is number of bytes to skip
	bytesToSkip := w.offset - w.skipped
	w.skipped += bytesToSkip
	if w.skipped > w.offset {
		return 0, ErrFatal
	}

	if !w.written {
		w.writeHeaders()
	}

	return w.ctx.Writer.Write(p[bytesToSkip:])
}

func parseDuration(raw string) (time.Duration, error) {
	// seconds
	if secs, err := strconv.ParseFloat(raw, 64); err == nil {
		return time.Duration(secs * float64(time.Second)), nil
	}

	// deprecated, golang format
	return time.ParseDuration(raw)
}

func seekAndMux(
	recordFormat conf.RecordFormat,
	pathName string,
	segments []*Segment,
	start time.Time,
	duration time.Duration,
	m muxer,
) error {
	if recordFormat == conf.RecordFormatFMP4 {
		var firstInit *fmp4.Init
		var segmentEnd time.Time

		f, err := os.Open(segments[0].Fpath)
		if err != nil {
			return err
		}
		defer f.Close()

		firstInit, err = segmentFMP4ReadInit(f)
		if err != nil {
			return err
		}
		m.writeInit(firstInit)

		segmentStartOffset := start.Sub(segments[0].Start)

		Index.RLock()
		fOffset := Index.FindBestOffset(pathName, start)
		Index.RUnlock()
		if fOffset != 0 {
			f.Seek(fOffset, io.SeekStart)
		}

		segmentMaxElapsed, err := segmentFMP4SeekAndMuxParts(f, segmentStartOffset, duration, firstInit, m)
		if err != nil {
			return err
		}

		segmentEnd = start.Add(segmentMaxElapsed)

		for _, seg := range segments[1:] {
			f, err = os.Open(seg.Fpath)
			if err != nil {
				return err
			}
			defer f.Close()

			var init *fmp4.Init
			init, err = segmentFMP4ReadInit(f)
			if err != nil {
				return err
			}

			if !segmentFMP4CanBeConcatenated(firstInit, segmentEnd, init, seg.Start) {
				break
			}

			segmentStartOffset := seg.Start.Sub(start)

			var segmentMaxElapsed time.Duration
			segmentMaxElapsed, err = segmentFMP4MuxParts(f, segmentStartOffset, duration, firstInit, m)
			if err != nil {
				return err
			}

			segmentEnd = start.Add(segmentMaxElapsed)
		}

		err = m.flush()
		if err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("MPEG-TS format is not supported yet")
}

func (p *Server) onGet(ctx *gin.Context) {
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

	ww := &writerWrapper{
		ctx:     ctx,
		offset:  0,
		offset2: math.MaxInt,
		pass1:   true,
	}

	range_hdr := ctx.GetHeader("Range") // Range: bytes=int-[int]
	n1 := strings.IndexRune(range_hdr, '=')
	n2 := strings.IndexRune(range_hdr, '-')
	if n1 >= 0 && n2 > n1 {
		ww.offset, err = strconv.Atoi(range_hdr[n1+1 : n2])
		if err != nil {
			ww.offset = 0
			ww.pass1 = false
		} else if len(range_hdr[n2+1:]) > 0 {
			ww.offset2, err = strconv.Atoi(range_hdr[n2+1:])
			if err != nil {
				ww.offset2 = math.MaxInt
				ww.pass1 = false
			}
		}
	} else {
		ww.pass1 = false
	}

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
		} else if os.IsNotExist(err) {
			p.writeError(ctx, http.StatusNotFound, errNoSegmentsFound)
		} else {
			p.writeError(ctx, http.StatusBadRequest, err)
		}
		return
	}

	if ww.pass1 {
		// pass 1: Find the metadata
		err = seekAndMux(pathConf.RecordFormat, pathName, segments, start, duration, m)
		if p.handleError(ctx, false, err) {
			return
		}

		if ww.offset2 == math.MaxInt {
			ww.offset2 = ww.length - 1
		}

		ctx.Header("Accept-Ranges", "bytes")
		ctx.Header("Content-Type", "video/mp4")
		ctx.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", ww.offset, ww.offset2, ww.length))
		ctx.Status(http.StatusPartialContent)
		ww.pass1 = false
		ww.written = true
	}

	// pass 2: actual write
	err = seekAndMux(pathConf.RecordFormat, pathName, segments, start, duration, m)
	if p.handleError(ctx, ww.written, err) {
		return
	}

	if _, ok := Index.entries[pathName]; !ok {
		go Index.IndexPath(pathConf, pathName)
	}
}

func (p *Server) handleError(ctx *gin.Context, written bool, err error) (shouldStop bool) {
	if err == io.EOF {
		return
	}

	if err != nil {
		// user aborted the download
		var neterr *net.OpError
		if errors.As(err, &neterr) {
			return true
		}

		// nothing has been written yet; send back JSON
		if !written {
			if errors.Is(err, errNoSegmentsFound) {
				p.writeError(ctx, http.StatusNotFound, err)
			} else {
				p.writeError(ctx, http.StatusBadRequest, err)
			}
			return true
		}

		// something has already been written: abort and write logs only
		if !errors.Is(err, errNoSegmentsFound) {
			p.Log(logger.Error, err.Error())
		}
		return true
	}

	return false
}
