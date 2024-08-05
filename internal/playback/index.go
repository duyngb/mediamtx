package playback

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/abema/go-mp4"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/gin-gonic/gin"
)

var ErrIndexIsOld = errors.New("index is old")

var Index = &index{
	entries: make(pathIndex),
	running: make(map[string]struct{}),
}

type indexEntry struct {
	Time   time.Time // Entry's absolute time
	Offset int64     // Data offset on segment file
}

type pathIndex map[string][]indexEntry

type index struct {
	sync.RWMutex
	entries pathIndex           // map<path, []entry>
	running map[string]struct{} // running jobs
}

func indexCmp(a, b indexEntry) int { return int(a.Time.Sub(b.Time)) }

func indexFileName(filePath string) string {
	ext := path.Ext(filePath)
	return filePath[:len(filePath)-len(ext)] + ".idx"
}

func readIndex(seg *Segment) (index []indexEntry, err error) {
	segStat, err := os.Stat(seg.Fpath)
	if err != nil {
		return
	}

	indexPath := indexFileName(seg.Fpath)
	indexStat, err := os.Stat(indexPath)
	if err != nil {
		return
	}

	if indexStat.ModTime().Before(segStat.ModTime()) {
		err = ErrIndexIsOld
		return
	}

	return readIndexFile(indexPath)
}

func readIndexFile(indexPath string) ([]indexEntry, error) {
	L.Log(Info, "[index] loading index from path: %s", path.Base(indexPath))
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	idx := make([]indexEntry, 0, 256)

	err = nil
	// [time.Time] needs 15-16 bytes
	var b = make([]byte, 16)
	var n int
	var entry indexEntry

	// Load 16 bytes reserved header
	n, err = f.Read(b[:])
	if err != nil || n != len(b) {
		return nil, ErrIndexIsOld
	}

	if b[0] != 'S' && b[1] != 'I' && b[2] != 'D' && b[3] != 'X' {
		return nil, ErrIndexIsOld
	}

	for {
		n, err = f.Read(b[:])
		if err != nil || n != len(b) {
			break
		}

		if b[0] == 1 {
			err = entry.Time.UnmarshalBinary(b[:15])
		} else {
			err = entry.Time.UnmarshalBinary(b)
		}

		if err != nil {
			break
		}

		err = binary.Read(f, binary.BigEndian, &entry.Offset)
		if err != nil {
			break
		}

		idx = append(idx, entry)
	}

	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	return idx, nil
}

func writeIndex(segPath string, idx []indexEntry) error {
	idxFile := indexFileName(segPath)
	tmpFile := idxFile + ".tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}

	f.Write([]byte{
		'S', 'I', 'D', 'X', 1, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
	})

	for _, entry := range idx {
		b, err := entry.Time.MarshalBinary()
		if err != nil {
			break
		}

		if len(b) == 15 {
			b = append(b, 0)
		}

		_, err = f.Write(b)
		if err != nil {
			break
		}

		err = binary.Write(f, binary.BigEndian, entry.Offset)
		if err != nil {
			break
		}
	}

	f.Close()

	// index file is broken
	if err != nil {
		os.Remove(tmpFile)
		return err
	}

	err = os.Rename(tmpFile, idxFile)
	if err != nil {
		os.Remove(tmpFile)
		return err
	}

	return nil
}

func scanSegment(seg *Segment) (index []indexEntry, err error) {
	f, err := os.Open(seg.Fpath)
	if err != nil {
		return
	}
	defer f.Close()

	init, err := segmentFMP4ReadInit(f)
	if err != nil {
		return
	}

	var moofOffset int64 = 0
	var timeScale time.Duration = 90000
	var tfhd *mp4.Tfhd
	var tfdt *mp4.Tfdt
	var start = seg.Start.Truncate(0)

	index = make([]indexEntry, 1, 128)
	index[0] = indexEntry{seg.Start.Truncate(0), 0}

	err = ReadBoxStructure(f, func(h *mp4.ReadHandle) (interface{}, error) {
		switch h.BoxInfo.Type.String() {
		case "moof":
			moofOffset = int64(h.BoxInfo.Offset)
			return h.Expand()

		case "traf":
			return h.Expand()

		case "tfhd":
			box, _, err := h.ReadPayload()
			if err != nil {
				return nil, err
			}
			tfhd = box.(*mp4.Tfhd)

			track := findInitTrack(init.Tracks, int(tfhd.TrackID))
			if track == nil {
				return nil, fmt.Errorf("invalid track ID: %v", tfhd.TrackID)
			}
			timeScale = time.Duration(track.TimeScale)

		case "tfdt":
			box, _, err := h.ReadPayload()
			if err != nil {
				return nil, err
			}
			tfdt = box.(*mp4.Tfdt)

			offset64 := time.Duration(tfdt.BaseMediaDecodeTimeV1)
			dt := (offset64/timeScale)*time.Second + (offset64%timeScale)*time.Second/timeScale

			index = append(index, indexEntry{start.Add(dt), moofOffset})
		}

		return nil, nil
	})

	return
}

func (i *index) IndexAll(paths map[string]*conf.Path) {
	pc := maps.Clone(paths)

	for pathName, pathConf := range pc {
		i.IndexPath(pathConf, pathName)
	}
}

func (i *index) IndexPath(pathConf *conf.Path, pathName string) {
	if pathName == "all_others" || pathName == "all" {
		return
	}

	segments, _ := FindSegments(pathConf, pathName)
	if len(segments) == 0 {
		return
	}

	switch pathConf.RecordFormat {
	case conf.RecordFormatFMP4:
	default:
		return
	}

	_, running := i.running[pathName]
	i.Lock()
	if running {
		i.Unlock()
		return
	}
	i.running[pathName] = struct{}{}

	if _, ok := i.entries[pathName]; !ok {
		i.entries[pathName] = make([]indexEntry, 0, 128)
	}

	i.Unlock()

	defer func() {
		i.Lock()
		delete(i.running, pathName)
		i.Unlock()
	}()

	L.Log(Info, "[index] index begin path=%s", pathName)

	t0 := time.Now()
	for _, seg := range segments {
		lst, err := readIndex(seg)
		if err == nil {
			goto appendIndex
		}

		lst, err = scanSegment(seg)
		if err != nil {
			L.Log(Warn, "[index] failed to scan segment: %v", err)
			continue
		}

		L.Log(Info, "[index] segment scanned segment=%s len=%d",
			path.Base(seg.Fpath), len(lst))
		err = ErrIndexIsOld

	appendIndex:
		i.Lock()
		curList := i.entries[pathName]
		curList = append(curList, lst...)
		slices.SortStableFunc(curList, indexCmp)
		i.entries[pathName] = curList
		i.Unlock()

		if err == ErrIndexIsOld {
			err = writeIndex(seg.Fpath, lst)
		}

		if err != nil {
			L.Log(Warn, "[index] index file write failed: %v", err)
		}
	}

	L.Log(Info, "[index] index done  path=%s duration=%s",
		pathName, time.Since(t0).Round(10*time.Millisecond))
}

func (i *index) Update(pathName string, segmentTime time.Time, offset int64) {
	i.Lock()
	defer i.Unlock()
	e, ok := i.entries[pathName]
	if !ok {
		e = make([]indexEntry, 0, 128)
	}

	e = append(e, indexEntry{
		Time:   segmentTime.Truncate(0),
		Offset: offset,
	})

	i.entries[pathName] = e
}

func (i *index) FindBestOffset(pathName string, start time.Time) int64 {
	i.RLock()
	entries, ok := i.entries[pathName]
	i.RUnlock()

	if !ok {
		return 0
	}

	start = start.Truncate(0)
	for n := 0; n < len(entries); n++ {
		if entries[n].Time.After(start) {
			if n > 0 && entries[n].Offset != 0 {
				return entries[n-1].Offset
			}
			break
		}
	}

	return 0
}

func (i *index) WriteIndex(pathName, segPath string, a, b time.Time) {
	i.RLock()
	entries, ok := i.entries[pathName]
	i.RUnlock()

	if !ok {
		return
	}

	// find the index list and pass to index writer
	var n0, n1 int

	for n0 = 0; n0 < len(entries); n0++ {
		if entries[n0].Time.Equal(a) {
			break
		}

		if entries[n0].Time.After(a) {
			if n0 == 0 || entries[n0].Offset == 0 {
				break
			}

			n0 -= 1
			break
		}
	}

	for n1 = n0; n1 < len(entries); n1++ {
		if !entries[n1].Time.Before(b) {
			break
		}
	}

	if n0 >= len(entries) || n1 <= n0 {
		L.Log(Warn, "[index] impossible condition: (n0=%d) >= n1=(%d)", n0, n1)
		return
	}

	err := writeIndex(segPath, entries[n0:n1])
	if err != nil {
		L.Log(Warn, "[index] write failed: %v", err)
	}
}

func (i *index) PruneIndex(pathName string, start time.Time) {
	// Remove index entries until next restart point
	i.RLock()
	idx, ok := i.entries[pathName]
	i.RUnlock()

	if !ok || len(idx) == 0 || idx[len(idx)-1].Time.Before(start) {
		return
	}

	var n0, n1 int
	for n0 = 0; n0 < len(idx); n0++ {
		if !idx[n0].Time.Before(start) {
			break
		}
	}

	for n1 = n0 + 1; n1 < len(idx); n1++ {
		if idx[n1].Offset == 0 {
			break
		}
	}

	// Rebuild list
	idx = slices.Concat(idx[:n0], idx[n1:])

	i.Lock()
	i.entries[pathName] = idx
	i.Unlock()
}

func (i *index) OnDumpIndex(ctx *gin.Context) {
	pathName := ctx.Query("path")

	i.RLock()
	entries, ok := i.entries[pathName]
	i.RUnlock()

	ctx.Header("content-Type", "text/plain")
	if ok {
		ctx.Writer.WriteHeader(200)
	} else {
		ctx.Writer.WriteHeader(404)
	}

	fmt.Fprintf(ctx.Writer, "# %s\n", pathName)
	enc := json.NewEncoder(ctx.Writer)

	for _, entry := range entries {
		enc.Encode(&entry)
	}
}

func (p *Server) onReIndex(ctx *gin.Context) {
	pathName := ctx.Query("path")

	Index.RLock()
	idx := Index.entries[pathName]
	_, running := Index.running[pathName]
	Index.RUnlock()

	if running {
		return
	}

	Index.Lock()
	Index.entries[pathName] = make([]indexEntry, 0, min(4096, (len(idx)>>12)<<12))
	Index.Unlock()

	pathConf, err := p.safeFindPathConf(pathName)
	if err != nil {
		return
	}

	go Index.IndexPath(pathConf, pathName)

	ctx.Writer.WriteHeader(http.StatusAccepted)
}
