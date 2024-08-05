package playback

import (
	"errors"
	"fmt"
	"io"

	"github.com/abema/go-mp4"
)

// Modified of [mp4.ReadBoxStructure] to avoid unnecessary seeking.
func ReadBoxStructure(r io.ReadSeeker, handler mp4.ReadHandler, params ...interface{}) error {
	return readBoxStructure(r, 0, true, nil, mp4.Context{}, handler, params)
}

func readBoxStructureFromInternal(
	r io.ReadSeeker,
	bi *mp4.BoxInfo,
	path mp4.BoxPath,
	handler mp4.ReadHandler,
	params []interface{},
) error {
	if _, err := bi.SeekToPayload(r); err != nil {
		return err
	}

	// check comatible-brands
	if len(path) == 0 && bi.Type == mp4.BoxTypeFtyp() {
		var ftyp mp4.Ftyp
		if _, err := mp4.Unmarshal(r, bi.Size-bi.HeaderSize, &ftyp, bi.Context); err != nil {
			return err
		}
		if ftyp.HasCompatibleBrand(mp4.BrandQT()) {
			bi.IsQuickTimeCompatible = true
		}
		if _, err := bi.SeekToPayload(r); err != nil {
			return err
		}
	}

	// parse numbered ilst items after keys box by saving EntryCount field to context
	if bi.Type == mp4.BoxTypeKeys() {
		var keys mp4.Keys
		if _, err := mp4.Unmarshal(r, bi.Size-bi.HeaderSize, &keys, bi.Context); err != nil {
			return err
		}
		bi.QuickTimeKeysMetaEntryCount = int(keys.EntryCount)
		if _, err := bi.SeekToPayload(r); err != nil {
			return err
		}
	}

	ctx := bi.Context
	if bi.Type == mp4.BoxTypeWave() {
		ctx.UnderWave = true
	} else if bi.Type == mp4.BoxTypeIlst() {
		ctx.UnderIlst = true
	} else if bi.UnderIlst && !bi.UnderIlstMeta && mp4.IsIlstMetaBoxType(bi.Type) {
		ctx.UnderIlstMeta = true
		if bi.Type == mp4.StrToBoxType("----") {
			ctx.UnderIlstFreeMeta = true
		}
	} else if bi.Type == mp4.BoxTypeUdta() {
		ctx.UnderUdta = true
	}

	newPath := make(mp4.BoxPath, len(path)+1)
	copy(newPath, path)
	newPath[len(path)] = bi.Type

	h := &mp4.ReadHandle{
		Params:  params,
		BoxInfo: *bi,
		Path:    newPath,
	}

	var childrenOffset uint64

	h.ReadPayload = func() (mp4.IBox, uint64, error) {
		if _, err := bi.SeekToPayload(r); err != nil {
			return nil, 0, err
		}

		box, n, err := mp4.UnmarshalAny(r, bi.Type, bi.Size-bi.HeaderSize, bi.Context)
		if err != nil {
			return nil, 0, err
		}
		childrenOffset = bi.Offset + bi.HeaderSize + n
		return box, n, nil
	}

	h.ReadData = func(w io.Writer) (uint64, error) {
		if _, err := bi.SeekToPayload(r); err != nil {
			return 0, err
		}

		size := bi.Size - bi.HeaderSize
		if _, err := io.CopyN(w, r, int64(size)); err != nil {
			return 0, err
		}
		return size, nil
	}

	h.Expand = func(params ...interface{}) ([]interface{}, error) {
		if childrenOffset == 0 {
			if _, err := bi.SeekToPayload(r); err != nil {
				return nil, err
			}

			_, n, err := mp4.UnmarshalAny(r, bi.Type, bi.Size-bi.HeaderSize, bi.Context)
			if err != nil {
				return nil, err
			}
			childrenOffset = bi.Offset + bi.HeaderSize + n
		} else {
			if _, err := r.Seek(int64(childrenOffset), io.SeekStart); err != nil {
				return nil, err
			}
		}

		childrenSize := bi.Offset + bi.Size - childrenOffset
		return nil, readBoxStructure(r, childrenSize, false, newPath, ctx, handler, params)
	}

	if _, err := handler(h); err != nil {
		return err
	} else if _, err := bi.SeekToEnd(r); err != nil {
		return err
	} else {
		return nil
	}
}

func readBoxStructure(
	r io.ReadSeeker,
	totalSize uint64,
	isRoot bool,
	path mp4.BoxPath,
	ctx mp4.Context,
	handler mp4.ReadHandler,
	params []interface{},
) error {
	for isRoot || totalSize >= mp4.SmallHeaderSize {
		bi, err := mp4.ReadBoxInfo(r)
		if isRoot && err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		if !isRoot && bi.Size > totalSize {
			return fmt.Errorf("too large box size: type=%s, size=%d, actual=%d", bi.Type.String(), bi.Size, totalSize)
		}
		totalSize -= bi.Size

		bi.Context = ctx

		err = readBoxStructureFromInternal(r, bi, path, handler, params)
		if err != nil {
			return err
		}

		if bi.IsQuickTimeCompatible {
			ctx.IsQuickTimeCompatible = true
		}

		// preserve keys entry count on context for subsequent ilst number item box
		if bi.Type == mp4.BoxTypeKeys() {
			ctx.QuickTimeKeysMetaEntryCount = bi.QuickTimeKeysMetaEntryCount
		}
	}

	if totalSize != 0 && !ctx.IsQuickTimeCompatible {
		return errors.New("unexpected EOF")
	}

	return nil
}
