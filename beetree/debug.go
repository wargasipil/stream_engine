package beetree

import (
	"log/slog"
	"time"
)

func (t *BeeTree) Inspect() {
	fsize := t.fileSize()
	off := BeeMetadataSize

	slog.Info("file size", slog.Int64("file_size", int64(fsize)))
	if fsize <= 0 {
		return
	}

	for off < int(fsize) {
		page := bpage{
			offset: off,
			data:   t.data,
		}

		slog.Info("page",
			slog.Int("type", int(page.pageType())),
			slog.Int("id", page.pageID()),
		)
		slog.Info("key", slog.Int("count", int(page.keyCount())))

		off += PageSize + BeeMetadataSize
		time.Sleep(time.Second)
	}
}
