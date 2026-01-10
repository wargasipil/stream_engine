package beetree

import (
	"log"
	"log/slog"
	"time"
)

func (t *BeeTree) Inspect() {
	fsize := t.fileSize()
	off := BeeMetadataSize

	log.Println("pages count", t.pageCount())
	slog.Info("file size", slog.Int64("file_size", int64(fsize)))
	if fsize <= 0 {
		return
	}

	log.Println(t.nextPageId())
	log.Println(t.nextPageId())
	log.Println(t.nextPageId())

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
