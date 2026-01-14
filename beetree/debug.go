package beetree

import (
	"fmt"
	"log"
	"log/slog"
	"time"
)

func (t *BeeTree) Inspect() {
	fsize := t.fileSize()
	log.Println("filesize", fsize)

	off := BeeMetadataSize

	log.Println("pages count", t.pageCount())
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

		off += PageSize
		time.Sleep(time.Second)
	}
}

func (t *BeeTree) SetDebug(debug bool) {
	t.debug = debug
}

func (t *BeeTree) Log(format string, a ...any) {
	if t.debug {
		fmt.Printf(format, a...)
	}
}

func (t *BeeTree) VerifyPage() {
	log.Println("page count:", t.pageCount())
	for i := 0; i <= t.pageCount(); i++ {

		switch getPageType(i, t.data) {
		case pageLeaf:
			page := getLeafPage(i, t.data)
			page.PrintDebug()
			entries := page.getEntry()
			entries.PrintMinMax()

		case pageInternal:
			page := getInternalPage(i, t.data)
			page.PrintDebug()
			entries := page.getEntry()
			entries.PrintMinMax()
			entries.Print()
		}

	}

}
