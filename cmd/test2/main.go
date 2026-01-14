package main

import (
	"log"
	"path"

	"github.com/wargasipil/stream_engine/beetree"
)

// func main() {
// 	key := []byte("day_team_account/2026-01-05/64/cash/debit")

// 	datas := [][]byte{[]byte{0x0}, []byte("daily/2026-01-05/team/50/all_stock/credit"), []byte("daily/2026-01-05/team/58/all_stock/balance")}

// 	i := sort.Search(len(datas), func(i int) bool {
// 		check := bytes.Compare(datas[i], key)

// 		log.Println(string(datas[i]), string(key), check)

// 		return check > 0
// 	})

// 	log.Println("found", i, "index", i-1)
// }

func main() {
	// os.RemoveAll("/tmp/worker_stat/counter_key")
	index, err := beetree.NewBeeTree(path.Join("/tmp/worker_stat", "counter_key"))
	if err != nil {
		panic(err)
	}
	defer index.Close()

	index.SetDebug(true)

	data, _ := index.GetKeyString("day_team_account_toteam/2026-01-08/74/selling_receivable/74/credit")

	log.Println(data)

	// index.VerifyPage()

	// for c := 0; c < 400; c++ {
	// 	key := fmt.Sprintf("%d_key", c)
	// 	index.InsertKeyString(key, uint64(c))
	// }

	// log.Print("---------------------------debug -----------------------------\n\n\n")

	// key := "168_key"
	// offset, ok := index.Get([]byte(key))
	// log.Println(offset, ok)
	// log.Println("---------------------------debug -----------------------------")
	// index.DebugTree()

}
