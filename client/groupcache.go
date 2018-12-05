// Running 3 instances:
// go run groupcache.go -addr=:8080 -pool=http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082
// go run groupcache.go -addr=:8081 -pool=http://127.0.0.1:8081,http://127.0.0.1:8080,http://127.0.0.1:8082
// go run groupcache.go -addr=:8082 -pool=http://127.0.0.1:8082,http://127.0.0.1:8080,http://127.0.0.1:8081
// Testing:
// curl localhost:8080/color?name=red
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/golang/groupcache"
	"github.com/klauspost/reedsolomon"
)

var Store = map[string][]byte{
	"Text1": []byte("Text 1 file"),
	"Text2": []byte("Text 2 file"),
	"Text3": []byte("Text 3 file"),
}

var Group = groupcache.NewGroup("foobar", 64<<20, groupcache.GetterFunc(
	func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
		log.Println("looking up", key)
		bv, err := dest.View()
		if err != nil {
			return errors.New("err....")
		}
		log.Println("dest:", bv, ".", bv.String() == "")

		if bv.String() == "" {
			v, ok := Store[key]
			log.Println("find the file? :", ok)
			if !ok {
				return errors.New("file not found")
			}
			dest.SetBytes(v)
		} else {
			log.Println("save dest to Store....:", bv)
			Store[key] = []byte(bv.String())
		}

		return nil
	},
))

func check(e error) {
	if e != nil {
		panic(e)
	}
}

////erasure coding
var dataShards = flag.Int("data", 4, "Number of shards to split the data into, must be below 257.")
var parShards = flag.Int("par", 2, "Number of parity shards")

//var outDir = flag.String("out", "", "Alternative output directory")
//var outFile = flag.String("out", "", "Alternative output path/file")
var inputpath = "/tmp/localfiles/input/"
var outputpath = "/tmp/localfiles/output/"

func init() {
	flag.Usage = func() { //
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  simple-encoder [-flags] filename.ext\n\n")
		fmt.Fprintf(os.Stderr, "Valid flags:\n")
		flag.PrintDefaults()
	}
}

func encoder(fname string) {
	// Create encoding matrix.
	enc, err := reedsolomon.New(*dataShards, *parShards)
	check(err)

	fmt.Println("Opening", fname)
	b, err := ioutil.ReadFile(inputpath + fname)
	check(err)

	// Split the file into equally sized shards.
	shards, err := enc.Split(b)
	check(err)
	fmt.Printf("File split into %d data+parity shards with %d bytes/shard.\n", len(shards), len(shards[0]))

	// Encode parity
	err = enc.Encode(shards)
	check(err)

	for i, shard := range shards {
		////////////////
		var b []byte
		dest := groupcache.AllocatingByteSliceSink(&b)
		dest.SetBytes(shard)
		//dest.SetBytes([]byte("test !!!! " + r.FormValue("name")))
		err = Group.Save(nil, fmt.Sprintf("%s.%d", fname, i), dest)
		check(err)
	}
}

func decoder(fname string) {
	// Create matrix
	enc, err := reedsolomon.New(*dataShards, *parShards)
	check(err)

	// Create shards and load the data.
	shards := make([][]byte, *dataShards+*parShards)
	for i := range shards {
		infn := fmt.Sprintf("%s.%d", fname, i)
		fmt.Println("geting", infn)
		//shards[i], err = ioutil.ReadFile(infn)
		var b []byte
		Group.Get(nil, infn, groupcache.AllocatingByteSliceSink(&b))
		//check(err)
		shards[i] = b
	}

	// Verify the shards
	ok, err := enc.Verify(shards)
	if ok {
		fmt.Println("No reconstruction needed")
	} else {
		fmt.Println("Verification failed. Reconstructing data")
		err = enc.Reconstruct(shards)
		if err != nil {
			fmt.Println("Reconstruct failed -", err)
			os.Exit(1)
		}
		ok, err = enc.Verify(shards)
		if !ok {
			fmt.Println("Verification failed after reconstruction, data likely corrupted.")
			os.Exit(1)
		}
		check(err)
	}

	// Join the shards and write them
	outfn := fmt.Sprintf("%s%s", outputpath, fname)

	fmt.Println("Writing data to", outfn)
	f, err := os.Create(outfn)
	check(err)

	// We don't know the exact filesize.
	err = enc.Join(f, shards, len(shards[0])**dataShards)
	check(err)
}

////erasure coding

func main() {
	addr := flag.String("addr", ":8080", "server address")
	peers := flag.String("pool", "http://localhost:8080", "server pool list")
	flag.Parse()
	http.HandleFunc("/savefile", func(w http.ResponseWriter, r *http.Request) {
		log.Println("save!!!!")
		key := r.FormValue("name")
		encoder(key)
	})
	http.HandleFunc("/getfile", func(w http.ResponseWriter, r *http.Request) {
		log.Println("get!!!!")
		key := r.FormValue("name")
		decoder(key)
	})
	p := strings.Split(*peers, ",")
	pool := groupcache.NewHTTPPool(p[0])
	pool.Set(p...)
	http.ListenAndServe(*addr, nil)
}

//go run groupcache.go -addr=:8081 -pool=http://127.0.0.1:8081,http://127.0.0.1:8080,http://127.0.0.1:8082
//curl localhost:8080/savefile?name=newTest.txt
//curl localhost:8080/getfile?name=newTest.txt
