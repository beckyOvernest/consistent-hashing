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
	"log"
	"net/http"
	"strings"

	"github.com/golang/groupcache"
)

var Store = map[string][]byte{
	"Text1": []byte("Text 1 file"),
	"Text2": []byte("Text 2 file"),
	"Text3": []byte("Text 3 file"),
}

var Group = groupcache.NewGroup("foobar", 64<<20, groupcache.GetterFunc(
	func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
		log.Println("looking up", key)
		s := strings.Split(key, ":")
		op, hashKey := s[0], s[1]
		if op == "get" {
			v, ok := Store[hashKey]
			if !ok {
				return errors.New("file not found")
			}
			dest.SetBytes(v)
		} else if op == "save" {
			bv, err := dest.View()
			if err != nil {
				return errors.New("err....")
			}
			log.Println("dest:", bv)
			Store[hashKey] = []byte(bv.String())
		}

		return nil
	},
))

func main() {
	addr := flag.String("addr", ":8080", "server address")
	peers := flag.String("pool", "http://localhost:8080", "server pool list")
	flag.Parse()
	http.HandleFunc("/savefile", func(w http.ResponseWriter, r *http.Request) {
		log.Println("save!!!!")
		fileOperation := "save:" + r.FormValue("name")
		var b []byte
		dest := groupcache.AllocatingByteSliceSink(&b)
		dest.SetBytes([]byte("test !!!! " + r.FormValue("name")))
		err := Group.Save(nil, fileOperation, dest)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Write(b)
		w.Write([]byte{'\n'})
	})
	http.HandleFunc("/getfile", func(w http.ResponseWriter, r *http.Request) {
		log.Println("get!!!!")
		fileOperation := "get:" + r.FormValue("name")
		var b []byte
		err := Group.Get(nil, fileOperation, groupcache.AllocatingByteSliceSink(&b))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Write(b)
		w.Write([]byte{'\n'})
	})
	p := strings.Split(*peers, ",")
	pool := groupcache.NewHTTPPool(p[0])
	pool.Set(p...)
	http.ListenAndServe(*addr, nil)
}
