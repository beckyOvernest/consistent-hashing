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
	"io/ioutil"
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

func main() {
	addr := flag.String("addr", ":8080", "server address")
	peers := flag.String("pool", "http://localhost:8080", "server pool list")
	flag.Parse()
	http.HandleFunc("/savefile", func(w http.ResponseWriter, r *http.Request) {
		log.Println("save!!!!")
		key := r.FormValue("name")
		var b []byte
		dest := groupcache.AllocatingByteSliceSink(&b)
		//read file content
		path := "/tmp/localfiles/"
		dat, err := ioutil.ReadFile(path + r.FormValue("name"))
		check(err)
		dest.SetBytes(dat)
		//dest.SetBytes([]byte("test !!!! " + r.FormValue("name")))
		err = Group.Save(nil, key, dest)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Write(b)
		w.Write([]byte{'\n'})
	})
	http.HandleFunc("/getfile", func(w http.ResponseWriter, r *http.Request) {
		log.Println("get!!!!")
		key := r.FormValue("name")
		var b []byte
		err := Group.Get(nil, key, groupcache.AllocatingByteSliceSink(&b))
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

//go run groupcache.go -addr=:8081 -pool=http://127.0.0.1:8081,http://127.0.0.1:8080,http://127.0.0.1:8082
//curl localhost:8080/savefile?name=newTest.txt
//curl localhost:8080/getfile?name=newTest.txt
