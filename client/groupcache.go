// Running 3 instances:
// go run groupcache.go -addr=:8080 -pool=http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082
// go run groupcache.go -addr=:8081 -pool=http://127.0.0.1:8081,http://127.0.0.1:8080,http://127.0.0.1:8082
// go run groupcache.go -addr=:8082 -pool=http://127.0.0.1:8082,http://127.0.0.1:8080,http://127.0.0.1:8081
// Testing:
// curl localhost:8080/color?name=red
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/cbergoon/merkletree"
	"github.com/golang/groupcache"
	"github.com/klauspost/reedsolomon"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var originalMT *merkletree.MerkleTree
var receivedMT *merkletree.MerkleTree
var files map[string]*merkletree.MerkleTree
var localPort string

type FileContent struct {
	c []byte
}

//CalculateHash hashes the values of a TestContent
func (t FileContent) CalculateHash() ([]byte, error) {
	h := sha256.New()
	if _, err := h.Write(t.c); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

//Equals tests for equality of two Contents
func (t FileContent) Equals(other merkletree.Content) (bool, error) {
	return bytes.Equal(t.c, other.(FileContent).c), nil
}

var Store = map[string][]byte{
	"Text1": []byte("Text 1 file"),
	"Text2": []byte("Text 2 file"),
	"Text3": []byte("Text 3 file"),
}

var Group = groupcache.NewGroup("foobar", 64<<20, groupcache.GetterFunc(
	func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
		log.Println("looking up", key)
		log.Println("the local server ipppppp========", localPort)
		bv, err := dest.View()
		if err != nil {
			return errors.New("err....")
		}
		log.Println("dest:", bv, ".", bv.String() == "")

		if bv.String() == "" {
			//v, ok := Store[key]
			v := downloadFileFromS3(key)
			// log.Println("find the file? :", ok)
			// if !ok {
			// 	return errors.New("file not found")
			// }
			dest.SetBytes(v)
		} else {
			log.Println("save dest to Store....:", bv)
			//Store[key] = []byte(bv.String())
			//tempxxxx
			AddFileToS3(key, []byte(bv.String()))
		}

		return nil
	},
))

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func calculateFileHash(t FileContent) (string, error) {
	h := sha256.New()
	if _, err := h.Write(t.c); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
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
	f := FileContent{c: b}

	// Split the file into equally sized shards.
	shards, err := enc.Split(b)
	check(err)
	fmt.Printf("File split into %d data+parity shards with %d bytes/shard.\n", len(shards), len(shards[0]))

	// Encode parity
	err = enc.Encode(shards)
	check(err)

	var list []merkletree.Content

	for _, shard := range shards {
		list = append(list, FileContent{c: shard})
	}
	originalMT, err = merkletree.NewTree(list)
	check(err)

	for _, leaf := range originalMT.Leafs {
		////////////////
		var b []byte
		dest := groupcache.AllocatingByteSliceSink(&b)
		dest.SetBytes(leaf.C.(FileContent).c)
		//dest.SetBytes([]byte("test !!!! " + r.FormValue("name")))
		key := base64.URLEncoding.EncodeToString(leaf.Hash)
		err = Group.Save(nil, key, dest)
		check(err)
	}

	files = make(map[string]*merkletree.MerkleTree)
	key, err := calculateFileHash(f)
	check(err)
	files[key] = originalMT
}

func decoder(fname string) {
	// Create matrix
	enc, err := reedsolomon.New(*dataShards, *parShards)
	check(err)

	// Create shards and load the data.
	shards := make([][]byte, *dataShards+*parShards)
	for i, leaf := range originalMT.Leafs {
		key := base64.URLEncoding.EncodeToString(leaf.Hash)
		fmt.Println("geting", key)
		//shards[i], err = ioutil.ReadFile(infn)
		var b []byte
		Group.Get(nil, key, groupcache.AllocatingByteSliceSink(&b))
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

	/////build receivedMerkleTree
	var list []merkletree.Content

	for _, shard := range shards {
		list = append(list, FileContent{c: shard})
	}
	receivedMT, err = merkletree.NewTree(list)
	check(err)

	if bytes.Equal(receivedMT.MerkleRoot(), originalMT.MerkleRoot()) {
		fmt.Println("The file received is same as the original file!")
	} else {
		fmt.Println("The file received is NOT same as the original file!")
	}
	/////

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

/////add files to s3
// TODO fill these in!
const (
	S3_REGION = "us-west-2"
	S3_BUCKET = "overnest-dev."
)

// AddFileToS3 will upload a single file to S3, it will require a pre-built aws session
// and will set file info like content type and encryption on the uploaded file.
func AddFileToS3(key string, content []byte) error {

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	// Create a single AWS session (we can re use this if we're uploading many files)
	s, err := session.NewSession(&aws.Config{Region: aws.String(S3_REGION)})
	if err != nil {
		log.Fatal(err)
	}

	ifaces, err := net.Interfaces()
	var ip net.IP
	// handle err
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		check(err)
		// handle err
		for _, addr := range addrs {

			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			// process IP address
			log.Println("IP:::")
			log.Println(ip)
		}
	}
	contentBytes := bytes.NewReader(content)
	bucket := S3_BUCKET + localPort
	log.Println("bucket===========+++++++++++")
	log.Println(bucket)
	log.Println(key)
	// Config settings: this is where you choose the bucket, filename, content-type etc.
	// of the file you're uploading.
	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		ACL:                  aws.String("private"),
		Body:                 contentBytes,
		ContentLength:        aws.Int64(contentBytes.Size()),
		ContentType:          aws.String(http.DetectContentType(content)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})
	log.Println("uploading error=================!!!!!!!!!!!!!")
	log.Println(err)
	return err
}

func downloadFileFromS3(key string) []byte {
	s, err := session.NewSession(&aws.Config{Region: aws.String(S3_REGION)})
	if err != nil {
		log.Fatal(err)
	}
	downloader := s3manager.NewDownloader(s)

	buff := &aws.WriteAtBuffer{}
	bucket := S3_BUCKET + localPort

	_, err = downloader.Download(buff,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		log.Fatal(err)
	}

	return buff.Bytes()
}

////////////////

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
	localPort = strings.Split(p[0], ":")[2]
	pool := groupcache.NewHTTPPool(p[0])
	pool.Set(p...)
	http.ListenAndServe(*addr, nil)
}

//go run groupcache.go -addr=:8080 -pool=http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082
//go run groupcache.go -addr=:8081 -pool=http://127.0.0.1:8081,http://127.0.0.1:8080,http://127.0.0.1:8082
//curl localhost:8080/savefile?name=newTest.txt
//curl localhost:8080/getfile?name=newTest.txt
