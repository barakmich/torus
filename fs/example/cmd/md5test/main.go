package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/coreos/torus"
	_ "github.com/coreos/torus/metadata/etcd"
	_ "github.com/coreos/torus/metadata/temp"
	"github.com/coreos/torus/server"
	_ "github.com/coreos/torus/storage/block"
)

var etcdAddress = flag.String("etcd", "127.0.0.1:2378", "Etcd")
var volume = flag.String("vol", "", "Agro volume to file")
var file = flag.String("file", "", "Agro path to file")

func main() {
	flag.Parse()
	cfg := torus.Config{
		MetadataAddress: *etcdAddress,
		ReadCacheSize:   40 * 1024 * 1024,
	}
	srv, _ := server.NewServer(cfg, "etcd", "temp")
	srv.OpenReplication()
	fsSrv, _ := srv.FS()
	f, err := fsSrv.Open(torus.Path{
		Volume: *volume,
		Path:   *file,
	})
	if err != nil {
		panic(err)
	}
	defer f.Close()
	m := md5.New()
	_, err = io.Copy(m, f)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("%x %s:%s\n", m.Sum(nil), *volume, *file)
}
