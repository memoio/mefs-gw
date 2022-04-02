package memo

import (
	"io"
	"io/ioutil"

	shapi "github.com/ipfs/go-ipfs-api"
)

func ChunkerSize(size string) shapi.AddOpts {
	return func(rb *shapi.RequestBuilder) error {
		rb.Option("chunker", size)
		return nil
	}
}

type IpFs struct {
	host string
}

func NewIpfsClient(host string) *IpFs {
	return &IpFs{
		host: host,
	}
}

func (i *IpFs) Putobject(r io.Reader) (string, error) {
	sh := shapi.NewShell(i.host)
	cidvereion := shapi.CidVersion(1)
	chunkersize := ChunkerSize("size-253952")
	hash, err := sh.Add(r, cidvereion, chunkersize)
	if err != nil {
		return "", err
	}
	return hash, nil
}

func (i *IpFs) GetObject(cid string) ([]byte, error) {
	sh := shapi.NewShell(i.host)
	r, err := sh.Cat(cid)
	if err != nil {
		return nil, err
	}
	data, _ := ioutil.ReadAll(r)
	return data, nil
}
