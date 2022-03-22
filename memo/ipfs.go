package memo

import (
	"io"
	"io/ioutil"

	shell "github.com/ipfs/go-ipfs-api"
)

type IpFs struct {
	host string
}

func NewIpfsClient(ip, port string) *IpFs {
	return &IpFs{
		host: ip + ":" + port,
	}
}

func (i *IpFs) Putobject(r io.Reader) (string, error) {
	sh := shell.NewShell(i.host)

	hash, err := sh.Add(r)
	if err != nil {
		return "", err
	}
	return hash, nil
}

func (i *IpFs) GetObject(cid string) ([]byte, error) {
	sh := shell.NewShell(i.host)
	r, err := sh.Cat(cid)
	if err != nil {
		return nil, err
	}
	data, _ := ioutil.ReadAll(r)
	return data, nil
}
