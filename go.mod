module github.com/memoio/mefs-gateway

go 1.16

require (
	github.com/ipfs/go-blockservice v0.2.1
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-ipfs-api v0.3.0
	github.com/ipfs/go-ipfs-blockstore v1.1.2
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-offline v0.1.1
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-merkledag v0.5.1
	github.com/ipfs/go-unixfs v0.3.1
	github.com/memoio/go-mefs-v2 v0.0.0-00010101000000-000000000000
	github.com/memoio/minio v0.2.4
	github.com/minio/cli v1.22.0
	github.com/minio/madmin-go v1.3.6
	github.com/minio/minio-go/v7 v7.0.23
	github.com/mitchellh/go-homedir v1.1.0
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
	golang.org/x/crypto v0.0.0-20220307211146-efcb8507fb70
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

replace (
	// github.com/memoio/console => ../console
	github.com/memoio/go-mefs-v2 => ../go-mefs-v2
	// github.com/memoio/minio => /home/lighthouse/g-project/minio
	memoc => ../memo-go-contracts-v2
)
