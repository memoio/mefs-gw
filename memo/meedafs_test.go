package memo

import (
	"context"
	"testing"
)

const (
	URL          = "http://183.240.197.189:48082"
	ID           = "9378c0db747d33b62045c9ae4332d98e058c29d53dc9ceac530da4d019574902102236f3725907c2e86b64212b32ba65"
	meeda_repo   = "~/user_meeda"
	meeda_object = "da-txdata9c22ff5f21f0b81b113e63f7db6da94fedef11b2119b4088b89664fb9a3cb658"
)

func TestPutObject(t *testing.T) {
	fs, err := NewMeedaFs(meeda_repo, URL)
	if err != nil {
		t.Error(err)
	}

	id, err := fs.Putobject([]byte("test"))
	if err != nil {
		t.Error(err)
	}

	t.Log(id)
}

// func TestGetObject(t *testing.T) {
// 	fs, err := NewMeedaFs(meeda_repo, URL)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	data, err := fs.GetObject(ID)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	t.Log(string(data))
// }

func TestListObjects(t *testing.T) {
	fs, err := NewMeedaFs(meeda_repo, URL)
	if err != nil {
		t.Error(err)
	}

	list, err := fs.ListObjects(context.TODO(), "", "", "/", 10)
	if err != nil {
		t.Error(err)
	}

	t.Log(list)
}

func TestGetObjectInfo(t *testing.T) {
	fs, err := NewMeedaFs(meeda_repo, URL)
	if err != nil {
		t.Error(err)
	}

	info, err := fs.GetObjectInfo(context.Background(), meeda_object)
	if err != nil {
		t.Error(err)
	}

	t.Log(info)
}
