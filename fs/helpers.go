package fs

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/torus/models"
)

func roaringToBytes(r *roaring.Bitmap) []byte {
	buf := new(bytes.Buffer)
	_, err := r.WriteTo(buf)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func bytesToRoaring(b []byte) *roaring.Bitmap {
	r := bytes.NewReader(b)
	bm := roaring.New()
	_, err := bm.ReadFrom(r)
	if err != nil {
		panic(err)
	}
	return bm
}

func newDirProto(md *models.Metadata) []byte {
	a := models.Directory{
		Metadata: md,
		Files:    make(map[string]uint64),
	}
	b, err := a.Marshal()
	if err != nil {
		panic(err)
	}
	return b
}
