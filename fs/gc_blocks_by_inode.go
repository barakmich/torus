package fs

import (
	"sync"

	"github.com/RoaringBitmap/roaring"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
)

type blocksByINode struct {
	mut     sync.RWMutex
	deadmap *roaring.Bitmap
	vol     *models.Volume
	mds     torus.FSMetadataService
	skip    bool
}

func NewBlocksByINodeGC(mds torus.MetadataService) GC {
	if m, ok := mds.(torus.FSMetadataService); ok {
		return &blocksByINode{mds: m}
	}
	return &nullGC{}
}

func (b *blocksByINode) PrepVolume(vol *models.Volume) error {
	b.mut.Lock()
	defer b.mut.Unlock()
	b.skip = false
	if vol.Type != models.Volume_FILE {
		b.skip = true
		return nil
	}
	b.vol = vol
	deadmap, held, err := b.mds.GetVolumeLiveness(torus.VolumeID(vol.Id))
	if err != nil {
		return err
	}
	for _, x := range held {
		deadmap.AndNot(x)
	}
	b.deadmap = deadmap
	return nil
}

func (b *blocksByINode) IsDead(ref torus.BlockRef) bool {
	b.mut.RLock()
	defer b.mut.RUnlock()
	if b.skip {
		return false
	}
	if ref.Volume() != torus.VolumeID(b.vol.Id) {
		clog.Error("checking dead ref we haven't prepared for")
		return false
	}

	if b.deadmap.Contains(uint32(ref.INode)) {
		return true
	}
	return false
}

func (b *blocksByINode) Clear() {}
