package fs

import (
	"sync"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
	"github.com/RoaringBitmap/roaring"
)

// TODO(barakmich): This should really be based on truly dead ones
// Since chains can go backwards, under heavy write contention we could,
// theoretically, lose an INode.
//
// There's a simple fix for this -- print a higher INode at file.Sync()
// time, if we appear to be going backwards. That way chains are
// strictly increasing and we're fine.

type deadINodes struct {
	mut  sync.RWMutex
	live *roaring.Bitmap
	vol  torus.VolumeID
	max  torus.INodeID
	mds  torus.FSMetadataService
	skip bool
}

func NewDeadINodeGC(mds torus.MetadataService) GC {
	if m, ok := mds.(torus.FSMetadataService); ok {
		return &deadINodes{mds: m}
	}
	return &nullGC{}
}

func (d *deadINodes) PrepVolume(vol *models.Volume) error {
	d.mut.Lock()
	defer d.mut.Unlock()
	d.skip = false
	if vol.Type != models.Volume_FILE {
		d.skip = true
		return nil
	}
	d.vol = torus.VolumeID(vol.Id)
	chains, err := d.mds.GetINodeChains(d.vol)
	if err != nil {
		return err
	}
	max := uint64(0)
	bm := roaring.NewBitmap()
	for _, c := range chains {
		for _, v := range c.Chains {
			if v > max {
				max = v
			}
			bm.Add(uint32(v))
		}
	}
	d.max = torus.INodeID(max)
	d.live = bm
	return nil
}

func (d *deadINodes) IsDead(ref torus.BlockRef) bool {
	if d.skip {
		return false
	}
	if ref.BlockType() != torus.TypeINode {
		return false
	}
	if ref.INode >= d.max {
		return false
	}
	if d.live.Contains(uint32(ref.INode)) {
		return false
	}
	return true
}

func (d *deadINodes) Clear() {}
