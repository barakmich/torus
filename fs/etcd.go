package fs

import (
	"bytes"
	"errors"
	"os"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"

	"github.com/RoaringBitmap/roaring"
	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/torus"
	"github.com/coreos/torus/metadata/etcd"
	"github.com/coreos/torus/models"
	"golang.org/x/net/context"
)

const (
	chainPageSize = 1000
)

type fsEtcd struct {
	*etcd.Etcd
	name string
	vid  torus.VolumeID
}

func (c *fsEtcd) getContext() context.Context {
	return context.TODO()
}

func keyNotExists(key string) etcdv3.Cmp {
	return etcdv3.Compare(etcdv3.Version(key), "=", 0)
}

func keyExists(key string) etcdv3.Cmp {
	return etcdv3.Compare(etcdv3.Version(key), ">", 0)
}

func keyIsVersion(key string, version int64) etcdv3.Cmp {
	return etcdv3.Compare(etcdv3.Version(key), "=", version)
}

func (c *fsEtcd) CreateFSVol(volume *models.Volume) error {
	key := Path{Volume: volume.Name, Path: "/"}
	new, err := c.AtomicModifyKey([]byte(etcd.MkKey("meta", "volumeminter")), etcd.BytesAddOne)
	volume.Id = new.(uint64)
	if err != nil {
		return err
	}
	t := uint64(time.Now().UnixNano())
	vbytes, err := volume.Marshal()
	if err != nil {
		return err
	}
	do := c.Etcd.Client.Txn(c.getContext()).If(
		keyNotExists(etcd.MkKey("volumes", volume.Name)),
	).Then(
		etcdv3.OpPut(etcd.MkKey("volumes", volume.Name), string(etcd.Uint64ToBytes(volume.Id))),
		etcdv3.OpPut(etcd.MkKey("volumeid", etcd.Uint64ToHex(volume.Id)), string(vbytes)),
		etcdv3.OpPut(etcd.MkKey("volumemeta", "inode", etcd.Uint64ToHex(volume.Id)), string(etcd.Uint64ToBytes(1))),
		etcdv3.OpPut(etcd.MkKey("volumemeta", "deadmap", etcd.Uint64ToHex(volume.Id)), string(roaringToBytes(roaring.NewBitmap()))),
		etcdv3.OpPut(etcd.MkKey("dirs", key.Key()), string(newDirProto(&models.Metadata{
			Ctime: t,
			Mtime: t,
			Mode:  uint32(os.ModeDir | 0755),
		}))),
	)
	resp, err := do.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return torus.ErrExists
	}
	return nil
}

func (c *fsEtcd) Mkdir(path Path, md *models.Metadata) error {
	// promOps.WithLabelValues("mkdir").Inc()
	parent, ok := path.Parent()
	if !ok {
		return errors.New("etcd: not a directory")
	}
	do := c.Etcd.Client.Txn(c.getContext()).If(
		keyExists(etcd.MkKey("dirs", parent.Key())),
	).Then(
		etcdv3.OpPut(etcd.MkKey("dirs", path.Key()), string(newDirProto(md))),
	)
	resp, err := do.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return os.ErrNotExist
	}
	return nil
}

func (c *fsEtcd) ChangeDirMetadata(p Path, md *models.Metadata) error {
	// promOps.WithLabelValues("change-dir-metadata").Inc()
	_, err := c.AtomicModifyKey(
		[]byte(etcd.MkKey("dirs", p.Key())),
		func(in []byte) ([]byte, interface{}, error) {
			dir := &models.Directory{}
			dir.Unmarshal(in)
			dir.Metadata = md
			b, err := dir.Marshal()
			return b, nil, err
		})
	return err
}

func (c *fsEtcd) Rmdir(path Path) error {
	// promOps.WithLabelValues("rmdir").Inc()
	if !path.IsDir() {
		clog.Error("rmdir: not a directory", path)
		return errors.New("etcd: not a directory")
	}
	if path.Path == "/" {
		clog.Error("rmdir: cannot delete root")
		return errors.New("etcd: cannot delete root directory")
	}
	dir, subdirs, version, err := c.getdir(path)
	if err != nil {
		clog.Error("rmdir: getdir err", err)
		return err
	}
	if len(dir.Files) != 0 || len(subdirs) != 0 {
		clog.Error("rmdir: dir not empty", dir, subdirs)
		return errors.New("etcd: directory not empty")
	}
	do := c.Etcd.Client.Txn(c.getContext()).If(
		keyIsVersion(etcd.MkKey("dirs", path.Key()), version),
	).Then(
		etcdv3.OpDelete(etcd.MkKey("dirs", path.Key())),
	)
	resp, err := do.Commit()
	if !resp.Succeeded {
		clog.Error("rmdir: txn failed")
		return os.ErrInvalid
	}
	return nil
}

func (c *fsEtcd) Getdir(p Path) (*models.Directory, []Path, error) {
	dir, paths, _, err := c.getdir(p)
	return dir, paths, err
}

func (c *fsEtcd) getdir(p Path) (*models.Directory, []Path, int64, error) {
	// promOps.WithLabelValues("getdir").Inc()
	clog.Tracef("getdir: %s", p.Key())
	do := c.Etcd.Client.Txn(c.getContext()).If(
		keyExists(etcd.MkKey("dirs", p.Key())),
	).Then(
		etcdv3.OpGet(etcd.MkKey("dirs", p.Key())),
		etcdv3.OpGet(etcd.MkKey("dirs", p.SubdirsPrefix()), etcdv3.WithPrefix()),
	)
	resp, err := do.Commit()
	if err != nil {
		return nil, nil, 0, err
	}
	if !resp.Succeeded {
		return nil, nil, 0, os.ErrNotExist
	}
	dirkv := resp.Responses[0].GetResponseRange().Kvs[0]
	outdir := &models.Directory{}
	err = outdir.Unmarshal(dirkv.Value)
	if err != nil {
		return nil, nil, 0, err
	}
	var outpaths []Path
	for _, kv := range resp.Responses[1].GetResponseRange().Kvs {
		s := bytes.SplitN(kv.Key, []byte{':'}, 3)
		outpaths = append(outpaths, Path{
			Volume: p.Volume,
			Path:   string(s[2]) + "/",
		})
	}
	clog.Tracef("outpaths %#v", outpaths)
	return outdir, outpaths, dirkv.Version, nil
}

func (c *fsEtcd) SetFileEntry(p Path, ent *models.FileEntry) error {
	// promOps.WithLabelValues("set-file-entry").Inc()
	_, err := c.AtomicModifyKey([]byte(etcd.MkKey("dirs", p.Key())), trySetFileEntry(p, ent))
	return err
}

func trySetFileEntry(p Path, ent *models.FileEntry) etcd.AtomicModifyFunc {
	return func(in []byte) ([]byte, interface{}, error) {
		dir := &models.Directory{}
		err := dir.Unmarshal(in)
		if err != nil {
			return nil, torus.INodeID(0), err
		}
		if dir.Files == nil {
			dir.Files = make(map[string]*models.FileEntry)
		}
		old := &models.FileEntry{}
		if v, ok := dir.Files[p.Filename()]; ok {
			old = v
		}
		if ent.Chain == 0 && ent.Sympath == "" {
			delete(dir.Files, p.Filename())
		} else {
			dir.Files[p.Filename()] = ent
		}
		bytes, err := dir.Marshal()
		return bytes, old, err
	}
}

func (c *fsEtcd) GetChainINode(base torus.INodeRef) (torus.INodeRef, error) {
	pageID := etcd.Uint64ToHex(uint64(base.INode / chainPageSize))
	volume := etcd.Uint64ToHex(uint64(base.Volume()))
	rangekey := etcd.MkKey("volumemeta", "chain", volume, pageID)
	resp, err := c.Etcd.Client.Get(c.getContext(), rangekey, etcdv3.WithPrefix())

	if len(resp.Kvs) == 0 {
		return torus.INodeRef{}, nil
	}
	set := &models.FileChainSet{}
	err = set.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return torus.INodeRef{}, err
	}
	v, ok := set.Chains[uint64(base.INode)]
	if !ok {
		return torus.INodeRef{}, err
	}
	return torus.NewINodeRef(base.Volume(), torus.INodeID(v)), nil
}

func (c *fsEtcd) SetChainINode(base torus.INodeRef, was torus.INodeRef, new torus.INodeRef) error {
	// promOps.WithLabelValues("set-chain-inode").Inc()
	pageID := etcd.Uint64ToHex(uint64(base.INode / chainPageSize))
	volume := etcd.Uint64ToHex(uint64(base.Volume()))
	_, err := c.AtomicModifyKey([]byte(etcd.MkKey("volumemeta", "chain", volume, pageID)), func(b []byte) ([]byte, interface{}, error) {
		set := &models.FileChainSet{}
		if len(b) == 0 {
			set.Chains = make(map[uint64]uint64)
		} else {
			err := set.Unmarshal(b)
			if err != nil {
				return nil, nil, err
			}
		}
		v, ok := set.Chains[uint64(base.INode)]
		if !ok {
			v = 0
		}
		if v != uint64(was.INode) {
			return nil, nil, torus.ErrCompareFailed
		}
		if new.INode != 0 {
			set.Chains[uint64(base.INode)] = uint64(new.INode)
		} else {
			delete(set.Chains, uint64(base.INode))
		}
		b, err := set.Marshal()
		return b, was.INode, err
	})
	return err
}

func (c *fsEtcd) GetVolumeLiveness(volumeID torus.VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error) {
	//promOps.WithLabelValues("get-volume-liveness").Inc()
	volume := etcd.Uint64ToHex(uint64(volumeID))
	do := c.Etcd.Client.Txn(c.getContext()).Then(
		etcdv3.OpGet(etcd.MkKey("volumemeta", "deadmap", volume)),
		etcdv3.OpGet(etcd.MkKey("volumemeta", "open", volume), etcdv3.WithPrefix()),
	)
	resp, err := do.Commit()
	if err != nil {
		return nil, nil, err
	}
	deadmap := bytesToRoaring(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	var l []*roaring.Bitmap
	for _, x := range resp.Responses[1].GetResponseRange().Kvs {
		l = append(l, bytesToRoaring(x.Value))
	}
	return deadmap, l, nil
}

func (c *fsEtcd) ClaimVolumeINodes(lease int64, volumeID torus.VolumeID, inodes *roaring.Bitmap) error {
	if lease == 0 {
		return errors.New("no lease")
	}
	// promOps.WithLabelValues("claim-volume-inodes").Inc()
	volume := etcd.Uint64ToHex(uint64(volumeID))
	key := etcd.MkKey("volumemeta", "open", volume, c.UUID())
	if inodes == nil {
		_, err := c.Etcd.Client.Delete(c.getContext(), string(key), etcdv3.WithPrefix())
		return err
	}
	data := roaringToBytes(inodes)
	_, err := c.Etcd.Client.Put(c.getContext(),
		key, string(data), etcdv3.WithLease(etcdv3.LeaseID(lease)))
	return err
}

func (c *fsEtcd) ModifyDeadMap(volumeID torus.VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error {
	// promOps.WithLabelValues("modify-deadmap").Inc()
	if clog.LevelAt(capnslog.DEBUG) {
		newdead := roaring.AndNot(dead, live)
		clog.Tracef("killing %s", newdead.String())
		revive := roaring.AndNot(live, dead)
		clog.Tracef("reviving %s", revive.String())
	}
	volume := etcd.Uint64ToHex(uint64(volumeID))
	_, err := c.AtomicModifyKey([]byte(etcd.MkKey("volumemeta", "deadmap", volume)), func(b []byte) ([]byte, interface{}, error) {
		bm := bytesToRoaring(b)
		bm.Or(dead)
		bm.AndNot(live)
		return roaringToBytes(bm), nil, nil
	})
	return err
}

func (c *fsEtcd) GetINodeChains(vid torus.VolumeID) ([]*models.FileChainSet, error) {
	volume := etcd.Uint64ToHex(uint64(vid))
	rangekey := etcd.MkKey("volumemeta", "chain", volume)
	resp, err := c.Etcd.Client.Get(c.getContext(), rangekey, etcdv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var out []*models.FileChainSet
	for _, x := range resp.Kvs {
		chains := &models.FileChainSet{}
		chains.Unmarshal(x.Value)
		out = append(out, chains)
	}
	return out, nil
}
