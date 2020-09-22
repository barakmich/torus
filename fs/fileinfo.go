package fs

import (
	"errors"
	"os"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/torus/models"
	"github.com/tgruben/roaring"
)

type FileInfo struct {
	Path torus.Path

	// And one of
	Ref   torus.INodeRef
	INode *models.INode

	Dir *models.Directory

	Symlink string
}

func (fi FileInfo) Name() string {
	return fi.Path.Path
}

func (fi FileInfo) Size() int64 {
	if fi.IsDir() {
		return int64(len(fi.Dir.Files))
	}
	if fi.Symlink != "" {
		return 0
	}
	return int64(fi.INode.Filesize)
}

func (fi FileInfo) Mode() os.FileMode {
	if fi.IsDir() {
		return os.FileMode(fi.Dir.Metadata.Mode)
	}
	if fi.Symlink != "" {
		return 0777 | os.ModeSymlink
	}
	return os.FileMode(fi.INode.Permissions.Mode)
}

func (fi FileInfo) ModTime() time.Time {
	if fi.IsDir() {
		return time.Unix(0, int64(fi.Dir.Metadata.Mtime))
	}
	if fi.Symlink != "" {
		return time.Unix(0, 0)
	}
	return time.Unix(0, int64(fi.INode.Permissions.Mtime))
}

func (fi FileInfo) IsDir() bool {
	return fi.Path.IsDir()
}

func (fi FileInfo) Sys() interface{} {
	return fi
}

func (s *server) Lstat(path torus.Path) (os.FileInfo, error) {
	promOps.WithLabelValues("lstat").Inc()
	s.mut.RLock()
	defer s.mut.RUnlock()
	for _, v := range s.openFileChains {
		for _, x := range v.fh.inode.Filenames {
			if path.Path == x {
				return FileInfo{
					INode: v.fh.inode,
					Path:  path,
					Ref:   torus.NewINodeRef(torus.VolumeID(v.fh.inode.Volume), torus.INodeID(v.fh.inode.INode)),
				}, nil
			}
		}
	}
	clog.Tracef("lstat %s", path)
	if path.IsDir() {
		clog.Tracef("is dir")
		d, _, err := s.fsMDS().Getdir(path)
		return FileInfo{
			Path: path,
			Dir:  d,
		}, err
	}
	vol, ent, err := s.FileEntryForPath(path)
	if err != nil {
		return nil, err
	}
	if ent.Sympath != "" {
		return FileInfo{
			Path:    path,
			Ref:     torus.NewINodeRef(vol, torus.INodeID(0)),
			Symlink: ent.Sympath,
		}, nil
	}
	ref, err := s.fsMDS().GetChainINode(torus.NewINodeRef(vol, torus.INodeID(ent.Chain)))
	if err != nil {
		return nil, err
	}

	inode, err := s.inodes.GetINode(s.getContext(), ref)

	if err != nil {
		return nil, err
	}

	return FileInfo{
		INode: inode,
		Path:  path,
		Ref:   ref,
	}, nil
}

func (s *server) Readdir(path torus.Path) ([]torus.Path, error) {
	promOps.WithLabelValues("readdir").Inc()
	if !path.IsDir() {
		return nil, errors.New("ENOTDIR")
	}

	dir, subdirs, err := s.fsMDS().Getdir(path)
	if err != nil {
		return nil, err
	}

	var entries []torus.Path
	entries = append(entries, subdirs...)

	for filename := range dir.Files {
		childPath, ok := path.Child(filename)
		if !ok {
			return nil, errors.New("server: entry path is not a directory")
		}

		entries = append(entries, childPath)
	}

	return entries, nil
}

func (s *server) Mkdir(path torus.Path, md *models.Metadata) error {
	promOps.WithLabelValues("mkdir").Inc()
	if !path.IsDir() {
		return os.ErrInvalid
	}
	return s.fsMDS().Mkdir(path, md)
}
func (s *server) CreateFSVolume(vol string) error {
	v := &models.Volume{
		Name: vol,
		Type: models.Volume_FILE,
	}
	err := s.mds.CreateVolume(v)
	if err == torus.ErrAgain {
		return s.CreateFSVolume(vol)
	}
	return err
}

func (s *server) incRef(vol string, bm *roaring.Bitmap) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if bm.GetCardinality() == 0 {
		return
	}
	if _, ok := s.openINodeRefs[vol]; !ok {
		s.openINodeRefs[vol] = make(map[torus.INodeID]int)
	}
	it := bm.Iterator()
	for it.HasNext() {
		id := torus.INodeID(it.Next())
		v, ok := s.openINodeRefs[vol][id]
		if !ok {
			s.openINodeRefs[vol][id] = 1
		} else {
			s.openINodeRefs[vol][id] = v + 1
		}
	}
}

func (s *server) decRef(vol string, bm *roaring.Bitmap) {
	s.mut.Lock()
	defer s.mut.Unlock()
	it := bm.Iterator()
	for it.HasNext() {
		id := torus.INodeID(it.Next())
		v, ok := s.openINodeRefs[vol][id]
		if !ok {
			panic("server: double remove of an inode reference")
		} else {
			v--
			if v == 0 {
				delete(s.openINodeRefs[vol], id)
			} else {
				s.openINodeRefs[vol][id] = v
			}
		}
	}
	if len(s.openINodeRefs[vol]) == 0 {
		delete(s.openINodeRefs, vol)
	}
}

func (s *server) getBitmap(vol string) (*roaring.Bitmap, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if _, ok := s.openINodeRefs[vol]; !ok {
		return nil, false
	}
	out := roaring.NewBitmap()
	for k := range s.openINodeRefs[vol] {
		out.Add(uint32(k))
	}
	return out, true
}

func (s *server) Remove(path torus.Path) error {
	promOps.WithLabelValues("remove").Inc()
	if path.IsDir() {
		return s.removeDir(path)
	}
	return s.removeFile(path)
}

// TODO(barakmich): Split into two functions, one for chain ID and one for path.
func (s *server) updateINodeChain(ctx context.Context, p torus.Path, modFunc func(oldINode *models.INode, vol torus.VolumeID) (*models.INode, torus.INodeRef, error)) (*models.INode, torus.INodeRef, error) {
	notExist := false
	vol, entry, err := s.FileEntryForPath(p)
	clog.Tracef("vol: %v, entry, %v, err %s", vol, entry, err)
	ref := torus.NewINodeRef(vol, torus.INodeID(0))
	if err != nil {
		if err != os.ErrNotExist {
			return nil, ref, err
		}
		notExist = true
		entry = &models.FileEntry{}
	} else {
		if entry.Sympath != "" {
			return nil, ref, torus.ErrIsSymlink
		}
	}
	clog.Tracef("notexist: %v", notExist)
	chainRef := torus.NewINodeRef(vol, torus.INodeID(entry.Chain))
	for {
		var inode *models.INode
		if !notExist {
			ref, err = s.fsMDS().GetChainINode(chainRef)
			clog.Tracef("ref: %s", ref)
			if err != nil {
				return nil, ref, err
			}
			if ref.INode != 0 {
				inode, err = s.inodes.GetINode(ctx, ref)
				if err != nil {
					return nil, ref, err
				}
			}
		}
		newINode, newRef, err := modFunc(inode, vol)
		if err != nil {
			return nil, ref, err
		}
		if chainRef.INode == 0 {
			err = s.fsMDS().SetChainINode(newRef, chainRef, newRef)
		} else {
			err = s.fsMDS().SetChainINode(chainRef, ref, newRef)
		}
		if err == nil {
			return newINode, ref, s.inodes.WriteINode(ctx, newRef, newINode)
		}
		if err == torus.ErrCompareFailed {
			continue
		}
		return nil, ref, err
	}
}

func (s *server) removeDir(path torus.Path) error {
	return s.fsMDS().Rmdir(path)
}

func (s *server) addOpenFile(chainID uint64, fh *fileHandle) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if v, ok := s.openFileChains[chainID]; ok {
		v.count++
		if fh != v.fh {
			panic("different pointers?")
		}
		s.openFileChains[chainID] = v
	} else {
		s.openFileChains[chainID] = openFileCount{fh, 1}
	}
	clog.Tracef("addOpenFile %#v", s.openFileChains)
}

func (s *server) getOpenFile(chainID uint64) (fh *fileHandle) {
	if v, ok := s.openFileChains[chainID]; ok {
		clog.Tracef("got open file %v", s.openFileChains)
		return v.fh
	}
	clog.Tracef("did not get open file")
	return nil
}

func (s *server) removeOpenFile(chainID uint64) {
	s.mut.Lock()
	v, ok := s.openFileChains[chainID]
	if !ok {
		panic("removing unopened handle")
	}
	v.count--
	if v.count != 0 {
		s.openFileChains[chainID] = v
		s.mut.Unlock()
		return
	}
	delete(s.openFileChains, chainID)
	s.mut.Unlock()
	v.fh.mut.Lock()
	defer v.fh.mut.Unlock()
	v.fh.updateHeldINodes(true)
	clog.Tracef("removeOpenFile %#v", s.openFileChains)
}
