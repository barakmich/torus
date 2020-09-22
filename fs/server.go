package fs

import (
	"os"
	"path"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
)

type FSServer interface {
	torus.Server

	// Standard file path calls.
	Create(Path) (File, error)
	Open(Path) (File, error)
	OpenFile(p Path, flag int, perm os.FileMode) (File, error)
	OpenFileMetadata(p Path, flag int, md *models.Metadata) (File, error)
	Rename(p Path, new Path) error
	Link(p Path, new Path) error
	Symlink(to string, new Path) error
	Lstat(Path) (os.FileInfo, error)
	Readdir(Path) ([]Path, error)
	Remove(Path) error
	Mkdir(Path, *models.Metadata) error

	Chmod(name Path, mode os.FileMode) error
	Chown(name Path, uid, gid int) error
}

type openFileCount struct {
	fh    *fileHandle
	count int
}

type Volume struct {
	*torus.Server
	openINodeRefs  map[string]map[torus.INodeID]int
	openFileChains map[uint64]openFileCount
}

func (s *FSServer) FileEntryForPath(p torus.Path) (torus.VolumeID, *models.FileEntry, error) {
	promOps.WithLabelValues("file-entry-for-path").Inc()
	dirname, filename := path.Split(p.Path)
	dirpath := torus.Path{p.Volume, dirname}
	dir, _, err := s.fsMDS().Getdir(dirpath)
	if err != nil {
		return torus.VolumeID(0), nil, err
	}

	vol, err := s.mds.GetVolume(p.Volume)
	if err != nil {
		return 0, nil, err
	}
	ent, ok := dir.Files[filename]
	if !ok {
		return torus.VolumeID(vol.Id), nil, os.ErrNotExist
	}

	return torus.VolumeID(vol.Id), ent, nil
}

func (s *FSServer) inodeRefForPath(p torus.Path) (torus.INodeRef, error) {
	vol, ent, err := s.FileEntryForPath(p)
	if err != nil {
		return torus.INodeRef{}, err
	}
	if ent.Sympath != "" {
		return s.inodeRefForPath(torus.Path{p.Volume, path.Clean(p.Base() + "/" + ent.Sympath)})
	}
	return s.fsMDS().GetChainINode(torus.NewINodeRef(vol, torus.INodeID(ent.Chain)))
}
