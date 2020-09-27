package fs

import (
	"os"
	"path"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
)

type GenericFS interface {
	// Standard file path calls.
	Create(string) (*File, error)
	Open(string) (*File, error)
	OpenFile(p string, flag int, perm os.FileMode) (*File, error)
	OpenFileMetadata(p string, flag int, md *models.Metadata) (*File, error)
	Rename(from string, to string) error
	Link(from string, to string) error
	Symlink(ref string, to Path) error
	Lstat(string) (os.FileInfo, error)
	Readdir(string) ([]string, error)
	Remove(string) error
	Mkdir(string, *models.Metadata) error

	Chmod(name string, mode os.FileMode) error
	Chown(name string, uid, gid int) error
}

type FSServer struct {
	srv *torus.Server
}

type openFileCount struct {
	fh    *fileHandle
	count int
}

type Volume struct {
	*torus.Server
	mds            FsMetadataService
	volumeID       torus.VolumeID
	openINodeRefs  map[string]map[torus.INodeID]int
	openFileChains map[uint64]openFileCount
}

func OpenFS(srv *torus.Server) *FSServer {
	return &FSServer{
		srv: srv,
	}
}

func (s *FSServer) CreateFSVol(volume *models.Volume) (*Volume, error) {
	panic("to implement")
}

func (s *FSServer) OpenFSVol(volume *models.Volume) (*Volume, error) {
	panic("to implement")
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
