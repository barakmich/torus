package fs

import (
	"errors"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
	"github.com/coreos/pkg/capnslog"
	"github.com/tgruben/roaring"
)

var clog = capnslog.NewPackageLogger("github.com/coreos/torus", "fs")

type fsMetadata interface {
	torus.MetadataService

	Mkdir(path Path, dir *models.Metadata) error
	ChangeDirMetadata(path Path, dir *models.Metadata) error
	Getdir(path Path) (*models.Directory, []Path, error)
	Rmdir(path Path) error
	SetFileEntry(path Path, ent *models.FileEntry) error

	GetINodeChains(vid torus.VolumeID) ([]*models.FileChainSet, error)
	GetChainINode(base torus.INodeRef) (torus.INodeRef, error)
	SetChainINode(base torus.INodeRef, was torus.INodeRef, new torus.INodeRef) error

	ClaimVolumeINodes(lease int64, vol torus.VolumeID, inodes *roaring.Bitmap) error

	ModifyDeadMap(vol torus.VolumeID, live *roaring.Bitmap, dead *roaring.Bitmap) error
	GetVolumeLiveness(vol torus.VolumeID) (*roaring.Bitmap, []*roaring.Bitmap, error)
}

func OpenFSVolume(mds torus.MetadataService, name string) FSVolume {
	panic("unimplemented -- only works with etcd metadata")
}

func CreateFSVolume(mds torus.MetadataService, name string) error {
	panic("unimplemented -- only works with etcd metadata")
}

func createFSMetadata(mds torus.MetadataService, vid torus.VolumeID) (fsMetadata, error) {
	switch mds.Kind() {
	case torus.EtcdMetadata:
		return createFSEtcdMetadata(mds, vid)
	case torus.TempMetadata:
		return createFSTempMetadata(mds, vid)
	default:
		return nil, errors.New("unimplemented for this kind of metadata")
	}
}
