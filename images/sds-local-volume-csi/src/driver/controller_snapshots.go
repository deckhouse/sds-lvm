//go:build !ee

package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func (d *Driver) CreateSnapshot(_ context.Context, _ *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	d.log.Info("call method CreateSnapshot")
	return nil, nil
}

func (d *Driver) DeleteSnapshot(_ context.Context, _ *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	d.log.Info("call method DeleteSnapshot")
	return nil, nil
}

func (d *Driver) ListSnapshots(_ context.Context, _ *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	d.log.Info("call method ListSnapshots")
	return nil, nil
}
