/*
Copyright 2024 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"fmt"
	"sds-lvm-csi/api/v1alpha1"
	"sds-lvm-csi/pkg/utils"

	kerrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (d *Driver) CreateVolume(ctx context.Context, request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	d.log.Info("method CreateVolume")

	d.log.Info("========== CreateVolume ============")
	d.log.Info(request.String())
	d.log.Info("========== CreateVolume ============")

	if len(request.Name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if request.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability cannot de empty")
	}

	var LvmBindingMode string
	switch request.GetParameters()[lvmBindingMode] {
	case BindingModeWFFC:
		LvmBindingMode = BindingModeWFFC
	case BindingModeI:
		LvmBindingMode = BindingModeI
	}
	d.log.Info(fmt.Sprintf("storage class LvmBindingMode: %s", LvmBindingMode))

	var LvmType string
	switch request.GetParameters()[lvmType] {
	case LLVTypeThin:
		LvmType = LLVTypeThin
	case LLVTypeThick:
		LvmType = LLVTypeThick
	}
	d.log.Info(fmt.Sprintf("storage class LvmType: %s", LvmType))

	lvmVG := make(map[string]string)
	if len(request.GetParameters()[lvmVolumeGroup]) != 0 {
		var lvmVolumeGroups LVMVolumeGroups
		err := yaml.Unmarshal([]byte(request.GetParameters()[lvmVolumeGroup]), &lvmVolumeGroups)
		if err != nil {
			d.log.Error(err, "unmarshal yaml lvmVolumeGroup")
		}
		for _, v := range lvmVolumeGroups {
			lvmVG[v.Name] = v.Thin.PoolName
		}
	}
	d.log.Info(fmt.Sprintf("lvm-volume-groups: %+v", lvmVG))

	llvName := request.Name
	d.log.Info(fmt.Sprintf("llv name: %s ", llvName))

	llvSize := resource.NewQuantity(request.CapacityRange.GetRequiredBytes(), resource.BinarySI)
	d.log.Info(fmt.Sprintf("llv size: %s ", llvSize.String()))

	var preferredNode string
	if LvmBindingMode == BindingModeI {
		prefNode, freeSpace, err := utils.GetNodeMaxFreeVGSize(ctx, d.cl)
		if err != nil {
			d.log.Error(err, "error GetNodeMaxVGSize")
		}
		preferredNode = prefNode
		if llvSize.Value() > freeSpace.Value() {
			return nil, status.Errorf(codes.Internal, "requested size: %s is greater than free space: %s", llvSize.String(), freeSpace.String())
		}
		d.log.Info(fmt.Sprintf("prefered node: %s, free space %s ", prefNode, freeSpace.String()))
	}

	if LvmBindingMode == BindingModeWFFC {
		if len(request.AccessibilityRequirements.Preferred) != 0 {
			t := request.AccessibilityRequirements.Preferred[0].Segments
			preferredNode = t[topologyKey]
		}
	}

	d.log.Info(fmt.Sprintf("prefered node: %s", preferredNode))
	d.log.Info(fmt.Sprintf("lvm-volume-groups: %+v", lvmVG))
	d.log.Info(fmt.Sprintf("lvm-type: %s", LvmType))
	lvmVolumeGroupName, vgName, err := utils.GetLVMVolumeGroupParams(ctx, d.cl, *d.log, lvmVG, preferredNode, LvmType)
	if err != nil {
		d.log.Error(err, "error GetVGName")
		// return nil, err
	}

	d.log.Info(fmt.Sprintf("LvmVolumeGroup: %s", lvmVolumeGroupName))
	d.log.Info(fmt.Sprintf("VGName: %s", vgName))
	d.log.Info(fmt.Sprintf("prefered node: %s", preferredNode))

	d.log.Info("------------ CreateLVMLogicalVolume ------------")
	llvThin := &v1alpha1.ThinLogicalVolumeSpec{}
	if LvmType == LLVTypeThick {
		llvThin = nil
	}
	if LvmType == LLVTypeThin {
		llvThin.PoolName = lvmVG[lvmVolumeGroupName]
	}

	spec := v1alpha1.LvmLogicalVolumeSpec{
		Type:           LvmType,
		Size:           *llvSize,
		LvmVolumeGroup: lvmVolumeGroupName,
		Thin:           llvThin,
	}

	d.log.Info(fmt.Sprintf("LvmLogicalVolumeSpec : %+v", spec))

	_, err = utils.CreateLVMLogicalVolume(ctx, d.cl, llvName, spec)
	if err != nil {
		if kerrors.IsAlreadyExists(err) {
			d.log.Info(fmt.Sprintf("LVMLogicalVolume %s already exists", llvName))
		} else {
			d.log.Error(err, "error CreateLVMLogicalVolume")
			return nil, err
		}
	}
	d.log.Info("------------ CreateLVMLogicalVolume ------------")

	d.log.Info("start wait CreateLVMLogicalVolume ")
	resizeDelta, err := resource.ParseQuantity(ResizeDelta)
	if err != nil {
		d.log.Error(err, "error ParseQuantity for ResizeDelta")
		return nil, err
	}
	attemptCounter, err := utils.WaitForStatusUpdate(ctx, d.cl, *d.log, request.Name, "", *llvSize, resizeDelta)
	if err != nil {
		d.log.Error(err, "error WaitForStatusUpdate")
		return nil, err
	}
	d.log.Info(fmt.Sprintf("stop wait CreateLVMLogicalVolume, attempt сounter = %d ", attemptCounter))

	//Create context
	volCtx := make(map[string]string)
	for k, v := range request.Parameters {
		volCtx[k] = v
	}

	volCtx[subPath] = request.Name
	volCtx[VGNameKey] = vgName

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			CapacityBytes: request.CapacityRange.GetRequiredBytes(),
			VolumeId:      request.Name,
			VolumeContext: volCtx,
			ContentSource: request.VolumeContentSource,
			AccessibleTopology: []*csi.Topology{
				{Segments: map[string]string{
					topologyKey: preferredNode,
				}},
			},
		},
	}, nil
}

func (d *Driver) DeleteVolume(ctx context.Context, request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	d.log.Info("method DeleteVolume")
	err := utils.DeleteLVMLogicalVolume(ctx, d.cl, request.VolumeId)
	if err != nil {
		d.log.Error(err, "error DeleteLVMLogicalVolume")
	}
	d.log.Info(fmt.Sprintf("delete volume %s", request.VolumeId))
	return &csi.DeleteVolumeResponse{}, nil
}

func (d *Driver) ControllerPublishVolume(ctx context.Context, request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	d.log.Info("method ControllerPublishVolume")
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{
			d.publishInfoVolumeName: request.VolumeId,
		},
	}, nil
}

func (d *Driver) ControllerUnpublishVolume(ctx context.Context, request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	d.log.Info("method ControllerUnpublishVolume")
	// todo called Immediate
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	d.log.Info("method ValidateVolumeCapabilities")
	return nil, nil
}

func (d *Driver) ListVolumes(ctx context.Context, request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	d.log.Info("method ListVolumes")
	return nil, nil
}

func (d *Driver) GetCapacity(ctx context.Context, request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	d.log.Info("method GetCapacity")

	//todo MaxSize one PV
	//todo call volumeBindingMode: WaitForFirstConsumer

	return &csi.GetCapacityResponse{
		AvailableCapacity: 1000000,
		MaximumVolumeSize: nil,
		MinimumVolumeSize: nil,
	}, nil
}

func (d *Driver) ControllerGetCapabilities(ctx context.Context, request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	d.log.Info("method ControllerGetCapabilities")
	capabilities := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	}

	csiCaps := make([]*csi.ControllerServiceCapability, len(capabilities))
	for i, capability := range capabilities {
		csiCaps[i] = &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: capability,
				},
			},
		}
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: csiCaps,
	}, nil
}

func (d *Driver) CreateSnapshot(ctx context.Context, request *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	d.log.Info(" call method CreateSnapshot")
	return nil, nil
}

func (d *Driver) DeleteSnapshot(ctx context.Context, request *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	d.log.Info(" call method DeleteSnapshot")
	return nil, nil
}

func (d *Driver) ListSnapshots(ctx context.Context, request *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	d.log.Info(" call method ListSnapshots")
	return nil, nil
}

func (d *Driver) ControllerExpandVolume(ctx context.Context, request *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	d.log.Info(" call method ControllerExpandVolume")

	d.log.Info("========== ExpandVolume ============")
	d.log.Info(request.String())
	d.log.Info("========== ExpandVolume ============")

	volumeID := request.GetVolumeId()
	resizeDelta, err := resource.ParseQuantity(ResizeDelta)
	d.log.Trace("resizeDelta: %s", resizeDelta.String())
	requestCapacity := resource.NewQuantity(request.CapacityRange.GetRequiredBytes(), resource.BinarySI)
	d.log.Trace("requestCapacity: %s", requestCapacity.String())

	if err != nil {
		d.log.Error(err, "error ParseQuantity for ResizeDelta")
		return nil, err
	}

	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id cannot be empty")
	}

	llv, err := utils.GetLVMLogicalVolume(ctx, d.cl, volumeID, "")
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "LVMLogicalVolume with id: %s not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "error getting LVMLogicalVolume: %v", err)
	}

	if llv.Status.ActualSize.Value() > requestCapacity.Value()+resizeDelta.Value() || utils.AreSizesEqualWithinDelta(*requestCapacity, llv.Status.ActualSize, resizeDelta) {
		d.log.Warning("requested size is less than or equal to the actual size of the volume include delta %s , no need to resize LVMLogicalVolume %s, requested size: %s, actual size: %s, return NodeExpansionRequired: true and CapacityBytes: %d", resizeDelta.String(), volumeID, requestCapacity.String(), llv.Status.ActualSize.String(), llv.Status.ActualSize.Value())
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         llv.Status.ActualSize.Value(),
			NodeExpansionRequired: true,
		}, nil
	}

	lvg, err := utils.GetLVMVolumeGroup(ctx, d.cl, llv.Spec.LvmVolumeGroup, llv.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting LVMVolumeGroup: %v", err)
	}

	lvgCapacity, err := utils.GetLVMVolumeGroupCapacity(*lvg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting LVMVolumeGroupCapacity: %v", err)
	}

	if lvgCapacity.Value() < (requestCapacity.Value() - llv.Status.ActualSize.Value()) {
		return nil, status.Errorf(codes.Internal, "requested size: %s is greater than the capacity of the LVMVolumeGroup: %s", requestCapacity.String(), lvgCapacity.String())
	}

	d.log.Info("start resize LVMLogicalVolume")
	d.log.Info(fmt.Sprintf("requested size: %s, actual size: %s", requestCapacity.String(), llv.Status.ActualSize.String()))
	llv.Spec.Size = *requestCapacity
	err = utils.UpdateLVMLogicalVolume(ctx, d.cl, llv)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error updating LVMLogicalVolume: %v", err)
	}

	attemptCounter, err := utils.WaitForStatusUpdate(ctx, d.cl, *d.log, llv.Name, llv.Namespace, *requestCapacity, resizeDelta)
	if err != nil {
		d.log.Error(err, "error WaitForStatusUpdate")
		return nil, err
	}
	d.log.Info(fmt.Sprintf("finish resize LVMLogicalVolume, attempt сounter = %d ", attemptCounter))

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         request.CapacityRange.RequiredBytes,
		NodeExpansionRequired: true,
	}, nil
}

func (d *Driver) ControllerGetVolume(ctx context.Context, request *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	d.log.Info(" call method ControllerGetVolume")
	return &csi.ControllerGetVolumeResponse{}, nil
}

func (d *Driver) ControllerModifyVolume(ctx context.Context, request *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	d.log.Info(" call method ControllerModifyVolume")
	return &csi.ControllerModifyVolumeResponse{}, nil
}
