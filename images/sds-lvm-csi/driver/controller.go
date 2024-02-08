/*
Copyright 2023 Flant JSC

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
		prefNode, freeSpace, err := utils.GetNodeMaxVGSize(ctx, d.cl)
		if err != nil {
			d.log.Error(err, "error GetNodeMaxVGSize")
		}
		preferredNode = prefNode
		d.log.Info(fmt.Sprintf("prefered node: %s, free space %s ", prefNode, freeSpace))
	}

	if LvmBindingMode == BindingModeWFFC {
		if len(request.AccessibilityRequirements.Preferred) != 0 {
			t := request.AccessibilityRequirements.Preferred[0].Segments
			preferredNode = t[topologyKey]
		}
	}

	lvmVolumeGroupName, vgName, err := utils.GetVGName(ctx, d.cl, lvmVG, preferredNode, LvmType)
	if err != nil {
		d.log.Error(err, "error GetVGName")
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

	err, llv := utils.CreateLVMLogicalVolume(ctx, d.cl, llvName, spec)
	if err != nil {
		d.log.Error(err, "error CreateLVMLogicalVolume")
		// todo if llv exist?
		//return nil, err
	}
	d.log.Info("------------ CreateLVMLogicalVolume ------------")

	d.log.Info("start wait CreateLVMLogicalVolume ")
	attemptCounter, err := utils.WaitForStatusUpdate(ctx, d.cl, request.Name, llv.Namespace)
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