/*
Copyright 2020 The Kubernetes Authors.

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

package podstate

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	klog "k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type PodState struct {
	handle    framework.Handle
	pvLister  corelisters.PersistentVolumeLister
	pvcLister corelisters.PersistentVolumeClaimLister
	scLister  storagelisters.StorageClassLister
}

var scheme = runtime.NewScheme()

/*
	type VolumeLocal struct {
		pvLister  corelisters.PersistentVolumeLister
		pvcLister corelisters.PersistentVolumeClaimLister
		scLister  storagelisters.StorageClassLister
	}
*/
var _ = framework.ScorePlugin(&PodState{})

// Name is the name of the plugin used in the Registry and configurations.
const Name = "PodState"

func (ps *PodState) Name() string {
	return Name
}

// Score invoked at the score extension point.
func (ps *PodState) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {

	nodeInfo, err := ps.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}
	// putting to worker node here.

	//	volumeGroupIDs, _ := getVolumebyPod(ctx, pod)

	//	score := InterfaceToIGW(volumeGroupIDs)

	// pe.score favors nodes with terminating pods instead of nominated pods
	// It calculates the sum of the node's terminating pods and nominated pods
	return ps.score(nodeInfo)
}

// ScoreExtensions of the Score plugin.
func (ps *PodState) ScoreExtensions() framework.ScoreExtensions {
	return ps
}

func (ps *PodState) score(nodeInfo *framework.NodeInfo) (int64, *framework.Status) {
	var terminatingPodNum, nominatedPodNum int64
	// get nominated Pods for node from nominatedPodMap
	nominatedPodNum = int64(len(ps.handle.NominatedPodsForNode(nodeInfo.Node().Name)))
	for _, p := range nodeInfo.Pods {
		// Pod is terminating if DeletionTimestamp has been set
		if p.Pod.DeletionTimestamp != nil {
			terminatingPodNum++
		}
	}
	return terminatingPodNum - nominatedPodNum, nil
}

func (ps *PodState) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// Find highest and lowest scores.
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	for _, nodeScore := range scores {
		if nodeScore.Score > highest {
			highest = nodeScore.Score
		}
		if nodeScore.Score < lowest {
			lowest = nodeScore.Score
		}
	}

	// Transform the highest to lowest score range to fit the framework's min to max node score range.
	oldRange := highest - lowest
	newRange := framework.MaxNodeScore - framework.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + framework.MinNodeScore
		}
	}

	return nil
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	informerFactory := h.SharedInformerFactory()
	pvLister := informerFactory.Core().V1().PersistentVolumes().Lister()
	pvcLister := informerFactory.Core().V1().PersistentVolumeClaims().Lister()
	scLister := informerFactory.Storage().V1().StorageClasses().Lister()
	return &PodState{
		handle:    h,
		pvLister:  pvLister,
		pvcLister: pvcLister,
		scLister:  scLister,
	}, nil
}

/*
func (no *PodState) Filter(ctx context.Context,

		cycleState *framework.CycleState,
		pod *v1.Pod,
		nodeInfo *framework.NodeInfo) *framework.Status {
		if nodeInfo.Node() == nil {
			return framework.NewStatus(framework.Error, "node not found")
		}

		volumeInfo := getVolumebyPod(ctx, pod)
	}
*/

func (ps *PodState) getVolumebyPod(ctx context.Context, pod *v1.Pod) ([]string, *framework.Status) {

	var volumeDetails []string
	for i := range pod.Spec.Volumes {
		volume := pod.Spec.Volumes[i]
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvcName := volume.PersistentVolumeClaim.ClaimName
		if pvcName == "" {
			return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, "PersistentVolumeClaim had no name")
		}
		pvc, err := ps.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
		if s := getErrorAsStatus(err); !s.IsSuccess() {
			return nil, s
		}

		pvName := pvc.Spec.VolumeName
		if pvName == "" {
			scName := storagehelpers.GetPersistentVolumeClaimClass(pvc)
			if len(scName) == 0 {
				return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, "PersistentVolumeClaim had no pv name and storageClass name")
			}

			class, err := ps.scLister.Get(scName)
			if s := getErrorAsStatus(err); !s.IsSuccess() {
				return nil, s
			}
			if class.VolumeBindingMode == nil {
				return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("VolumeBindingMode not set for StorageClass %q", scName))
			}
			if *class.VolumeBindingMode == storage.VolumeBindingWaitForFirstConsumer {
				// Skip unbound volumes
				continue
			}

			return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, "PersistentVolume had no name")
		}

		pv, err := ps.pvLister.Get(pvName)
		if s := getErrorAsStatus(err); !s.IsSuccess() {
			return nil, s
		}

		volumeDetails = append(volumeDetails, pv.Spec.CSI.VolumeHandle)
	}
	return volumeDetails, nil
}

func getErrorAsStatus(err error) *framework.Status {
	if err != nil {
		if apierrors.IsNotFound(err) {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
		}
		return framework.AsStatus(err)
	}
	return nil
}

func createAndUpdateConfigMap(handle framework.Handle, data map[string]string) error {

	k8sclient, err := client.New(handle.KubeConfig(), client.Options{
		Scheme: scheme,
	})

	configMapList := v1.ConfigMapList{}
	scoreMapName := types.NamespacedName{
		Name:      "scoreCM",
		Namespace: "default",
	}
	listOpts := client.InNamespace(scoreMapName.Namespace)

	err = k8sclient.List(context.TODO(), &configMapList, listOpts)
	if err != nil {
		fmt.Printf("ConfigMap '%s' not found in namespace '%s'\n", scoreMapName.Name, scoreMapName.Namespace)
	}

	foundConfigMaps := len(configMapList.Items) > 0
	if foundConfigMaps {
		for _, existingConfigMap := range configMapList.Items {
			if existingConfigMap.Name == "score" {
				existingConfigMap.Data = data
				err = k8sclient.Update(context.TODO(), &existingConfigMap)
				if err != nil {
					klog.Error(err)
				}
			} else {
				configMap := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      scoreMapName.Name,
						Namespace: scoreMapName.Namespace,
					},
					Data: data,
				}
				err = k8sclient.Create(context.TODO(), configMap)
				if err != nil {
					klog.Error(err)
				}
			}
		}
	}
	return nil
}
