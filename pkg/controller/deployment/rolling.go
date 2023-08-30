/*
Copyright 2016 The Kubernetes Authors.

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

package deployment

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
	"k8s.io/utils/integer"
)

type State int

var isRestart bool = true
var isPause bool = false
var FirstPause bool
var RestartInvalid bool = true
var status State = StateNormal

const (
	StateNormal State = iota
	StatePause
	StateRestart
)

var StateIntenal bool = false

// rolloutRolling implements the logic for rolling a new replica set.
func (dc *DeploymentController) rolloutRolling(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet) error {
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, true)
	if err != nil {
		return err
	}
	allRSs := append(oldRSs, newRS)

	// pauseNum := deploymentutil.ProportionalPause(*d)

	// if _, ok := d.Annotations["pause_restart"]; !ok && pauseNum == -1 {
	// 	d.Annotations["pause_restart_status"] = "Normal"
	// 	status = StateNormal
	// } else if ok && pauseNum == -1 {
	// 	d.Annotations["pause_restart_status"] = "Restart"
	// 	status = StateRestart
	// } else if pauseNum != -1 && !ok {
	// 	d.Annotations["pause_restart_status"] = "Pause"
	// 	status = StatePause
	// }

	var pauseNumStr string
	if d.Spec.PauseNum == nil {
		pauseNumStr = "nil"
	} else {
		pauseNumStr = strconv.Itoa(int(*d.Spec.PauseNum))
	}

	pauseRestartStr := strconv.FormatBool(d.Spec.PauseRestart)

	dc.eventRecorder.Eventf(d, v1.EventTypeNormal, "Check pauseNum is %s", pauseNumStr)
	dc.eventRecorder.Eventf(d, v1.EventTypeNormal, "Check pauseRestart is %s", pauseRestartStr)

	// var isRestart bool
	// var firstRestart bool
	// 判断是否为重启, 使用添加标签判断是否为重启之后的状态，逻辑写在RestartDeployment中
	// if FirstPause, ok := d.Annotations["pause_first_restart"]; !ok { // 没有pause_first_restart标签，则说明重启还未成功

	// 	if isPause && !isRestart {
	// 		if d.Spec.PauseRestart {
	// 			if err := dc.RestartDeployment(ctx, d); err != nil {
	// 				return err
	// 			}
	// 			isPause = false
	// 			isRestart = true
	// 			// 设置一个暂停状态拉起的等待时间
	// 			// time.Sleep(3000 * time.Millisecond)
	// 			//释放信号量
	// 			d.Annotations["pause_first_restart"] = "true"
	// 		}
	// 	}

	// 	if !isPause && isRestart && deploymentutil.ProportionalPause(*d) != -1 {
	// 		// 若不是重启，则进行判断是否暂停的操作。
	// 		if deploymentutil.IsProportionComplete(d, allRSs, newRS) {
	// 			if err := dc.PauseDeployment(ctx, d); err != nil {
	// 				klog.Error(err)
	// 				return err
	// 			}
	// 			klog.Warning("Deployment is paused")
	// 			isPause = true
	// 			//释放信号量
	// 			d.Annotations["pause_first_restart"] = "true"
	// 		}
	// 	}
	// } else {

	// if isPause && !isRestart && status == StateRestart && RestartInvalid {
	// if dc.restartLabel {
	// 	err := dc.RestartDeployment(ctx, d)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	// TODO 重启deployment之后，删除旧的replicaset
	// 	// 考虑如何设置newRS和oldRS的
	// 	// for _, oldRS := range oldRSs {
	// 	// 	dc.deleteReplicaSet(oldRS)
	// 	// }
	// 	isPause = false
	// 	isRestart = true
	// 	RestartInvalid = false
	// 	dc.restartLabel = false
	// 	d.Annotations["pause_first_restart"] = "true"

	// }

	// 若中途更新，则重置状态
	// if dc.updateAgain {
	// 	status = StateNormal
	// 	dc.updateAgain = false
	// }

	if d.Spec.PauseRestart && status == StatePause {
		if err := dc.RestartDeployment(ctx, d); err != nil {
			return err
		}
		klog.Warning("Deployment is restart")
		// oldRSs = append(oldRSs, newRS)
		*newRS.Spec.Replicas = *d.Spec.Replicas
		isPause = false
		isRestart = true
		status = StateRestart
		// 设置一个暂停状态拉起的等待时间
		// time.Sleep(3000 * time.Millisecond)
		//释放信号量
		d.Annotations["pause_first_restart"] = "true"
	}

	if !isPause && isRestart && d.Spec.PauseNum != nil || d.Spec.PauseProportion != nil && deploymentutil.ProportionalPause(*d) != -1 && status == StateNormal {
		// 若不是重启，则进行判断是否暂停的操作。
		if deploymentutil.IsProportionComplete(d, allRSs, newRS) {
			if err := dc.PauseDeployment(ctx, d); err != nil {
				klog.Error(err)
				return err
			}
			klog.Warning("Deployment is paused")
			isPause = true
			isRestart = false
			status = StatePause
			//释放信号量
			d.Annotations["pause_first_restart"] = "true"
			// return nil
		}
	}

	// Scale up, if we can.
	scaledUp, err := dc.reconcileNewReplicaSet(ctx, allRSs, newRS, d)
	if err != nil {
		return err
	}
	if scaledUp {
		// Update DeploymentStatus
		return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	// Scale down, if we can.
	scaledDown, err := dc.reconcileOldReplicaSets(ctx, allRSs, controller.FilterActiveReplicaSets(oldRSs), newRS, d)
	if err != nil {
		return err
	}
	if scaledDown {
		// Update DeploymentStatus
		return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	if deploymentutil.DeploymentComplete(d, &d.Status) {
		if err := dc.cleanupDeployment(ctx, oldRSs, d); err != nil {
			return err
		}

		if status == StateRestart {
			status = StateNormal
			dpCopy := d.DeepCopy()
			*dpCopy.Spec.PauseNum = -1
			_, err := dc.client.AppsV1().Deployments(dpCopy.Namespace).Update(ctx, dpCopy, metav1.UpdateOptions{})
			if err == nil {
				dc.eventRecorder.Eventf(d, v1.EventTypeNormal, "DeloymentFinished", "Deployment Status become to %s", "2")
			}
		}
	}

	// Sync deployment status
	return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
}

func (dc *DeploymentController) reconcileNewReplicaSet(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	if *(newRS.Spec.Replicas) == *(deployment.Spec.Replicas) {
		// Scaling not required.
		return false, nil
	}
	if *(newRS.Spec.Replicas) > *(deployment.Spec.Replicas) {
		// Scale down.
		scaled, _, err := dc.scaleReplicaSetAndRecordEvent(ctx, newRS, *(deployment.Spec.Replicas), deployment)
		return scaled, err
	}
	newReplicasCount, err := deploymentutil.NewRSNewReplicas(deployment, allRSs, newRS)
	if err != nil {
		return false, err
	}
	scaled, _, err := dc.scaleReplicaSetAndRecordEvent(ctx, newRS, newReplicasCount, deployment)
	return scaled, err
}

func (dc *DeploymentController) reconcileOldReplicaSets(ctx context.Context, allRSs []*apps.ReplicaSet, oldRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	logger := klog.FromContext(ctx)
	oldPodsCount := deploymentutil.GetReplicaCountForReplicaSets(oldRSs)
	if oldPodsCount == 0 {
		// Can't scale down further
		return false, nil
	}
	allPodsCount := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	logger.V(4).Info("New replica set", "replicaSet", klog.KObj(newRS), "availableReplicas", newRS.Status.AvailableReplicas)
	maxUnavailable := deploymentutil.MaxUnavailable(*deployment)

	// Check if we can scale down. We can scale down in the following 2 cases:
	// * Some old replica sets have unhealthy replicas, we could safely scale down those unhealthy replicas since that won't further
	//  increase unavailability.
	// * New replica set has scaled up and it's replicas becomes ready, then we can scale down old replica sets in a further step.
	//
	// maxScaledDown := allPodsCount - minAvailable - newReplicaSetPodsUnavailable
	// take into account not only maxUnavailable and any surge pods that have been created, but also unavailable pods from
	// the newRS, so that the unavailable pods from the newRS would not make us scale down old replica sets in a further
	// step(that will increase unavailability).
	//
	// Concrete example:
	//
	// * 10 replicas
	// * 2 maxUnavailable (absolute number, not percent)
	// * 3 maxSurge (absolute number, not percent)
	//
	// case 1:
	// * Deployment is updated, newRS is created with 3 replicas, oldRS is scaled down to 8, and newRS is scaled up to 5.
	// * The new replica set pods crashloop and never become available.
	// * allPodsCount is 13. minAvailable is 8. newRSPodsUnavailable is 5.
	// * A node fails and causes one of the oldRS pods to become unavailable. However, 13 - 8 - 5 = 0, so the oldRS won't be scaled down.
	// * The user notices the crashloop and does kubectl rollout undo to rollback.
	// * newRSPodsUnavailable is 1, since we rolled back to the good replica set, so maxScaledDown = 13 - 8 - 1 = 4. 4 of the crashlooping pods will be scaled down.
	// * The total number of pods will then be 9 and the newRS can be scaled up to 10.
	//
	// case 2:
	// Same example, but pushing a new pod template instead of rolling back (aka "roll over"):
	// * The new replica set created must start with 0 replicas because allPodsCount is already at 13.
	// * However, newRSPodsUnavailable would also be 0, so the 2 old replica sets could be scaled down by 5 (13 - 8 - 0), which would then
	// allow the new replica set to be scaled up by 5.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	newRSUnavailablePodCount := *(newRS.Spec.Replicas) - newRS.Status.AvailableReplicas
	maxScaledDown := allPodsCount - minAvailable - newRSUnavailablePodCount
	if maxScaledDown <= 0 {
		return false, nil
	}

	// Clean up unhealthy replicas first, otherwise unhealthy replicas will block deployment
	// and cause timeout. See https://github.com/kubernetes/kubernetes/issues/16737
	oldRSs, cleanupCount, err := dc.cleanupUnhealthyReplicas(ctx, oldRSs, deployment, maxScaledDown)
	if err != nil {
		return false, nil
	}
	logger.V(4).Info("Cleaned up unhealthy replicas from old RSes", "count", cleanupCount)

	// Scale down old replica sets, need check maxUnavailable to ensure we can scale down
	allRSs = append(oldRSs, newRS)
	scaledDownCount, err := dc.scaleDownOldReplicaSetsForRollingUpdate(ctx, allRSs, oldRSs, deployment)
	if err != nil {
		return false, nil
	}
	logger.V(4).Info("Scaled down old RSes", "deployment", klog.KObj(deployment), "count", scaledDownCount)

	totalScaledDown := cleanupCount + scaledDownCount
	return totalScaledDown > 0, nil
}

// cleanupUnhealthyReplicas will scale down old replica sets with unhealthy replicas, so that all unhealthy replicas will be deleted.
func (dc *DeploymentController) cleanupUnhealthyReplicas(ctx context.Context, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment, maxCleanupCount int32) ([]*apps.ReplicaSet, int32, error) {
	logger := klog.FromContext(ctx)
	sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))
	// Safely scale down all old replica sets with unhealthy replicas. Replica set will sort the pods in the order
	// such that not-ready < ready, unscheduled < scheduled, and pending < running. This ensures that unhealthy replicas will
	// been deleted first and won't increase unavailability.
	totalScaledDown := int32(0)
	for i, targetRS := range oldRSs {
		if totalScaledDown >= maxCleanupCount {
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			// cannot scale down this replica set.
			continue
		}
		logger.V(4).Info("Found available pods in old RS", "replicaSet", klog.KObj(targetRS), "availableReplicas", targetRS.Status.AvailableReplicas)
		if *(targetRS.Spec.Replicas) == targetRS.Status.AvailableReplicas {
			// no unhealthy replicas found, no scaling required.
			continue
		}

		scaledDownCount := int32(integer.IntMin(int(maxCleanupCount-totalScaledDown), int(*(targetRS.Spec.Replicas)-targetRS.Status.AvailableReplicas)))
		newReplicasCount := *(targetRS.Spec.Replicas) - scaledDownCount
		if newReplicasCount > *(targetRS.Spec.Replicas) {
			return nil, 0, fmt.Errorf("when cleaning up unhealthy replicas, got invalid request to scale down %s/%s %d -> %d", targetRS.Namespace, targetRS.Name, *(targetRS.Spec.Replicas), newReplicasCount)
		}
		_, updatedOldRS, err := dc.scaleReplicaSetAndRecordEvent(ctx, targetRS, newReplicasCount, deployment)
		if err != nil {
			return nil, totalScaledDown, err
		}
		totalScaledDown += scaledDownCount
		oldRSs[i] = updatedOldRS
	}
	return oldRSs, totalScaledDown, nil
}

// scaleDownOldReplicaSetsForRollingUpdate scales down old replica sets when deployment strategy is "RollingUpdate".
// Need check maxUnavailable to ensure availability
func (dc *DeploymentController) scaleDownOldReplicaSetsForRollingUpdate(ctx context.Context, allRSs []*apps.ReplicaSet, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment) (int32, error) {
	logger := klog.FromContext(ctx)
	maxUnavailable := deploymentutil.MaxUnavailable(*deployment)

	// Check if we can scale down.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	// Find the number of available pods.
	availablePodCount := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	if availablePodCount <= minAvailable {
		// Cannot scale down.
		return 0, nil
	}
	logger.V(4).Info("Found available pods in deployment, scaling down old RSes", "deployment", klog.KObj(deployment), "availableReplicas", availablePodCount)

	sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))

	totalScaledDown := int32(0)
	totalScaleDownCount := availablePodCount - minAvailable
	for _, targetRS := range oldRSs {
		if totalScaledDown >= totalScaleDownCount {
			// No further scaling required.
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			// cannot scale down this ReplicaSet.
			continue
		}
		// Scale down.
		scaleDownCount := int32(integer.IntMin(int(*(targetRS.Spec.Replicas)), int(totalScaleDownCount-totalScaledDown)))
		newReplicasCount := *(targetRS.Spec.Replicas) - scaleDownCount
		if newReplicasCount > *(targetRS.Spec.Replicas) {
			return 0, fmt.Errorf("when scaling down old RS, got invalid request to scale down %s/%s %d -> %d", targetRS.Namespace, targetRS.Name, *(targetRS.Spec.Replicas), newReplicasCount)
		}
		_, _, err := dc.scaleReplicaSetAndRecordEvent(ctx, targetRS, newReplicasCount, deployment)
		if err != nil {
			return totalScaledDown, err
		}

		totalScaledDown += scaleDownCount
	}

	return totalScaledDown, nil
}
