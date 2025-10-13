package controller

import (
	"context"
	"fmt"
	instoragev1alpha1 "instorage-operator/pkg/apis/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// [TODO]
// 1. CSD 노드 스케줄링 로직
// 2. 정적 CSD 라벨 기반 -> 동적 CSD 모니터링 상태 기반

// selectCSDNode - CSD 노드 선택 로직
func (r *InstorageJobReconciler) selectCSDNode(job *instoragev1alpha1.InstorageJob) (string, error) {
	// 1. 먼저 nodeScheduling.nodeName이 지정되어 있는지 확인
	if job.Spec.NodeScheduling != nil && job.Spec.NodeScheduling.NodeName != "" {
		specifiedNode := job.Spec.NodeScheduling.NodeName
		r.Log.Info("Using specified node from nodeScheduling", "node", specifiedNode, "job", job.Name)

		// 지정된 노드가 CSD 노드인지 확인
		node := &corev1.Node{}
		if err := r.Get(context.Background(), types.NamespacedName{Name: specifiedNode}, node); err != nil {
			return "", fmt.Errorf("specified node %s not found: %w", specifiedNode, err)
		}

		// CSD 라벨 확인
		if node.Labels["csd.enabled"] != "true" || node.Labels["csd.status"] != "ready" {
			return "", fmt.Errorf("specified node %s is not a ready CSD node", specifiedNode)
		}

		return specifiedNode, nil
	}

	// 2. nodeScheduling.nodeName이 없으면 CSD 노드 라벨로 필터링
	nodes := &corev1.NodeList{}
	labelSelector := labels.SelectorFromSet(labels.Set{
		"csd.enabled": "true",
		"csd.status":  "ready",
	})

	if err := r.List(context.Background(), nodes, &client.ListOptions{
		LabelSelector: labelSelector,
	}); err != nil {
		return "", fmt.Errorf("failed to list CSD nodes: %w", err)
	}

	if len(nodes.Items) == 0 {
		return "", fmt.Errorf("no CSD nodes available")
	}

	// 현재는 단순히 첫 번째 노드 선택 (추후 스케줄링 로직 개선 가능)
	selectedNode := nodes.Items[0].Name
	r.Log.Info("Selected CSD node", "node", selectedNode, "job", job.Name)

	return selectedNode, nil
}
