package k8s

import (
	"encoding/json"
	"fmt"
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DiscoveryHandlerFunc for mock resource
var DiscoveryHandlerFunc = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	var obj interface{}
	switch req.URL.Path {
	case "/apis/batch.volcano.sh/v1alpha1":
		obj = &metav1.APIResourceList{
			GroupVersion: "batch.volcano.sh/v1alpha1",
			APIResources: []metav1.APIResource{
				{Name: "jobs", Namespaced: true, Kind: "Job"},
			},
		}
	case "/apis/scheduling.volcano.sh/v1beta1":
		obj = &metav1.APIResourceList{
			GroupVersion: "scheduling.volcano.sh/v1beta1",
			APIResources: []metav1.APIResource{
				{Name: "queues", Namespaced: false, Kind: "Queue"},
				{Name: "elasticresourcequotas", Namespaced: false, Kind: "ElasticResourceQuota"},
			},
		}
	case "/apis/sparkoperator.k8s.io/v1beta2":
		obj = &metav1.APIResourceList{
			GroupVersion: "sparkoperator.k8s.io/v1beta2",
			APIResources: []metav1.APIResource{
				{Name: "sparkapplications", Namespaced: true, Kind: "SparkApplication"},
			},
		}
	case "/apis/batch.paddlepaddle.org/v1":
		obj = &metav1.APIResourceList{
			GroupVersion: "batch.paddlepaddle.org/v1",
			APIResources: []metav1.APIResource{
				{Name: "paddlejobs", Namespaced: true, Kind: "PaddleJob"},
			},
		}
	case "/apis/kubeflow.org/v1":
		obj = &metav1.APIResourceList{
			GroupVersion: "kubeflow.org/v1",
			APIResources: []metav1.APIResource{
				{Name: "pytorchjobs", Namespaced: true, Kind: "PyTorchJob"},
				{Name: "tfjobs", Namespaced: true, Kind: "TFJob"},
				{Name: "mpijobs", Namespaced: true, Kind: "MPIJob"},
			},
		}
	case "/apis/argoproj.io/v1alpha1":
		obj = &metav1.APIResourceList{
			GroupVersion: "argoproj.io/v1alpha1",
			APIResources: []metav1.APIResource{
				{Name: "workflows", Namespaced: true, Kind: "Workflow"},
			},
		}
	case "/apis/ray.io/v1alpha1":
		obj = &metav1.APIResourceList{
			GroupVersion: "ray.io/v1alpha1",
			APIResources: []metav1.APIResource{
				{Name: "rayjobs", Namespaced: true, Kind: "RayJob"},
			},
		}
	case "/api/v1":
		obj = &metav1.APIResourceList{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod"},
				{Name: "namespaces", Namespaced: false, Kind: "Namespace"},
				{Name: "configmaps", Namespaced: true, Kind: "ConfigMap"},
			},
		}
	case "/api":
		obj = &metav1.APIVersions{
			Versions: []string{
				"v1",
			},
		}
	case "/apis":
		obj = &metav1.APIGroupList{
			Groups: []metav1.APIGroup{
				{
					Name: "batch.volcano.sh",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "batch.volcano.sh/v1alpha1", Version: "v1alpha1"},
					},
				},
				{
					Name: "scheduling.volcano.sh",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "scheduling.volcano.sh/v1beta1", Version: "v1beta1"},
					},
				},
				{
					Name: "sparkoperator.k8s.io",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "sparkoperator.k8s.io/v1beta2", Version: "v1beta2"},
					},
				},
				{
					Name: "batch.paddlepaddle.org",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "batch.paddlepaddle.org/v1", Version: "v1"},
					},
				},
				{
					Name: "kubeflow.org",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "kubeflow.org/v1", Version: "v1"},
					},
				},
				{
					Name: "argoproj.io",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "argoproj.io/v1alpha1", Version: "v1alpha1"},
					},
				},
				{
					Name: "ray.io",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "ray.io/v1alpha1", Version: "v1alpha1"},
					},
				},
			},
		}
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
	output, err := json.Marshal(obj)
	if err != nil {
		fmt.Printf("unexpected encoding error: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(output); err != nil {
		fmt.Printf("unexpected w.Write error: %v", err)
		return
	}
})
