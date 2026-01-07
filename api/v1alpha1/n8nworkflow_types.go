/*
Copyright 2025.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// N8nReference defines a reference to an N8n instance
type N8nReference struct {
	// Name is the name of the N8n instance
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Namespace is the namespace of the N8n instance
	// +kubebuilder:validation:Optional
	Namespace string `json:"namespace,omitempty"`
}

// WorkflowNode defines a single node in an n8n workflow
type WorkflowNode struct {
	// ID is the unique identifier for this node
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ID string `json:"id"`
	// Name is the display name of the node
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Type is the node type (e.g., "n8n-nodes-base.start", "n8n-nodes-base.httpRequest")
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Type string `json:"type"`
	// TypeVersion is the version of the node type (must be a number like 1.3, not a string)
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Type=number
	// +kubebuilder:validation:Format=double
	// +crd:allowDangerousTypes=true
	TypeVersion float64 `json:"typeVersion,omitempty"`
	// Position defines the visual position of the node in the workflow editor [x, y]
	// +kubebuilder:validation:Optional
	Position []int `json:"position,omitempty"`
	// Parameters contains the configuration parameters for this node
	// +kubebuilder:validation:Optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Parameters *runtime.RawExtension `json:"parameters,omitempty"`
	// Credentials contains references to credentials used by this node
	// +kubebuilder:validation:Optional
	Credentials map[string]string `json:"credentials,omitempty"`
}

// WorkflowDefinition defines the structure of an n8n workflow
type WorkflowDefinition struct {
	// Name is the display name of the workflow
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Active indicates whether the workflow should be active
	// +kubebuilder:validation:Optional
	// +kubebuilder:default=false
	Active bool `json:"active,omitempty"`
	// Tags are labels associated with the workflow
	// +kubebuilder:validation:Optional
	Tags []string `json:"tags,omitempty"`
	// Nodes contains all the nodes in the workflow
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Nodes []WorkflowNode `json:"nodes"`
	// Connections defines how nodes are connected to each other
	// +kubebuilder:validation:Required
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Connections *runtime.RawExtension `json:"connections"`
	// Settings contains workflow-level settings
	// +kubebuilder:validation:Optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Settings *runtime.RawExtension `json:"settings,omitempty"`
}

// SecretReference defines a reference to a Kubernetes secret
type SecretReference struct {
	// Name is the name of the secret
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Namespace is the namespace of the secret
	// +kubebuilder:validation:Optional
	Namespace string `json:"namespace,omitempty"`
}

// CredentialSpec defines a credential that should be created in n8n
type CredentialSpec struct {
	// Name is the name of the credential in n8n
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Type is the credential type (e.g., "httpBasicAuth", "apiKey", "oauth2")
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=httpBasicAuth;apiKey;oauth2;httpHeaderAuth;httpQueryAuth
	Type string `json:"type"`
	// SecretRef is a reference to the Kubernetes secret containing credential data
	// +kubebuilder:validation:Required
	SecretRef SecretReference `json:"secretRef"`
}

// N8nWorkflowSpec defines the desired state of N8nWorkflow
type N8nWorkflowSpec struct {
	// N8nRef is a reference to the N8n instance where this workflow should be deployed
	// +kubebuilder:validation:Required
	N8nRef N8nReference `json:"n8nRef"`
	// Workflow contains the workflow definition
	// +kubebuilder:validation:Required
	Workflow WorkflowDefinition `json:"workflow"`
	// Credentials contains references to secrets that should be created as n8n credentials
	// +kubebuilder:validation:Optional
	Credentials []CredentialSpec `json:"credentials,omitempty"`
}

// N8nWorkflowStatus defines the observed state of N8nWorkflow
type N8nWorkflowStatus struct {
	// WorkflowID is the ID of the workflow in n8n
	// +kubebuilder:validation:Optional
	WorkflowID string `json:"workflowId,omitempty"`
	// Active indicates whether the workflow is currently active in n8n
	// +kubebuilder:validation:Optional
	Active bool `json:"active,omitempty"`
	// LastSync is the timestamp of the last successful sync with n8n
	// +kubebuilder:validation:Optional
	LastSync *metav1.Time `json:"lastSync,omitempty"`
	// SyncStatus indicates the current sync status with n8n
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=Synced;Syncing;Failed;Unknown
	SyncStatus string `json:"syncStatus,omitempty"`
	// ErrorMessage contains the last error message if sync failed
	// +kubebuilder:validation:Optional
	ErrorMessage string `json:"errorMessage,omitempty"`
	// CredentialsSynced indicates whether all required credentials have been synced
	// +kubebuilder:validation:Optional
	CredentialsSynced bool `json:"credentialsSynced,omitempty"`
	// CredentialIDs maps credential names to their IDs in n8n
	// +kubebuilder:validation:Optional
	CredentialIDs map[string]string `json:"credentialIds,omitempty"`
	// ObservedGeneration reflects the generation of the most recently observed N8nWorkflow
	// +kubebuilder:validation:Optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions represent the latest available observations of the workflow's state
	// +kubebuilder:validation:Optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=n8nwf
// +kubebuilder:printcolumn:name="Workflow ID",type="string",JSONPath=".status.workflowId"
// +kubebuilder:printcolumn:name="Active",type="boolean",JSONPath=".status.active"
// +kubebuilder:printcolumn:name="Sync Status",type="string",JSONPath=".status.syncStatus"
// +kubebuilder:printcolumn:name="N8n Instance",type="string",JSONPath=".spec.n8nRef.name"
// +kubebuilder:printcolumn:name="Last Sync",type="date",JSONPath=".status.lastSync"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// N8nWorkflow is the Schema for the n8nworkflows API
type N8nWorkflow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   N8nWorkflowSpec   `json:"spec,omitempty"`
	Status N8nWorkflowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// N8nWorkflowList contains a list of N8nWorkflow
type N8nWorkflowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []N8nWorkflow `json:"items"`
}

func init() {
	SchemeBuilder.Register(&N8nWorkflow{}, &N8nWorkflowList{})
}
