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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition type constants for CdnTenant status
const (
	// TypeReady indicates whether the CdnTenant is fully operational
	// and all resources (namespace, deployment, service) are ready.
	TypeReady = "Ready"

	// TypeProgressing indicates that the controller is actively
	// reconciling the CdnTenant resources.
	TypeProgressing = "Progressing"

	// TypeDegraded indicates that the CdnTenant is in a degraded state,
	// meaning some functionality may be impaired but the tenant is still operational.
	TypeDegraded = "Degraded"

	// TypeSecondariesSynced indicates whether the tenant configuration
	// has been successfully synchronized to all secondary CDN servers.
	TypeSecondariesSynced = "SecondariesSynced"
)

// Condition reason constants for CdnTenant status
const (
	// ReasonReconciling indicates the controller is actively reconciling.
	ReasonReconciling = "Reconciling"

	// ReasonReconcileSuccess indicates reconciliation completed successfully.
	ReasonReconcileSuccess = "ReconcileSuccess"

	// ReasonReconcileFailed indicates reconciliation failed.
	ReasonReconcileFailed = "ReconcileFailed"

	// ReasonNamespaceFailed indicates namespace creation/update failed.
	ReasonNamespaceFailed = "NamespaceFailed"

	// ReasonDeploymentFailed indicates deployment creation/update failed.
	ReasonDeploymentFailed = "DeploymentFailed"

	// ReasonServiceFailed indicates service creation/update failed.
	ReasonServiceFailed = "ServiceFailed"

	// ReasonAllResourcesReady indicates all managed resources are ready.
	ReasonAllResourcesReady = "AllResourcesReady"

	// ReasonResourcesNotReady indicates some managed resources are not ready.
	ReasonResourcesNotReady = "ResourcesNotReady"

	// ReasonSyncInProgress indicates secondary synchronization is in progress.
	ReasonSyncInProgress = "SyncInProgress"

	// ReasonSyncSuccess indicates secondary synchronization completed successfully.
	ReasonSyncSuccess = "SyncSuccess"

	// ReasonSyncFailed indicates secondary synchronization failed.
	ReasonSyncFailed = "SyncFailed"

	// ReasonNoSecondaries indicates no secondary servers are configured.
	ReasonNoSecondaries = "NoSecondaries"

	// ReasonPartialFailure indicates some operations succeeded but others failed.
	ReasonPartialFailure = "PartialFailure"

	// ReasonDeleting indicates the resource is being deleted.
	ReasonDeleting = "Deleting"

	// ReasonDeploymentNotReady indicates the deployment is not yet ready.
	ReasonDeploymentNotReady = "DeploymentNotReady"

	// ReasonDomainTLSPending indicates one or more domain certificates are not ready.
	ReasonDomainTLSPending = "DomainTLSPending"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type Domain struct {
	// +required
	Name string `json:"name"`

	// +optional
	Cert string `json:"cert,omitempty"`

	// +optional
	Key string `json:"key,omitempty"`

	// +optional
	TLS *DomainTLS `json:"tls,omitempty"`

	Config map[string]string `json:"config,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!(has(self.minVersion) && has(self.maxVersion) && self.minVersion == 'TLSv1.3' && self.maxVersion == 'TLSv1.2')",message="minVersion cannot be greater than maxVersion"
type DomainTLS struct {
	// +kubebuilder:validation:Enum=provided;letsencrypt
	// +optional
	Mode string `json:"mode,omitempty"`

	// +kubebuilder:validation:Enum=TLSv1.2;TLSv1.3
	// +optional
	MinVersion string `json:"minVersion,omitempty"`

	// +kubebuilder:validation:Enum=TLSv1.2;TLSv1.3
	// +optional
	MaxVersion string `json:"maxVersion,omitempty"`

	// +optional
	RedirectHTTPToHTTPS *bool `json:"redirectHttpToHttps,omitempty"`
}

type Origin struct {
	// +required
	// +kubebuilder:validation:MaxLength=2048
	Url string `json:"url"`

	// +optional
	// Name identifies this origin in generated config, logs, and health output.
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +optional
	// Weight controls load-balancing preference. Defaults to 1 when omitted.
	Weight *int32 `json:"weight,omitempty"`

	// +optional
	HealthCheck *OriginHealthCheck `json:"healthCheck,omitempty"`

	Config map[string]string `json:"config,omitempty"`
}

type OriginHealthCheck struct {
	// +optional
	// Enabled controls active origin health checks. Defaults to true when omitted.
	Enabled *bool `json:"enabled,omitempty"`

	// +kubebuilder:validation:Pattern=`^/.*`
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	// Path is requested for active origin health checks. Defaults to / when omitted.
	Path string `json:"path,omitempty"`

	// +kubebuilder:validation:Minimum=100
	// +kubebuilder:validation:Maximum=599
	// +optional
	// ExpectedStatus is the HTTP status considered healthy. Defaults to 200 when omitted.
	ExpectedStatus *int32 `json:"expectedStatus,omitempty"`

	// +optional
	// Interval is the active health check interval, e.g. 10s. Defaults to 10s when omitted.
	Interval string `json:"interval,omitempty"`

	// +optional
	// Timeout is the active health check timeout, e.g. 2s. Defaults to 2s when omitted.
	Timeout string `json:"timeout,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +optional
	// HealthyThreshold is the consecutive success count required to mark healthy. Defaults to 2 when omitted.
	HealthyThreshold *int32 `json:"healthyThreshold,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +optional
	// UnhealthyThreshold is the consecutive failure count required to mark unhealthy. Defaults to 3 when omitted.
	UnhealthyThreshold *int32 `json:"unhealthyThreshold,omitempty"`
}

type ElasticsearchConfig struct {
	// +required
	// Enable or disable Elasticsearch logging
	Enabled bool `json:"enabled"`

	// +required
	// Configuration Options
	Config map[string]string `json:"config"`
}

// CdnTenantSpec defines the desired state of CdnTenant
type CdnTenantSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:XValidation:rule="self.all(o, o.url.matches('^https?://[^/?#]+($|[/?#])'))",message="origin URLs must use http or https scheme and include a host"
	// +kubebuilder:validation:XValidation:rule="self.size() == 1 || self.all(o, !(o.url.matches('^https?://[^/]+/.+')))",message="path-prefixed origin URLs are not supported with multiple origins"
	Origins []Origin `json:"origins"`

	// +kubebuilder:validation:MinItems=1
	Domains []Domain `json:"domains"`

	// +optional
	// Elasticsearch configuration for sending access logs
	Elasticsearch *ElasticsearchConfig `json:"elasticsearch,omitempty"`

	Config map[string]string `json:"config,omitempty"`
}

// CdnTenantStatus defines the observed state of CdnTenant.
type CdnTenantStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the CdnTenant resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	DomainTLS []DomainTLSStatus `json:"domainTLS,omitempty"`
}

type DomainTLSStatus struct {
	// +required
	Name string `json:"name"`

	// +required
	Mode string `json:"mode"`

	// +required
	Ready bool `json:"ready"`

	// +required
	Reason string `json:"reason"`

	// +required
	Message string `json:"message"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// CdnTenant is the Schema for the cdntenants API
type CdnTenant struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of CdnTenant
	// +required
	Spec CdnTenantSpec `json:"spec"`

	// status defines the observed state of CdnTenant
	// +optional
	Status CdnTenantStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// CdnTenantList contains a list of CdnTenant
type CdnTenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CdnTenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CdnTenant{}, &CdnTenantList{})
}
