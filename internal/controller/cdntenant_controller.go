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

package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	cdnv1 "github.com/CloudWebManage/cwm-cdn-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	tenantFinalizer          = "cdn.cloudwm-cdn.com/finalizer"
	tenantAnnotationKey      = "cdn.cloudwm-cdn.com/tenant"
	configAnnotationKey      = "cdn.cloudwm-cdn.com/config"
	configAnnotationValue    = "true"
	configSecretName         = "cwm-cdn-tenants-config"
	secondariesAnnotationKey = "cdn.cloudwm-cdn.com/secondaries-"
	configEnvVarPrefix       = "CWM_CDN_TENANT_DEFAULT_"
	defaultImage             = "ghcr.io/cloudwebmanage/cwm-cdn-api-tenant-nginx:latest"
	defaultReplicas          = 1
	defaultClusterIssuer     = "letsencrypt"
	http01SolverLabelKey     = "cdn.cloudwm-cdn.com/acme-http01-solver"
	placeholderCertKey       = "cdn.cloudwm-cdn.com/placeholder-certificate"
)

// CdnTenantReconciler reconciles a CdnTenant object
type CdnTenantReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// setCondition sets a condition on the tenant status and updates it in the API server.
func (r *CdnTenantReconciler) setCondition(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, conditionType string, status metav1.ConditionStatus, reason, message string) error {
	// Re-fetch the tenant first to get the latest version
	if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to re-fetch tenant")
		return err
	}
	meta.SetStatusCondition(&tenant.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: tenant.Generation,
	})
	if err := r.Status().Update(ctx, tenant); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to update Tenant status")
		return err
	}
	return nil
}

// setConditions sets multiple conditions on the tenant status and updates it in the API server.
func (r *CdnTenantReconciler) setConditions(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, conditions []metav1.Condition) error {
	logf.FromContext(ctx).V(1).Info("Setting conditions", "conditions", conditions)
	// Re-fetch the tenant first to get the latest version
	if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to re-fetch tenant")
		return err
	}
	for _, cond := range conditions {
		cond.ObservedGeneration = tenant.Generation
		meta.SetStatusCondition(&tenant.Status.Conditions, cond)
	}
	if err := r.Status().Update(ctx, tenant); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to update Tenant status")
		return err
	}
	return nil
}

// setProgressingCondition sets the Progressing condition to indicate reconciliation is in progress.
func (r *CdnTenantReconciler) setProgressingCondition(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, message string) error {
	return r.setConditions(ctx, req, tenant, []metav1.Condition{
		{
			Type:    cdnv1.TypeProgressing,
			Status:  metav1.ConditionTrue,
			Reason:  cdnv1.ReasonReconciling,
			Message: message,
		},
		{
			Type:    cdnv1.TypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonReconciling,
			Message: message,
		},
	})
}

// setErrorConditions sets conditions indicating a reconciliation failure.
func (r *CdnTenantReconciler) setErrorConditions(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, reason, message string) error {
	return r.setConditions(ctx, req, tenant, []metav1.Condition{
		{
			Type:    cdnv1.TypeProgressing,
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: message,
		},
		{
			Type:    cdnv1.TypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: message,
		},
		{
			Type:    cdnv1.TypeDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reason,
			Message: message,
		},
	})
}

// setSuccessConditions sets conditions indicating successful reconciliation.
func (r *CdnTenantReconciler) setSuccessConditions(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, secondariesSynced bool) error {
	conditions := []metav1.Condition{
		{
			Type:    cdnv1.TypeProgressing,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonReconcileSuccess,
			Message: "Reconciliation completed successfully",
		},
		{
			Type:    cdnv1.TypeReady,
			Status:  metav1.ConditionTrue,
			Reason:  cdnv1.ReasonAllResourcesReady,
			Message: "All resources are ready",
		},
		{
			Type:    cdnv1.TypeDegraded,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonReconcileSuccess,
			Message: "Tenant is fully operational",
		},
	}

	if secondariesSynced {
		conditions = append(conditions, metav1.Condition{
			Type:    cdnv1.TypeSecondariesSynced,
			Status:  metav1.ConditionTrue,
			Reason:  cdnv1.ReasonSyncSuccess,
			Message: "All secondary CDN servers are synchronized",
		})
	}

	return r.setConditions(ctx, req, tenant, conditions)
}

// setSecondariesSyncingCondition sets conditions indicating secondary sync is in progress.
func (r *CdnTenantReconciler) setSecondariesSyncingCondition(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, message string) error {
	return r.setConditions(ctx, req, tenant, []metav1.Condition{
		{
			Type:    cdnv1.TypeSecondariesSynced,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonSyncInProgress,
			Message: message,
		},
		{
			Type:    cdnv1.TypeDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  cdnv1.ReasonSyncFailed,
			Message: message,
		},
	})
}

// setDeletingConditions sets conditions indicating the tenant is being deleted.
func (r *CdnTenantReconciler) setDeletingConditions(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, message string) error {
	return r.setConditions(ctx, req, tenant, []metav1.Condition{
		{
			Type:    cdnv1.TypeProgressing,
			Status:  metav1.ConditionTrue,
			Reason:  cdnv1.ReasonDeleting,
			Message: message,
		},
		{
			Type:    cdnv1.TypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonDeleting,
			Message: message,
		},
	})
}

// handleReconcileError logs the error and sets error conditions.
func (r *CdnTenantReconciler) handleReconcileError(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, err error, reason, message string) (ctrl.Result, error) {
	logf.FromContext(ctx).Error(err, message)
	if e := r.setErrorConditions(ctx, req, tenant, reason, message); e != nil {
		return ctrl.Result{}, e
	}
	return ctrl.Result{}, err
}

// handleRequeueWithCondition sets syncing conditions and returns a requeue result.
func (r *CdnTenantReconciler) handleRequeueWithCondition(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, message string, requeueAfter time.Duration) (ctrl.Result, error) {
	logf.FromContext(ctx).Info(message)
	if e := r.setSecondariesSyncingCondition(ctx, req, tenant, message); e != nil {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// setDeploymentNotReadyConditions sets conditions indicating the deployment is not yet ready.
func (r *CdnTenantReconciler) setDeploymentNotReadyConditions(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, message string) error {
	return r.setConditions(ctx, req, tenant, []metav1.Condition{
		{
			Type:    cdnv1.TypeProgressing,
			Status:  metav1.ConditionTrue,
			Reason:  cdnv1.ReasonDeploymentNotReady,
			Message: message,
		},
		{
			Type:    cdnv1.TypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonDeploymentNotReady,
			Message: message,
		},
		{
			Type:    cdnv1.TypeDegraded,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonDeploymentNotReady,
			Message: "Waiting for deployment to become ready",
		},
	})
}

func (r *CdnTenantReconciler) setDomainTLSPendingConditions(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, message string) error {
	return r.setConditions(ctx, req, tenant, []metav1.Condition{
		{
			Type:    cdnv1.TypeProgressing,
			Status:  metav1.ConditionTrue,
			Reason:  cdnv1.ReasonDomainTLSPending,
			Message: message,
		},
		{
			Type:    cdnv1.TypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonDomainTLSPending,
			Message: message,
		},
		{
			Type:    cdnv1.TypeDegraded,
			Status:  metav1.ConditionFalse,
			Reason:  cdnv1.ReasonDomainTLSPending,
			Message: "Waiting for domain TLS certificates to become ready",
		},
	})
}

func allDomainTLSReady(statuses []cdnv1.DomainTLSStatus) bool {
	for _, status := range statuses {
		if !status.Ready {
			return false
		}
	}
	return true
}

// isDeploymentReady checks if a deployment has the desired number of ready replicas.
func isDeploymentReady(deployment *appsv1.Deployment) bool {
	if deployment.Spec.Replicas == nil {
		return deployment.Status.ReadyReplicas > 0
	}
	return deployment.Status.ReadyReplicas >= *deployment.Spec.Replicas
}

func domainTLSMode(domain cdnv1.Domain) string {
	if domain.TLS == nil || domain.TLS.Mode == "" {
		return "provided"
	}
	return domain.TLS.Mode
}

func domainTLSMinVersion(domain cdnv1.Domain) string {
	if domain.TLS == nil || domain.TLS.MinVersion == "" {
		return "TLSv1.2"
	}
	return domain.TLS.MinVersion
}

func domainTLSMaxVersion(domain cdnv1.Domain) string {
	if domain.TLS == nil || domain.TLS.MaxVersion == "" {
		return "TLSv1.3"
	}
	return domain.TLS.MaxVersion
}

func domainRedirectHTTPToHTTPS(domain cdnv1.Domain) bool {
	if domain.TLS == nil || domain.TLS.RedirectHTTPToHTTPS == nil {
		return false
	}
	return *domain.TLS.RedirectHTTPToHTTPS
}

func domainCertificateResourceName(index int, domainName string) string {
	return fmt.Sprintf("tenant-domain-%d-%s", index, domainCertificateHash(domainName))
}

func domainCertificateHash(domainName string) string {
	domainHash := sha256.Sum256([]byte(domainName))
	return hex.EncodeToString(domainHash[:])[:12]
}

func domainCertificateSecretName(index int, domainName string) string {
	return domainCertificateResourceName(index, domainName)
}

func domainCertificateMountPath(index int) string {
	return fmt.Sprintf("/certs/letsencrypt/%d", index)
}

func generatePlaceholderTLSSecretData(domainName string) (map[string][]byte, error) {
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, err
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	notBefore := time.Now().Add(-1 * time.Hour)
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: domainName,
		},
		NotBefore:             notBefore,
		NotAfter:              notBefore.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	if ip := net.ParseIP(domainName); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{domainName}
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return map[string][]byte{
		corev1.TLSCertKey:       certPEM,
		corev1.TLSPrivateKeyKey: keyPEM,
	}, nil
}

func certificateReady(certificate *unstructured.Unstructured) bool {
	conditions, ok, err := unstructured.NestedSlice(certificate.Object, "status", "conditions")
	if !ok || err != nil {
		return false
	}
	for _, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]interface{})
		if !ok {
			continue
		}
		conditionType, _, _ := unstructured.NestedString(condition, "type")
		conditionStatus, _, _ := unstructured.NestedString(condition, "status")
		if conditionType == "Ready" && conditionStatus == "True" {
			return true
		}
	}
	return false
}

func (r *CdnTenantReconciler) ensurePlaceholderTLSSecret(ctx context.Context, tenant *cdnv1.CdnTenant, domain cdnv1.Domain, secretName string) error {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: tenant.Name}, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		data, err := generatePlaceholderTLSSecretData(domain.Name)
		if err != nil {
			return err
		}
		placeholder := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: tenant.Name,
				Annotations: map[string]string{
					tenantAnnotationKey:          fmt.Sprintf("%s/%s", tenant.Namespace, tenant.Name),
					"cdn.cloudwm-cdn.com/domain": domain.Name,
					placeholderCertKey:           "true",
				},
				Labels: map[string]string{
					"cdn.cloudwm-cdn.com/tenant":      tenant.Name,
					"cdn.cloudwm-cdn.com/domain-hash": domainCertificateHash(domain.Name),
				},
			},
			Type: corev1.SecretTypeTLS,
			Data: data,
		}
		if err := r.Create(ctx, placeholder); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		return nil
	}
	return nil
}

func (r *CdnTenantReconciler) reconcileDomainTLSResources(ctx context.Context, tenant *cdnv1.CdnTenant) ([]cdnv1.DomainTLSStatus, error) {
	statuses := make([]cdnv1.DomainTLSStatus, 0, len(tenant.Spec.Domains))
	for i, domain := range tenant.Spec.Domains {
		mode := domainTLSMode(domain)
		status := cdnv1.DomainTLSStatus{Name: domain.Name, Mode: mode}
		switch mode {
		case "provided":
			if domain.Cert != "" && domain.Key != "" {
				status.Ready = true
				status.Reason = "ProvidedCertificateConfigured"
				status.Message = "Provided certificate and key are configured"
			} else {
				status.Reason = "ProvidedCertificateMissing"
				status.Message = "Provided TLS mode requires cert and key"
			}
		case "letsencrypt":
			secretName := domainCertificateSecretName(i, domain.Name)
			certificateName := domainCertificateResourceName(i, domain.Name)
			domainHash := domainCertificateHash(domain.Name)
			tenantReference := fmt.Sprintf("%s/%s", tenant.Namespace, tenant.Name)
			certificate := &unstructured.Unstructured{}
			certificate.SetAPIVersion("cert-manager.io/v1")
			certificate.SetKind("Certificate")
			certificate.SetName(certificateName)
			certificate.SetNamespace(tenant.Name)
			opres, err := controllerutil.CreateOrUpdate(ctx, r.Client, certificate, func() error {
				labels := certificate.GetLabels()
				if labels == nil {
					labels = map[string]string{}
				}
				labels["cdn.cloudwm-cdn.com/tenant"] = tenant.Name
				labels["cdn.cloudwm-cdn.com/domain-hash"] = domainHash
				labels[http01SolverLabelKey] = "true"
				certificate.SetLabels(labels)
				annotations := certificate.GetAnnotations()
				if annotations == nil {
					annotations = map[string]string{}
				}
				annotations["cdn.cloudwm-cdn.com/domain"] = domain.Name
				certificate.SetAnnotations(annotations)
				unstructured.SetNestedMap(certificate.Object, map[string]interface{}{
					"secretName": secretName,
					"dnsNames":   []interface{}{domain.Name},
					"issuerRef": map[string]interface{}{
						"name": defaultClusterIssuer,
						"kind": "ClusterIssuer",
					},
					"secretTemplate": map[string]interface{}{
						"annotations": map[string]interface{}{
							tenantAnnotationKey:          tenantReference,
							"cdn.cloudwm-cdn.com/domain": domain.Name,
						},
						"labels": map[string]interface{}{
							"cdn.cloudwm-cdn.com/tenant":      tenant.Name,
							"cdn.cloudwm-cdn.com/domain-hash": domainHash,
						},
					},
				}, "spec")
				return nil
			})
			if err != nil {
				return nil, err
			}
			logf.FromContext(ctx).V(1).Info("Reconciled Certificate", "name", certificateName, "namespace", tenant.Name, "opres", opres)
			if err := r.ensurePlaceholderTLSSecret(ctx, tenant, domain, secretName); err != nil {
				return nil, err
			}

			secret := &corev1.Secret{}
			if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: tenant.Name}, secret); err != nil {
				if apierrors.IsNotFound(err) {
					status.Reason = "CertificatePending"
					status.Message = "Waiting for Let's Encrypt HTTP-01 validation to issue the TLS secret"
				} else {
					return nil, err
				}
			} else if certificateReady(certificate) && len(secret.Data[corev1.TLSCertKey]) > 0 && len(secret.Data[corev1.TLSPrivateKeyKey]) > 0 {
				status.Ready = true
				status.Reason = "CertificateReady"
				status.Message = "Let's Encrypt certificate is ready"
			} else {
				status.Reason = "CertificatePending"
				status.Message = "Waiting for Let's Encrypt HTTP-01 validation to complete"
			}
		default:
			status.Reason = "UnsupportedTLSMode"
			status.Message = fmt.Sprintf("Unsupported TLS mode %q", mode)
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func (r *CdnTenantReconciler) updateDomainTLSStatus(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, statuses []cdnv1.DomainTLSStatus) error {
	if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
		return err
	}
	if equality.Semantic.DeepEqual(tenant.Status.DomainTLS, statuses) {
		return nil
	}
	tenant.Status.DomainTLS = statuses
	return r.Status().Update(ctx, tenant)
}

func (r *CdnTenantReconciler) getTenantSpecConfig(tenant *cdnv1.CdnTenant, configKey string, defaultValue string) string {
	value, ok := tenant.Spec.Config[configKey]
	if !ok || value == "" {
		value = os.Getenv(fmt.Sprintf("%s%s", configEnvVarPrefix, configKey))
		if value == "" {
			value = defaultValue
		}
	}
	return value
}

func (r *CdnTenantReconciler) getTenantSpecConfigInt(tenant *cdnv1.CdnTenant, configKey string, defaultValue int) int {
	value := r.getTenantSpecConfig(tenant, configKey, "")
	if value == "" {
		return defaultValue
	}
	valueInt, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return valueInt
}

// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CdnTenant object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *CdnTenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	tenant := &cdnv1.CdnTenant{}
	err := r.Get(ctx, req.NamespacedName, tenant)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("tenant resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get tenant")
		return ctrl.Result{}, err
	}
	r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", "start reconciliation")
	if tenant.DeletionTimestamp.IsZero() {
		hasUpdates := false
		if len(tenant.Status.Conditions) == 0 {
			log.V(1).Info("Setting initial conditions")
			hasUpdates = true
			if err = r.setProgressingCondition(ctx, req, tenant, "Starting reconciliation"); err != nil {
				return ctrl.Result{}, err
			}
		}
		if !controllerutil.ContainsFinalizer(tenant, tenantFinalizer) {
			log.V(1).Info("Adding finalizer")
			hasUpdates = true
			controllerutil.AddFinalizer(tenant, tenantFinalizer)
			if err := r.Update(ctx, tenant); err != nil {
				log.Error(err, "Failed to add finalizer to tenant")
				return ctrl.Result{}, err
			}
		}
		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: tenant.Name,
			},
		}
		opres, err := controllerutil.CreateOrUpdate(ctx, r.Client, namespace, func() error {
			return nil
		})
		if err != nil {
			return r.handleReconcileError(ctx, req, tenant, err, cdnv1.ReasonNamespaceFailed, "Failed to reconcile Namespace")
		}
		if opres != controllerutil.OperationResultNone {
			hasUpdates = true
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", fmt.Sprintf("Namespace %s", opres))
		}
		log.V(1).Info("Reconciled Namespace", "opres", opres)
		domainTLSStatuses, err := r.reconcileDomainTLSResources(ctx, tenant)
		if err != nil {
			return r.handleReconcileError(ctx, req, tenant, err, cdnv1.ReasonReconcileFailed, "Failed to reconcile domain TLS resources")
		}
		if err := r.updateDomainTLSStatus(ctx, req, tenant, domainTLSStatuses); err != nil {
			return ctrl.Result{}, err
		}
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tenant",
				Namespace: tenant.Name,
			},
		}
		opres, err = controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
			if deployment.CreationTimestamp.IsZero() {
				deployment.Spec.Selector = &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "tenant"},
				}
			}
			if deployment.Annotations == nil {
				deployment.Annotations = map[string]string{}
			}
			deployment.Annotations[tenantAnnotationKey] = fmt.Sprintf("%s/%s", tenant.Namespace, tenant.Name)
			if deployment.Labels == nil {
				deployment.Labels = map[string]string{}
			}
			deployment.Labels["app"] = "tenant"
			if deployment.Spec.Template.Labels == nil {
				deployment.Spec.Template.Labels = map[string]string{}
			}
			deployment.Spec.Template.Labels["app"] = "tenant"
			deployment.Spec.Replicas = ptr.To(int32(r.getTenantSpecConfigInt(tenant, "REPLICAS", defaultReplicas)))
			ps := &deployment.Spec.Template.Spec
			tolerations := []corev1.Toleration{{Key: "cwm-iac-worker-role", Operator: corev1.TolerationOpEqual, Value: "cdn", Effect: corev1.TaintEffectNoExecute}}
			if !equality.Semantic.DeepEqual(deployment.Spec.Template.Spec.Tolerations, tolerations) {
				ps.Tolerations = tolerations
			}
			if len(ps.Containers) == 0 {
				ps.Containers = []corev1.Container{
					{
						Name: "nginx",
					},
				}
			}
			c := &ps.Containers[0]
			c.Image = r.getTenantSpecConfig(tenant, "IMAGE", defaultImage)
			ports := []corev1.ContainerPort{
				{
					ContainerPort: 80,
				},
				{
					ContainerPort: 443,
				},
			}
			if !equality.Semantic.DeepEqual(c.Ports, ports) {
				c.Ports = ports
			}
			env := []corev1.EnvVar{
				{
					Name:  "TENANT_NAME",
					Value: tenant.Name,
				},
			}
			for i, domain := range tenant.Spec.Domains {
				env = append(env, corev1.EnvVar{
					Name:  fmt.Sprintf("D%d_NAME", i),
					Value: domain.Name,
				})
				mode := domainTLSMode(domain)
				if domain.TLS != nil {
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_TLS_MODE", i),
						Value: mode,
					})
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_TLS_MIN_VERSION", i),
						Value: domainTLSMinVersion(domain),
					})
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_TLS_MAX_VERSION", i),
						Value: domainTLSMaxVersion(domain),
					})
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_REDIRECT_HTTP_TO_HTTPS", i),
						Value: strconv.FormatBool(domainRedirectHTTPToHTTPS(domain)),
					})
				}
				if mode == "letsencrypt" {
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_CERT_PATH", i),
						Value: fmt.Sprintf("%s/%s", domainCertificateMountPath(i), corev1.TLSCertKey),
					})
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_KEY_PATH", i),
						Value: fmt.Sprintf("%s/%s", domainCertificateMountPath(i), corev1.TLSPrivateKeyKey),
					})
				} else {
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_CERT", i),
						Value: domain.Cert,
					})
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_KEY", i),
						Value: domain.Key,
					})
				}
				for k, v := range tenant.Spec.Domains[i].Config {
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("D%d_%s", i, strings.ToUpper(k)),
						Value: v,
					})
				}
			}
			for i, origin := range tenant.Spec.Origins {
				env = append(env, corev1.EnvVar{
					Name:  fmt.Sprintf("O%d_URL", i),
					Value: origin.Url,
				})
				for k, v := range tenant.Spec.Origins[i].Config {
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("O%d_%s", i, strings.ToUpper(k)),
						Value: v,
					})
				}
			}
			if tenant.Spec.Elasticsearch != nil && tenant.Spec.Elasticsearch.Enabled {
				env = append(env, corev1.EnvVar{
					Name:  "ENABLE_TENANT_ACCESS_LOGS",
					Value: "true",
				})
				env = append(env, corev1.EnvVar{
					Name:  "ENABLE_ES_SINK",
					Value: "true",
				})
				for k, v := range tenant.Spec.Elasticsearch.Config {
					env = append(env, corev1.EnvVar{
						Name:  fmt.Sprintf("ES_%s", strings.ToUpper(k)),
						Value: v,
					})
				}
			}
			for k, v := range tenant.Spec.Config {
				if !strings.Contains("IMAGE,REPLICAS", k) {
					env = append(env, corev1.EnvVar{
						Name:  strings.ToUpper(k),
						Value: v,
					})
				}
			}
			if !equality.Semantic.DeepEqual(deployment.Spec.Template.Spec.Containers[0].Env, env) {
				c.Env = env
			}
			volumes := []corev1.Volume{}
			volumeMounts := []corev1.VolumeMount{}
			optionalSecret := true
			for i, domain := range tenant.Spec.Domains {
				if domainTLSMode(domain) != "letsencrypt" {
					continue
				}
				volumeName := fmt.Sprintf("domain-cert-%d", i)
				volumes = append(volumes, corev1.Volume{
					Name: volumeName,
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: domainCertificateSecretName(i, domain.Name),
							Optional:   &optionalSecret,
						},
					},
				})
				volumeMounts = append(volumeMounts, corev1.VolumeMount{
					Name:      volumeName,
					MountPath: domainCertificateMountPath(i),
					ReadOnly:  true,
				})
			}
			if len(volumes) == 0 {
				volumes = nil
				volumeMounts = nil
			}
			if !equality.Semantic.DeepEqual(ps.Volumes, volumes) {
				ps.Volumes = volumes
			}
			if !equality.Semantic.DeepEqual(c.VolumeMounts, volumeMounts) {
				c.VolumeMounts = volumeMounts
			}
			return nil
		})
		if err != nil {
			return r.handleReconcileError(ctx, req, tenant, err, cdnv1.ReasonDeploymentFailed, "Failed to reconcile Deployment")
		}
		if opres != controllerutil.OperationResultNone {
			hasUpdates = true
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", fmt.Sprintf("Deployment %s", opres))
		}
		log.V(1).Info("Reconciled Deployment", "opres", opres)
		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tenant",
				Namespace: tenant.Name,
			},
		}
		opres, err = controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
			if service.Annotations == nil {
				service.Annotations = map[string]string{}
			}
			service.Annotations[tenantAnnotationKey] = fmt.Sprintf("%s/%s", tenant.Namespace, tenant.Name)
			if service.Labels == nil {
				service.Labels = map[string]string{}
			}
			service.Labels["app"] = "tenant"
			if service.Spec.Selector == nil {
				service.Spec.Selector = map[string]string{}
			}
			service.Spec.Selector["app"] = "tenant"
			ports := []corev1.ServicePort{
				{
					Name:       "http",
					Protocol:   corev1.ProtocolTCP,
					Port:       80,
					TargetPort: intstr.IntOrString{Type: intstr.Int, IntVal: 80},
				},
				{
					Name:       "https",
					Protocol:   corev1.ProtocolTCP,
					Port:       443,
					TargetPort: intstr.IntOrString{Type: intstr.Int, IntVal: 443},
				},
			}
			if !equality.Semantic.DeepEqual(service.Spec.Ports, ports) {
				service.Spec.Ports = ports
			}
			return nil
		})
		if err != nil {
			return r.handleReconcileError(ctx, req, tenant, err, cdnv1.ReasonServiceFailed, "Failed to reconcile Service")
		}
		if opres != controllerutil.OperationResultNone {
			hasUpdates = true
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", fmt.Sprintf("Service %s", opres))
		}
		log.V(1).Info("Reconciled Service", "opres", opres)
		// Check if deployment is ready before proceeding
		if err := r.Get(ctx, types.NamespacedName{Name: "tenant", Namespace: tenant.Name}, deployment); err != nil {
			return r.handleReconcileError(ctx, req, tenant, err, cdnv1.ReasonDeploymentFailed, "Failed to get Deployment status")
		}
		if !isDeploymentReady(deployment) {
			message := fmt.Sprintf("Waiting for deployment to be ready (ready: %d, desired: %d)",
				deployment.Status.ReadyReplicas, *deployment.Spec.Replicas)
			log.V(1).Info(message)
			if err := r.setDeploymentNotReadyConditions(ctx, req, tenant, message); err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", message)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		log.V(1).Info("Deployment is ready")
		requeueAfter, err := r.syncSecondaries(req, ctx, tenant)
		if err != nil {
			return r.handleReconcileError(ctx, req, tenant, err, cdnv1.ReasonSyncFailed, "Failed to sync updates to secondaries")
		}
		if requeueAfter > 0 {
			return r.handleRequeueWithCondition(ctx, req, tenant, "Failed to sync secondaries, will retry", requeueAfter)
		}
		log.V(1).Info("Reconciled Secondaries")
		if !allDomainTLSReady(domainTLSStatuses) {
			message := "Waiting for one or more domain TLS certificates to become ready"
			if err := r.setDomainTLSPendingConditions(ctx, req, tenant, message); err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", message)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		// Always set success conditions when reconciliation completes successfully
		// Check if we need to update (either hasUpdates or Ready condition is not True)
		if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
			return ctrl.Result{}, err
		}
		readyCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeReady)
		needsConditionUpdate := hasUpdates || readyCondition == nil || readyCondition.Status != metav1.ConditionTrue
		log.V(1).Info(
			"Reconciliation complete",
			"needsConditionUpdate", needsConditionUpdate,
			"hasUpdates", hasUpdates,
			"readyConditionStatus", func() string {
				if readyCondition == nil {
					return "nil"
				} else {
					return string(readyCondition.Status)
				}
			},
		)
		if needsConditionUpdate {
			if err := r.setSuccessConditions(ctx, req, tenant, true); err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", "Reconciliation successful")
		}
	} else {
		log.V(1).Info("Tenant is being deleted, cleaning up resources")
		if err := r.setDeletingConditions(ctx, req, tenant, "Deleting tenant resources"); err != nil {
			log.Error(err, "Failed to set deleting conditions")
		}
		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: tenant.Name,
			},
		}
		err := r.Delete(ctx, namespace)
		if err != nil && !apierrors.IsNotFound(err) {
			return r.handleReconcileError(ctx, req, tenant, err, cdnv1.ReasonNamespaceFailed, "Failed to delete Namespace")
		}
		log.V(1).Info("Namespace deleted")
		requeueAfter, err := r.syncSecondaries(req, ctx, tenant)
		if err != nil {
			return r.handleReconcileError(ctx, req, tenant, err, cdnv1.ReasonSyncFailed, "Failed to sync delete to secondaries")
		}
		if requeueAfter > 0 {
			return r.handleRequeueWithCondition(ctx, req, tenant, "Failed to sync secondaries, will retry", requeueAfter)
		}
		log.V(1).Info("Reconciled Secondaries for deletion")
		if controllerutil.ContainsFinalizer(tenant, tenantFinalizer) {
			controllerutil.RemoveFinalizer(tenant, tenantFinalizer)
			if err := r.Update(ctx, tenant); err != nil {
				log.Error(err, "Failed to remove finalizer from tenant")
				return ctrl.Result{}, err
			}
			log.V(1).Info("Finalizer removed")
		}
		r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", "Tenant deleted")
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CdnTenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("tenant-controller")
	h := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			ta := obj.GetAnnotations()[tenantAnnotationKey]
			if ta == "" {
				return nil
			}
			sp := strings.Split(ta, "/")
			if len(sp) != 2 {
				return nil
			}
			return []reconcile.Request{{
				NamespacedName: types.NamespacedName{
					Namespace: sp[0],
					Name:      sp[1],
				},
			}}
		},
	)
	configH := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			if obj.GetAnnotations()[configAnnotationKey] != configAnnotationValue {
				return nil
			}
			if obj.GetName() != configSecretName {
				return nil
			}
			var list cdnv1.CdnTenantList
			if err := r.List(ctx, &list); err != nil {
				return nil
			}
			var reqs []reconcile.Request
			for _, t := range list.Items {
				if obj.GetNamespace() == t.Namespace {
					reqs = append(reqs, reconcile.Request{
						NamespacedName: types.NamespacedName{
							Namespace: t.Namespace,
							Name:      t.Name,
						},
					})
				}
			}
			return reqs
		},
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&cdnv1.CdnTenant{}).
		Watches(&appsv1.Deployment{}, h).
		Watches(&corev1.Service{}, h).
		Watches(&corev1.Secret{}, h).
		Watches(&corev1.Secret{}, configH).
		Complete(r)
}

func (r *CdnTenantReconciler) syncSecondaries(req ctrl.Request, ctx context.Context, tenant *cdnv1.CdnTenant) (time.Duration, error) {
	var log = logf.FromContext(ctx)
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: configSecretName, Namespace: tenant.Namespace}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("Config secret not found")
			return 0, nil
		} else {
			log.V(1).Info("Failed to get config secret")
			return 0, err
		}
	} else {
		primaryKeyBytes, ok := secret.Data["primaryKey"]
		if !ok {
			primaryKeyBytes = []byte("")
		}
		primaryKey := string(primaryKeyBytes)
		secondariesJSON, ok := secret.Data["secondaries.json"]
		if !ok {
			log.V(1).Info("Failed to get secondaries config")
			return 0, nil
		} else {
			var secondaries map[string]map[string]string
			if err = json.Unmarshal(secondariesJSON, &secondaries); err != nil {
				return 0, err
			}
			updateTenantAnnotations := make(map[string]string)
			requeueAfter := time.Duration(0)
			tenantHash, err := r.getTenantHash(tenant)
			if err != nil {
				return 0, err
			}
			for name, secondaryConfig := range secondaries {
				syncStatus, ok := tenant.Annotations[fmt.Sprintf("%s-%s-status", secondariesAnnotationKey, name)]
				if !ok {
					syncStatus = ""
				}
				syncedHash, ok := tenant.Annotations[fmt.Sprintf("%s-%s-hash", secondariesAnnotationKey, name)]
				if !ok {
					syncedHash = ""
				}
				log.V(1).Info(
					fmt.Sprintf("Checking secondary sync status (%s)", name),
					"secondary", name,
					"syncStatus", syncStatus,
					"syncedHash", syncedHash,
					"tenantHash", tenantHash,
				)
				if syncStatus == "" || syncStatus == "syncing" || syncedHash != tenantHash {
					log.Info(
						fmt.Sprintf("Syncing secondary (%s)", name),
						"syncStatus", syncStatus,
						"syncedHash", syncedHash,
						"tenantHash", tenantHash,
					)
					if syncStatus != "syncing" {
						updateTenantAnnotations[fmt.Sprintf("%s-%s-status", secondariesAnnotationKey, name)] = "syncing"
					}
					if syncedHash != "" {
						updateTenantAnnotations[fmt.Sprintf("%s-%s-hash", secondariesAnnotationKey, name)] = ""
					}
					if len(updateTenantAnnotations) > 0 {
						if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
							log.Error(err, "Failed to re-fetch tenant")
							return 0, err
						}
						for k, v := range updateTenantAnnotations {
							tenant.Annotations[k] = v
						}
						if err = r.Update(ctx, tenant); err != nil {
							return 0, err
						}
					}
					updateTenantAnnotations = make(map[string]string)
					requeue, err := r.syncSecondary(ctx, tenant, name, secondaryConfig, primaryKey)
					if err != nil {
						log.V(1).Info(fmt.Sprintf("Failed to sync secondary (%s)", name))
						return 0, err
					}
					if requeue {
						retryNum, ok := tenant.Annotations[fmt.Sprintf("%s-%s-retry", secondariesAnnotationKey, name)]
						if !ok {
							retryNum = "0"
						}
						retryNumInt, err := strconv.Atoi(retryNum)
						if err != nil {
							retryNumInt = 0
						}
						log.V(1).Info(fmt.Sprintf("syncSecondary requeue (%s)", name), "retryNum", retryNumInt)
						if retryNumInt < 10 {
							retryNumInt += 1
							updateTenantAnnotations[fmt.Sprintf("%s-%s-retry", secondariesAnnotationKey, name)] = fmt.Sprintf("%d", retryNumInt)
						}
						newRequeueAfter := time.Duration(retryNumInt*retryNumInt*2) * time.Second
						if newRequeueAfter > requeueAfter {
							requeueAfter = newRequeueAfter
						}
					} else {
						log.V(1).Info(fmt.Sprintf("Secondary synced successfully (%s)", name))
						updateTenantAnnotations[fmt.Sprintf("%s-%s-status", secondariesAnnotationKey, name)] = "synced"
						updateTenantAnnotations[fmt.Sprintf("%s-%s-hash", secondariesAnnotationKey, name)] = tenantHash
					}
				}
			}
			if len(updateTenantAnnotations) > 0 {
				if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
					logf.FromContext(ctx).Error(err, "Failed to re-fetch tenant")
					return 0, err
				}
				for k, v := range updateTenantAnnotations {
					tenant.Annotations[k] = v
				}
				if err = r.Update(ctx, tenant); err != nil {
					return 0, err
				}
			}
			return requeueAfter, nil
		}
	}
}

func (r *CdnTenantReconciler) syncSecondary(ctx context.Context, tenant *cdnv1.CdnTenant, name string, config map[string]string, primaryKey string) (bool, error) {
	action := "delete"
	var body io.Reader
	if tenant.DeletionTimestamp.IsZero() {
		action = "apply"
		m := make(map[string]interface{})
		b, err := json.Marshal(tenant.Spec)
		if err != nil {
			return false, err
		}
		if err := json.Unmarshal(b, &m); err != nil {
			return false, err
		}
		m["primaryKey"] = primaryKey
		b, err = json.Marshal(m)
		if err != nil {
			return false, err
		}
		body = bytes.NewReader(b)
	}
	url := fmt.Sprintf("%s/%s?cdn_tenant_name=%s", config["url"], action, tenant.Name)
	if action == "delete" {
		url += "&primary_key=" + primaryKey
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		body,
	)
	if err != nil {
		return false, err
	}
	req.SetBasicAuth(config["user"], config["pass"])
	if action == "apply" {
		req.Header.Set("Content-Type", "application/json")
	}
	c := &http.Client{Timeout: 10 * time.Second}
	//dump, _ := httputil.DumpRequestOut(req, true)
	//log.Println(string(dump))
	resp, err := c.Do(req)
	if err != nil {
		logf.FromContext(ctx).Error(
			err,
			fmt.Sprintf("Failed to call %s for secondary, will retry", action),
			"secondary", name, "tenant", tenant.Name,
		)
		return true, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slurp, _ := io.ReadAll(resp.Body)
		logf.FromContext(ctx).Error(
			fmt.Errorf("Unexpected %s status: %s\n%s", action, resp.Status, string(slurp)),
			fmt.Sprintf("Failed to call %s for secondary, will retry", action),
			"secondary", name, "tenant", tenant.Name,
		)
		return true, nil
	}
	return false, nil
}

func (r *CdnTenantReconciler) getTenantHash(tenant *cdnv1.CdnTenant) (string, error) {
	if tenant.DeletionTimestamp.IsZero() {
		b, err := json.Marshal(tenant.Spec)
		if err != nil {
			return "", err
		}
		h := sha256.Sum256(b)
		return hex.EncodeToString(h[:]), nil
	} else {
		return "deleted", nil
	}
}
