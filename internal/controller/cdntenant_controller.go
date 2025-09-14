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
	"context"
	"fmt"
	cdnv1 "github.com/CloudWebManage/cwm-cdn-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
)

const (
	typeAvailableTenant = "Available"
	tenantFinalizer     = "cdn.cloudwm-cdn.com/finalizer"
	tenantAnnotationKey = "cdn.cloudwm-cdn.com/tenant"
)

// CdnTenantReconciler reconciles a CdnTenant object
type CdnTenantReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

func (r *CdnTenantReconciler) setReconcilingCondition(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, message string) error {
	meta.SetStatusCondition(&tenant.Status.Conditions, metav1.Condition{
		Type:   typeAvailableTenant,
		Status: metav1.ConditionFalse, Reason: "Reconciling",
		Message: message,
	})
	if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to re-fetch tenant")
		return err
	}
	err := r.Status().Update(ctx, tenant)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to update Tenant status")
		return err
	}
	return nil
}

func (r *CdnTenantReconciler) setReconcilingConditionError(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, err error, message string) (ctrl.Result, error) {
	logf.FromContext(ctx).Error(err, message)
	if e := r.setReconcilingCondition(ctx, req, tenant, message); e != nil {
		return ctrl.Result{}, e
	} else {
		return ctrl.Result{}, err
	}
}

// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

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
			hasUpdates = true
			if err = r.setReconcilingCondition(ctx, req, tenant, "Starting reconciliation"); err != nil {
				return ctrl.Result{}, err
			}
		}
		if !controllerutil.ContainsFinalizer(tenant, tenantFinalizer) {
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
			return r.setReconcilingConditionError(ctx, req, tenant, err, "Failed to reconcile Namespace")
		}
		if opres != controllerutil.OperationResultNone {
			hasUpdates = true
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", fmt.Sprintf("Namespace %s", opres))
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
			deployment.Spec.Replicas = ptr.To(int32(1))
			ps := &deployment.Spec.Template.Spec
			if len(ps.Containers) == 0 {
				ps.Containers = []corev1.Container{
					{
						Name: "nginx",
					},
				}
			}
			c := &ps.Containers[0]
			c.Image = "ghcr.io/cloudwebmanage/cwm-cdn-api-tenant-nginx:latest"
			ports := []corev1.ContainerPort{
				{
					ContainerPort: 443,
				},
			}
			if !equality.Semantic.DeepEqual(deployment.Spec.Template.Spec.Containers[0].Ports, c.Ports) {
				c.Ports = ports
			}
			env := []corev1.EnvVar{
				{
					Name:  "CERT",
					Value: tenant.Spec.Domains[0].Cert,
				},
				{
					Name:  "KEY",
					Value: tenant.Spec.Domains[0].Key,
				},
				{
					Name:  "ORIGIN_URL",
					Value: tenant.Spec.Origins[0].Url,
				},
			}
			if !equality.Semantic.DeepEqual(deployment.Spec.Template.Spec.Containers[0].Env, env) {
				c.Env = env
			}
			return nil
		})
		if err != nil {
			return r.setReconcilingConditionError(ctx, req, tenant, err, "Failed to reconcile Deployment")
		}
		if opres != controllerutil.OperationResultNone {
			hasUpdates = true
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", fmt.Sprintf("Deployment %s", opres))
		}
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
			return r.setReconcilingConditionError(ctx, req, tenant, err, "Failed to reconcile Service")
		}
		if opres != controllerutil.OperationResultNone {
			hasUpdates = true
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", fmt.Sprintf("Service %s", opres))
		}
		if hasUpdates {
			meta.SetStatusCondition(&tenant.Status.Conditions, metav1.Condition{
				Type:    typeAvailableTenant,
				Status:  metav1.ConditionTrue,
				Reason:  "Reconciling",
				Message: fmt.Sprintf("tenant reconciled successfully"),
			})
			if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
				logf.FromContext(ctx).Error(err, "Failed to re-fetch tenant")
				return ctrl.Result{}, err
			}
			if err := r.Status().Update(ctx, tenant); err != nil {
				log.Error(err, "Failed to update Tenant status")
				return ctrl.Result{}, err
			}
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", "Reconciliation successful")
		}
	} else {
		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: tenant.Name,
			},
		}
		err := r.Delete(ctx, namespace)
		if err != nil && !apierrors.IsNotFound(err) {
			return r.setReconcilingConditionError(ctx, req, tenant, err, "Failed to delete Namespace")
		}
		if controllerutil.ContainsFinalizer(tenant, tenantFinalizer) {
			controllerutil.RemoveFinalizer(tenant, tenantFinalizer)
			if err := r.Update(ctx, tenant); err != nil {
				log.Error(err, "Failed to remove finalizer from tenant")
				return ctrl.Result{}, err
			}
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&cdnv1.CdnTenant{}).
		Watches(&appsv1.Deployment{}, h).
		Watches(&corev1.Service{}, h).
		Complete(r)
}
