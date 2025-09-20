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
	"fmt"
	cdnv1 "github.com/CloudWebManage/cwm-cdn-operator/api/v1"
	"io"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"net/http"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
	"time"
)

const (
	typeAvailableTenant      = "Available"
	tenantFinalizer          = "cdn.cloudwm-cdn.com/finalizer"
	tenantAnnotationKey      = "cdn.cloudwm-cdn.com/tenant"
	configAnnotationKey      = "cdn.cloudwm-cdn.com/config"
	configAnnotationValue    = "true"
	configSecretName         = "cwm-cdn-tenants-config"
	secondariesAnnotationKey = "cdn.cloudwm-cdn.com/secondaries-"
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

func (r *CdnTenantReconciler) setReconcilingConditionRequeue(ctx context.Context, req ctrl.Request, tenant *cdnv1.CdnTenant, message string, requeueAfter time.Duration) (ctrl.Result, error) {
	logf.FromContext(ctx).Info(message)
	if e := r.setReconcilingCondition(ctx, req, tenant, message); e != nil {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	} else {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
}

// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cdn.cloudwm-cdn.com,resources=cdntenants/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

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
				{
					Name:  "TENANT_NAME",
					Value: tenant.Name,
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
			return r.setReconcilingConditionError(ctx, req, tenant, err, "Failed to reconcile Service")
		}
		if opres != controllerutil.OperationResultNone {
			hasUpdates = true
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Reconciling", fmt.Sprintf("Service %s", opres))
		}
		hasUpdates, requeueAfter, err := r.syncSecondaries(req, ctx, tenant, hasUpdates)
		if err != nil {
			return r.setReconcilingConditionError(ctx, req, tenant, err, "Failed to sync updates to secondaries")
		}
		if requeueAfter > 0 {
			return r.setReconcilingConditionRequeue(ctx, req, tenant, "Failed to sync secondaries, will retry", requeueAfter)
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
		_, requeueAfter, err := r.syncSecondaries(req, ctx, tenant, true)
		if err != nil {
			return r.setReconcilingConditionError(ctx, req, tenant, err, "Failed to sync delete to secondaries")
		}
		if requeueAfter > 0 {
			return r.setReconcilingConditionRequeue(ctx, req, tenant, "Failed to sync secondaries, will retry", requeueAfter)
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
		Watches(&corev1.Secret{}, configH).
		Complete(r)
}

func (r *CdnTenantReconciler) syncSecondaries(req ctrl.Request, ctx context.Context, tenant *cdnv1.CdnTenant, hasUpdates bool) (bool, time.Duration, error) {
	var secret corev1.Secret
	forceSync := hasUpdates
	if err := r.Get(ctx, types.NamespacedName{Name: configSecretName, Namespace: tenant.Namespace}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return hasUpdates, 0, nil
		} else {
			return hasUpdates, 0, err
		}
	} else {
		primaryKeyBytes, ok := secret.Data["primaryKey"]
		if !ok {
			primaryKeyBytes = []byte("")
		}
		primaryKey := string(primaryKeyBytes)
		secondariesJSON, ok := secret.Data["secondaries.json"]
		if !ok {
			return hasUpdates, 0, nil
		} else {
			var secondaries map[string]map[string]string
			if err = json.Unmarshal(secondariesJSON, &secondaries); err != nil {
				return hasUpdates, 0, err
			}
			updateTenantAnnotations := false
			requeueAfter := time.Duration(0)
			for name, secondaryConfig := range secondaries {
				syncStatus, ok := tenant.Annotations[fmt.Sprintf("%s-%s-status", secondariesAnnotationKey, name)]
				if !ok {
					syncStatus = ""
				}
				if (forceSync && syncStatus != "synced") || (!forceSync && syncStatus == "syncing") {
					logf.FromContext(ctx).Info(
						"Syncing secondary",
						"secondary", name,
						"tenant", tenant.Name,
						"syncStatus", syncStatus,
						"forceSync", forceSync,
					)
					hasUpdates = true
					if syncStatus == "" {
						tenant.Annotations[fmt.Sprintf("%s-%s-status", secondariesAnnotationKey, name)] = "syncing"
						updateTenantAnnotations = true
					}
					requeue, err := r.syncSecondary(ctx, tenant, name, secondaryConfig, primaryKey)
					if err != nil {
						return hasUpdates, 0, err
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
						if retryNumInt < 10 {
							retryNumInt += 1
							updateTenantAnnotations = true
							tenant.Annotations[fmt.Sprintf("%s-%s-retry", secondariesAnnotationKey, name)] = fmt.Sprintf("%d", retryNumInt)
						}
						newRequeueAfter := time.Duration(retryNumInt*retryNumInt*2) * time.Second
						if newRequeueAfter > requeueAfter {
							requeueAfter = newRequeueAfter
						}
					} else {
						tenant.Annotations[fmt.Sprintf("%s-%s-status", secondariesAnnotationKey, name)] = "synced"
						updateTenantAnnotations = true
					}
				}
			}
			if updateTenantAnnotations {
				if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
					logf.FromContext(ctx).Error(err, "Failed to re-fetch tenant")
					return hasUpdates, 0, err
				}
				if err = r.Update(ctx, tenant); err != nil {
					return hasUpdates, 0, err
				}
			}
			if requeueAfter > 0 {
				return hasUpdates, requeueAfter, nil
			} else {
				for name, _ := range secondaries {
					tenant.Annotations[fmt.Sprintf("%s-%s-status", secondariesAnnotationKey, name)] = ""
				}
				if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
					logf.FromContext(ctx).Error(err, "Failed to re-fetch tenant")
					return hasUpdates, 0, err
				}
				if err = r.Update(ctx, tenant); err != nil {
					return hasUpdates, 0, err
				}
				return hasUpdates, 0, nil
			}
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
