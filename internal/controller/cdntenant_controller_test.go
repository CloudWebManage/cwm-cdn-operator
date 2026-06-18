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
	stdjson "encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cdnv1 "github.com/CloudWebManage/cwm-cdn-operator/api/v1"
)

type noMatchDeleteClient struct {
	client.Client
}

func (c noMatchDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return &meta.NoKindMatchError{GroupKind: scaledObjectGVK.GroupKind(), SearchedVersions: []string{scaledObjectGVK.Version}}
}

type beforeStatusUpdateClient struct {
	client.Client
	beforeStatusUpdate func(context.Context, client.Object) error
}

func (c beforeStatusUpdateClient) Status() client.SubResourceWriter {
	return beforeStatusUpdateWriter{
		SubResourceWriter:  c.Client.Status(),
		beforeStatusUpdate: c.beforeStatusUpdate,
	}
}

type beforeStatusUpdateWriter struct {
	client.SubResourceWriter
	beforeStatusUpdate func(context.Context, client.Object) error
}

func (w beforeStatusUpdateWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if w.beforeStatusUpdate != nil {
		if err := w.beforeStatusUpdate(ctx, obj); err != nil {
			return err
		}
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func updateTenantAnnotationOnce(baseClient client.Client, annotationValue string) func(context.Context, client.Object) error {
	updated := false
	return func(ctx context.Context, obj client.Object) error {
		if updated {
			return nil
		}
		if _, ok := obj.(*cdnv1.CdnTenant); !ok {
			return nil
		}
		updated = true
		latest := &cdnv1.CdnTenant{}
		if err := baseClient.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, latest); err != nil {
			return err
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations["cdn.cloudwm-cdn.com/status-conflict-test"] = annotationValue
		return baseClient.Update(ctx, latest)
	}
}

var _ = Describe("CdnTenant Controller", func() {
	Context("When deriving domain TLS configuration", func() {
		It("should default existing domains to provided TLS without redirect", func() {
			domain := cdnv1.Domain{Name: "test.example.com", Cert: "cert", Key: "key"}
			Expect(domainTLSMode(domain)).To(Equal("provided"))
			Expect(domainTLSMinVersion(domain)).To(Equal("TLSv1.2"))
			Expect(domainTLSMaxVersion(domain)).To(Equal("TLSv1.3"))
			Expect(domainRedirectHTTPToHTTPS(domain)).To(BeFalse())
		})

		It("should use stable internal certificate resource names for letsencrypt domains", func() {
			name := domainCertificateResourceName(0, "customer.example.com")
			Expect(name).To(Equal(domainCertificateSecretName(0, "customer.example.com")))
			Expect(name).To(HavePrefix("tenant-domain-0-"))
			Expect(len(name)).To(BeNumerically("<=", 63))
		})

		It("should generate usable placeholder TLS secret data", func() {
			data, err := generatePlaceholderTLSSecretData("customer.example.com")
			Expect(err).NotTo(HaveOccurred())
			Expect(data).To(HaveKey(corev1.TLSCertKey))
			Expect(data).To(HaveKey(corev1.TLSPrivateKeyKey))
			Expect(string(data[corev1.TLSCertKey])).To(ContainSubstring("BEGIN CERTIFICATE"))
			Expect(string(data[corev1.TLSPrivateKeyKey])).To(ContainSubstring("BEGIN RSA PRIVATE KEY"))
		})

		It("should use cert-manager Ready condition for letsencrypt readiness", func() {
			certificate := &unstructured.Unstructured{Object: map[string]interface{}{
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{"type": "Ready", "status": "False"},
					},
				},
			}}
			Expect(certificateReady(certificate)).To(BeFalse())
			certificate.Object["status"].(map[string]interface{})["conditions"] = []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			}
			Expect(certificateReady(certificate)).To(BeTrue())
		})

		It("should not issue letsencrypt certificates on secondary clusters", func() {
			oldValue := os.Getenv(isPrimaryEnvVar)
			defer os.Setenv(isPrimaryEnvVar, oldValue)
			Expect(os.Setenv(isPrimaryEnvVar, "false")).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}
			statuses, err := controllerReconciler.reconcileDomainTLSResources(context.Background(), &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: "tenant-secondary", Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{
						Name: "customer.example.com",
						TLS:  &cdnv1.DomainTLS{Mode: "letsencrypt"},
					}},
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(statuses).To(HaveLen(1))
			Expect(statuses[0].Ready).To(BeFalse())
			Expect(statuses[0].Reason).To(Equal("LetsEncryptUnsupportedOnSecondary"))
		})
	})

	Context("When reconciling a resource", func() {
		const resourceName = "tenant1"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		cdntenant := &cdnv1.CdnTenant{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind CdnTenant")
			err := k8sClient.Get(ctx, typeNamespacedName, cdntenant)
			if err != nil && errors.IsNotFound(err) {
				resource := &cdnv1.CdnTenant{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: cdnv1.CdnTenantSpec{
						Domains: []cdnv1.Domain{
							{
								Name: "test.example.com",
								Cert: "aaa",
								Key:  "bbb",
								Config: map[string]string{
									"example": "value",
									"FOO":     "BAR",
								},
							},
						},
						Origins: []cdnv1.Origin{
							{
								Url: "http://example.com",
								Config: map[string]string{
									"hello": "world",
									"TEST":  "123",
								},
							},
						},
						Config: map[string]string{
							"key":         "value",
							"ANOTHER_KEY": "ANOTHER_VALUE",
							"IMAGE":       "test",
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &cdnv1.CdnTenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance CdnTenant")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deployKey := types.NamespacedName{
				Namespace: "tenant1",
				Name:      "tenant",
			}
			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, deployKey, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Annotations["cdn.cloudwm-cdn.com/tenant"]).To(Equal("default/tenant1"))
			Expect(deploy.Spec.Replicas).ToNot(BeNil())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(1)))
			Expect(deploy.Spec.Template.Spec.Tolerations).To(Equal([]corev1.Toleration{{Key: "cwm-iac-worker-role", Operator: corev1.TolerationOpEqual, Value: "cdn", Effect: corev1.TaintEffectNoExecute}}))
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].Name).To(Equal("nginx"))
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("test"))
			Expect(deploy.Spec.Template.Spec.Containers[0].Ports).To(ConsistOf([]corev1.ContainerPort{
				{ContainerPort: 80, Protocol: corev1.ProtocolTCP},
				{ContainerPort: 443, Protocol: corev1.ProtocolTCP},
			}))
			Expect(deploy.Spec.Template.Spec.Containers[0].Env).To(ConsistOf([]corev1.EnvVar{
				{Name: "TENANT_NAME", Value: "tenant1"},
				{Name: "CACHE_ENABLED", Value: "true"},
				{Name: "CACHE_MODE", Value: "cache_everything"},
				{Name: "CACHE_EDGE_TTL_SECONDS", Value: "3600"},
				{Name: "CACHE_RESPECT_ORIGIN_CACHE_CONTROL", Value: "true"},
				{Name: "CACHE_STATUS_HEADER", Value: "X-CWM-Cache-Status"},
				{Name: "CWM_CDN_POLICY_PATH", Value: "/etc/cwm-cdn/policy.json"},
				{Name: "D0_NAME", Value: "test.example.com"},
				{Name: "D0_KEY", Value: "bbb"},
				{Name: "D0_CERT", Value: "aaa"},
				{Name: "D0_EXAMPLE", Value: "value"},
				{Name: "D0_FOO", Value: "BAR"},
				{Name: "O0_URL", Value: "http://example.com"},
				{Name: "O0_NAME", Value: "origin-0"},
				{Name: "O0_WEIGHT", Value: "1"},
				{Name: "O0_HELLO", Value: "world"},
				{Name: "O0_TEST", Value: "123"},
				{Name: "KEY", Value: "value"},
				{Name: "ANOTHER_KEY", Value: "ANOTHER_VALUE"},
			}))
		})
	})

	Context("When checking status conditions - deployment not ready", func() {
		const resourceName = "tenant-notready"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource for conditions testing")
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{
						{
							Name: "notready.example.com",
							Cert: "cert",
							Key:  "key",
						},
					},
					Origins: []cdnv1.Origin{
						{
							Url: "http://origin.example.com",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &cdnv1.CdnTenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Removing finalizer to allow deletion")
				resource.Finalizers = nil
				_ = k8sClient.Update(ctx, resource)
				By("Cleanup the specific resource instance CdnTenant")
				_ = k8sClient.Delete(ctx, resource)
			}
		})

		It("should set DeploymentNotReady condition when deployment is not ready", func() {
			By("Reconciling the resource for the first time")
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0), "Should requeue when deployment is not ready")

			By("Checking the tenant status conditions")
			tenant := &cdnv1.CdnTenant{}
			err = k8sClient.Get(ctx, typeNamespacedName, tenant)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Ready condition is False with DeploymentNotReady reason")
			readyCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal(cdnv1.ReasonDeploymentNotReady))

			By("Verifying Progressing condition is True with DeploymentNotReady reason")
			progressingCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeProgressing)
			Expect(progressingCondition).NotTo(BeNil())
			Expect(progressingCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(progressingCondition.Reason).To(Equal(cdnv1.ReasonDeploymentNotReady))

			By("Verifying Degraded condition is False")
			degradedCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeDegraded)
			Expect(degradedCondition).NotTo(BeNil())
			Expect(degradedCondition.Status).To(Equal(metav1.ConditionFalse))

			By("Verifying domain TLS status is surfaced without internal resource names")
			Expect(tenant.Status.DomainTLS).To(ConsistOf(cdnv1.DomainTLSStatus{
				Name:    "notready.example.com",
				Mode:    "provided",
				Ready:   true,
				Reason:  "ProvidedCertificateConfigured",
				Message: "Provided certificate and key are configured",
			}))
		})
	})

	Context("When checking status conditions - deployment becomes ready", func() {
		const resourceName = "tenant-ready"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource for conditions testing")
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{
						{
							Name: "ready.example.com",
							Cert: "cert",
							Key:  "key",
						},
					},
					Origins: []cdnv1.Origin{
						{
							Url: "http://origin.example.com",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &cdnv1.CdnTenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Removing finalizer to allow deletion")
				resource.Finalizers = nil
				_ = k8sClient.Update(ctx, resource)
				By("Cleanup the specific resource instance CdnTenant")
				_ = k8sClient.Delete(ctx, resource)
			}
		})

		It("should set Ready condition to True when deployment becomes ready", func() {
			By("Reconciling the resource to create deployment")
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Simulating deployment becoming ready")
			deployKey := types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}
			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, deployKey, deploy)
			Expect(err).NotTo(HaveOccurred())

			// Update deployment status to simulate ready state
			deploy.Status.Replicas = 1
			deploy.Status.ReadyReplicas = 1
			deploy.Status.AvailableReplicas = 1
			err = k8sClient.Status().Update(ctx, deploy)
			Expect(err).NotTo(HaveOccurred())

			By("Reconciling again after deployment is ready")
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero(), "Should not requeue when deployment is ready")

			By("Checking the tenant status conditions after deployment is ready")
			tenant := &cdnv1.CdnTenant{}
			err = k8sClient.Get(ctx, typeNamespacedName, tenant)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Ready condition is True")
			readyCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition.Reason).To(Equal(cdnv1.ReasonAllResourcesReady))

			By("Verifying Progressing condition is False")
			progressingCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeProgressing)
			Expect(progressingCondition).NotTo(BeNil())
			Expect(progressingCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(progressingCondition.Reason).To(Equal(cdnv1.ReasonReconcileSuccess))

			By("Verifying Degraded condition is False")
			degradedCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeDegraded)
			Expect(degradedCondition).NotTo(BeNil())
			Expect(degradedCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(degradedCondition.Reason).To(Equal(cdnv1.ReasonReconcileSuccess))

			By("Verifying SecondariesSynced condition is True")
			secondariesCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeSecondariesSynced)
			Expect(secondariesCondition).NotTo(BeNil())
			Expect(secondariesCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(secondariesCondition.Reason).To(Equal(cdnv1.ReasonSyncSuccess))
		})
	})

	Context("When checking condition transitions", func() {
		const resourceName = "tenant-transitions"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource for conditions testing")
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{
						{
							Name: "transitions.example.com",
							Cert: "cert",
							Key:  "key",
						},
					},
					Origins: []cdnv1.Origin{
						{
							Url: "http://origin.example.com",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &cdnv1.CdnTenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Removing finalizer to allow deletion")
				resource.Finalizers = nil
				_ = k8sClient.Update(ctx, resource)
				By("Cleanup the specific resource instance CdnTenant")
				_ = k8sClient.Delete(ctx, resource)
			}
		})

		It("should track condition transitions from not ready to ready", func() {
			By("Initial reconciliation - deployment not ready")
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))

			// Verify initial not ready state
			tenant := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())
			readyCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal(cdnv1.ReasonDeploymentNotReady))

			By("Make deployment ready")
			deployKey := types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, deployKey, deploy)).To(Succeed())
			deploy.Status.Replicas = 1
			deploy.Status.ReadyReplicas = 1
			deploy.Status.AvailableReplicas = 1
			Expect(k8sClient.Status().Update(ctx, deploy)).To(Succeed())

			By("Reconcile again - deployment now ready")
			result, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			// Verify ready state - status should transition from False to True
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())
			readyCondition = meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue), "Ready status should transition to True")
			Expect(readyCondition.Reason).To(Equal(cdnv1.ReasonAllResourcesReady), "Ready reason should change to AllResourcesReady")

			// Verify all conditions are in expected final state
			progressingCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeProgressing)
			Expect(progressingCondition).NotTo(BeNil())
			Expect(progressingCondition.Status).To(Equal(metav1.ConditionFalse))

			degradedCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeDegraded)
			Expect(degradedCondition).NotTo(BeNil())
			Expect(degradedCondition.Status).To(Equal(metav1.ConditionFalse))
		})
	})

	Context("When testing condition helper functions", func() {
		It("should correctly determine deployment readiness", func() {
			By("Testing deployment with no replicas specified")
			deploy := &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: nil,
				},
				Status: appsv1.DeploymentStatus{
					ReadyReplicas: 0,
				},
			}
			Expect(isDeploymentReady(deploy)).To(BeFalse())

			deploy.Status.ReadyReplicas = 1
			Expect(isDeploymentReady(deploy)).To(BeTrue())

			By("Testing deployment with replicas specified")
			replicas := int32(3)
			deploy.Spec.Replicas = &replicas
			deploy.Status.ReadyReplicas = 2
			Expect(isDeploymentReady(deploy)).To(BeFalse())

			deploy.Status.ReadyReplicas = 3
			Expect(isDeploymentReady(deploy)).To(BeTrue())

			deploy.Status.ReadyReplicas = 4
			Expect(isDeploymentReady(deploy)).To(BeTrue())
		})
	})

	Context("When status updates race with tenant metadata updates", func() {
		ctx := context.Background()

		AfterEach(func() {
			tenantList := &cdnv1.CdnTenantList{}
			_ = k8sClient.List(ctx, tenantList)
			for _, t := range tenantList.Items {
				t.Finalizers = nil
				_ = k8sClient.Update(ctx, &t)
				_ = k8sClient.Delete(ctx, &t)
			}
		})

		It("should retry a conflict during success condition updates", func() {
			const resourceName = "tenant-condition-conflict"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "condition-conflict.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client: beforeStatusUpdateClient{
					Client:             k8sClient,
					beforeStatusUpdate: updateTenantAnnotationOnce(k8sClient, "success-conditions"),
				},
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}
			tenant := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())

			err := controllerReconciler.setSuccessConditions(ctx, reconcile.Request{NamespacedName: typeNamespacedName}, tenant, true)
			Expect(err).NotTo(HaveOccurred())

			latest := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, latest)).To(Succeed())
			Expect(latest.Annotations).To(HaveKeyWithValue("cdn.cloudwm-cdn.com/status-conflict-test", "success-conditions"))
			readyCondition := meta.FindStatusCondition(latest.Status.Conditions, cdnv1.TypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition.Reason).To(Equal(cdnv1.ReasonAllResourcesReady))
		})
	})

	// ============================================
	// Deletion and Finalizer Tests
	// ============================================
	Context("When deleting a tenant", func() {
		const resourceName = "tenant-delete"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		It("should set deleting conditions and clean up resources", func() {
			By("Creating the tenant resource")
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{
						{Name: "delete.example.com", Cert: "cert", Key: "key"},
					},
					Origins: []cdnv1.Origin{
						{Url: "http://origin.example.com"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			By("Reconciling to create resources and add finalizer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying finalizer was added")
			tenant := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())
			Expect(tenant.Finalizers).To(ContainElement("cdn.cloudwm-cdn.com/finalizer"))

			By("Verifying namespace was created")
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName}, ns)).To(Succeed())

			By("Deleting the tenant")
			Expect(k8sClient.Delete(ctx, tenant)).To(Succeed())

			By("Reconciling after deletion request")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying finalizer was removed")
			err = k8sClient.Get(ctx, typeNamespacedName, tenant)
			Expect(errors.IsNotFound(err)).To(BeTrue(), "Tenant should be deleted after finalizer removal")
		})
	})

	// ============================================
	// Configuration Tests
	// ============================================
	Context("When testing configuration handling", func() {
		ctx := context.Background()

		AfterEach(func() {
			// Cleanup any created resources
			tenantList := &cdnv1.CdnTenantList{}
			_ = k8sClient.List(ctx, tenantList)
			for _, t := range tenantList.Items {
				t.Finalizers = nil
				_ = k8sClient.Update(ctx, &t)
				_ = k8sClient.Delete(ctx, &t)
			}
			_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: configSecretName, Namespace: "default"}})
		})

		It("should use default image when not specified in config", func() {
			const resourceName = "tenant-default-image"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "default.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					// No IMAGE in config - should use default
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/cloudwebmanage/cwm-cdn-api-tenant-nginx:latest"))
		})

		It("should use custom image from config", func() {
			const resourceName = "tenant-custom-image"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "custom.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Config: map[string]string{
						"IMAGE": "my-custom-image:v1.0",
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("my-custom-image:v1.0"))
		})

		It("should use custom replicas from config", func() {
			const resourceName = "tenant-custom-replicas"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "replicas.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Config: map[string]string{
						"REPLICAS": "3",
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(*deploy.Spec.Replicas).To(Equal(int32(3)))
		})

		It("should create a KEDA ScaledObject when autoscaling is enabled", func() {
			const resourceName = "tenant-autoscaling-enabled"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "autoscaling.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Autoscaling: &cdnv1.CdnTenantAutoscalingSpec{
						Enabled:               true,
						MinReplicas:           2,
						MaxReplicas:           7,
						TargetClientRpsPerPod: 50,
						TargetOriginRpsPerPod: 25,
						ScaleDownStabilization: metav1.Duration{
							Duration: 2 * time.Minute,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &record.FakeRecorder{}}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, deploy)).To(Succeed())
			kedaReplicas := int32(6)
			deploy.Spec.Replicas = &kedaReplicas
			Expect(k8sClient.Update(ctx, deploy)).To(Succeed())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, deploy)).To(Succeed())
			Expect(deploy.Spec.Replicas).NotTo(BeNil())
			Expect(*deploy.Spec.Replicas).To(Equal(kedaReplicas))

			scaledObject := &unstructured.Unstructured{}
			scaledObject.SetGroupVersionKind(scaledObjectGVK)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, scaledObject)).To(Succeed())
			minReplicas, found, err := unstructured.NestedInt64(scaledObject.Object, "spec", "minReplicaCount")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(minReplicas).To(Equal(int64(2)))
			maxReplicas, found, err := unstructured.NestedInt64(scaledObject.Object, "spec", "maxReplicaCount")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(maxReplicas).To(Equal(int64(7)))
			triggers, found, err := unstructured.NestedSlice(scaledObject.Object, "spec", "triggers")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(triggers).To(HaveLen(2))
			clientTrigger, ok := triggers[0].(map[string]interface{})
			Expect(ok).To(BeTrue())
			clientMetadata, ok := clientTrigger["metadata"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(clientMetadata).To(HaveKeyWithValue("serverAddress", prometheusServerAddress))
			Expect(clientMetadata).To(HaveKeyWithValue("metricName", "tenant_client_rps"))
			Expect(clientMetadata).To(HaveKeyWithValue("query", `sum(rate(nginx_http_requests_total{job="tenant", namespace="tenant-autoscaling-enabled", host!="_"}[2m]))`))
			Expect(clientMetadata).To(HaveKeyWithValue("threshold", "50"))
			originTrigger, ok := triggers[1].(map[string]interface{})
			Expect(ok).To(BeTrue())
			originMetadata, ok := originTrigger["metadata"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(originMetadata).To(HaveKeyWithValue("metricName", "tenant_origin_rps"))
			Expect(originMetadata).To(HaveKeyWithValue("query", `sum(rate(nginx_http_requests_total{job="tenant", namespace="tenant-autoscaling-enabled", host="_"}[2m]))`))
			Expect(originMetadata).To(HaveKeyWithValue("threshold", "25"))
			stabilizationSeconds, found, err := unstructured.NestedInt64(scaledObject.Object, "spec", "advanced", "horizontalPodAutoscalerConfig", "behavior", "scaleDown", "stabilizationWindowSeconds")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(stabilizationSeconds).To(Equal(int64(120)))

			latest := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, latest)).To(Succeed())
			Expect(latest.Status.Autoscaling).NotTo(BeNil())
			Expect(latest.Status.Autoscaling.Enabled).To(BeTrue())
			Expect(latest.Status.Autoscaling.MinReplicas).To(Equal(int32(2)))
			Expect(latest.Status.Autoscaling.MaxReplicas).To(Equal(int32(7)))
			Expect(latest.Status.Autoscaling.CurrentReplicas).To(Equal(int32(0)))
			Expect(latest.Status.Autoscaling.DesiredReplicas).To(Equal(kedaReplicas))
			Expect(latest.Status.Autoscaling.LastMessage).NotTo(BeEmpty())
		})

		It("should remove ScaledObject and restore fixed replicas when autoscaling is disabled", func() {
			const resourceName = "tenant-autoscaling-disabled"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "autoscaling-disabled.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Config:  map[string]string{"REPLICAS": "4"},
					Autoscaling: &cdnv1.CdnTenantAutoscalingSpec{
						Enabled:     true,
						MinReplicas: 1,
						MaxReplicas: 5,
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &record.FakeRecorder{}}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			scaledObject := &unstructured.Unstructured{}
			scaledObject.SetGroupVersionKind(scaledObjectGVK)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, scaledObject)).To(Succeed())

			latest := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, latest)).To(Succeed())
			latest.Spec.Autoscaling.Enabled = false
			Expect(k8sClient.Update(ctx, latest)).To(Succeed())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, deploy)).To(Succeed())
			Expect(deploy.Spec.Replicas).NotTo(BeNil())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(4)))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, scaledObject)).To(MatchError(ContainSubstring("not found")))

			Expect(k8sClient.Get(ctx, typeNamespacedName, latest)).To(Succeed())
			Expect(latest.Status.Autoscaling).NotTo(BeNil())
			Expect(latest.Status.Autoscaling.Enabled).To(BeFalse())
			Expect(latest.Status.Autoscaling.DesiredReplicas).To(Equal(int32(4)))
			Expect(latest.Status.Autoscaling.LastMessage).To(ContainSubstring("Autoscaling disabled"))
		})

		It("should preserve fixed-replica behavior when KEDA API is absent and autoscaling is disabled", func() {
			const resourceName = "tenant-autoscaling-no-keda-api"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "no-keda-api.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Config:  map[string]string{"REPLICAS": "3"},
					Autoscaling: &cdnv1.CdnTenantAutoscalingSpec{
						Enabled: false,
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   noMatchDeleteClient{Client: k8sClient},
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, deploy)).To(Succeed())
			Expect(deploy.Spec.Replicas).NotTo(BeNil())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(3)))

			latest := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, latest)).To(Succeed())
			Expect(latest.Status.Autoscaling).NotTo(BeNil())
			Expect(latest.Status.Autoscaling.Enabled).To(BeFalse())
			Expect(latest.Status.Autoscaling.DesiredReplicas).To(Equal(int32(3)))
		})

		It("should reject autoscaling minReplicas greater than maxReplicas", func() {
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: "tenant-autoscaling-invalid", Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains:     []cdnv1.Domain{{Name: "invalid-autoscaling.example.com", Cert: "cert", Key: "key"}},
					Origins:     []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Autoscaling: &cdnv1.CdnTenantAutoscalingSpec{Enabled: true, MinReplicas: 5, MaxReplicas: 2},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(MatchError(ContainSubstring("minReplicas must be less than or equal to maxReplicas")))
		})

		It("should pass custom config as environment variables", func() {
			const resourceName = "tenant-custom-env"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "env.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Config: map[string]string{
						"CUSTOM_VAR":  "custom_value",
						"another_var": "another_value",
						"IMAGE":       "test", // Should not be in env vars
						"REPLICAS":    "2",    // Should not be in env vars
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			envVars := deploy.Spec.Template.Spec.Containers[0].Env
			envMap := make(map[string]string)
			for _, env := range envVars {
				envMap[env.Name] = env.Value
			}

			Expect(envMap).To(HaveKeyWithValue("CUSTOM_VAR", "custom_value"))
			Expect(envMap).To(HaveKeyWithValue("ANOTHER_VAR", "another_value"))
			Expect(envMap).NotTo(HaveKey("IMAGE"))
			Expect(envMap).NotTo(HaveKey("REPLICAS"))
		})
	})

	Context("When testing CDN cache and policy configuration", func() {
		ctx := context.Background()

		AfterEach(func() {
			tenantList := &cdnv1.CdnTenantList{}
			_ = k8sClient.List(ctx, tenantList)
			for _, t := range tenantList.Items {
				t.Finalizers = nil
				_ = k8sClient.Update(ctx, &t)
				_ = k8sClient.Delete(ctx, &t)
			}
			_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: configSecretName, Namespace: "default"}})
		})

		It("should calculate effective cache defaults", func() {
			cache, err := effectiveCache(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(cache.Enabled).To(BeTrue())
			Expect(cache.Mode).To(Equal("cache_everything"))
			Expect(cache.EdgeTTLSeconds).To(Equal(int64(3600)))
			Expect(cache.RespectOriginCacheControl).To(BeTrue())
			Expect(cache.StatusHeader).To(Equal("X-CWM-Cache-Status"))
		})

		It("should reject unsafe cache status headers", func() {
			header := "Set-Cookie"
			_, err := effectiveCache(&cdnv1.CacheConfig{StatusHeader: &header})
			Expect(err).To(HaveOccurred())
			header = "X-CWMCDN-Cache-Enabled"
			_, err = effectiveCache(&cdnv1.CacheConfig{StatusHeader: &header})
			Expect(err).To(HaveOccurred())
		})

		It("should reject cache semantics that the cache layer cannot honor yet", func() {
			_, err := effectiveCache(&cdnv1.CacheConfig{EdgeTtl: "5m"})
			Expect(err).To(MatchError(ContainSubstring("dynamic TTL")))
			respectOrigin := false
			_, err = effectiveCache(&cdnv1.CacheConfig{RespectOriginCacheControl: &respectOrigin})
			Expect(err).To(MatchError(ContainSubstring("origin-header override")))
		})

		It("should render typed cache env vars and policy file without loose config override", func() {
			Expect(os.Setenv("CWM_CDN_TENANT_DEFAULT_ENABLE_PLATFORM_LOGS", "true")).To(Succeed())
			Expect(os.Setenv("CWM_CDN_TENANT_DEFAULT_POP_ID", "pop-test")).To(Succeed())
			defer os.Unsetenv("CWM_CDN_TENANT_DEFAULT_ENABLE_PLATFORM_LOGS")
			defer os.Unsetenv("CWM_CDN_TENANT_DEFAULT_POP_ID")
			const resourceName = "tenant-cache-policy"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}
			enabled := false
			header := "X-Cache-Test"
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "cache.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Cache: &cdnv1.CacheConfig{
						Enabled:      &enabled,
						Mode:         "cache_everything",
						StatusHeader: &header,
					},
					Config: map[string]string{
						"CUSTOM_VAR": "custom_value",
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			controllerReconciler := &CdnTenantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &record.FakeRecorder{}}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, deploy)).To(Succeed())
			envMap := map[string]string{}
			for _, env := range deploy.Spec.Template.Spec.Containers[0].Env {
				envMap[env.Name] = env.Value
			}
			Expect(envMap).To(HaveKeyWithValue("CACHE_ENABLED", "false"))
			Expect(envMap).To(HaveKeyWithValue("CACHE_EDGE_TTL_SECONDS", "3600"))
			Expect(envMap).To(HaveKeyWithValue("CACHE_RESPECT_ORIGIN_CACHE_CONTROL", "true"))
			Expect(envMap).To(HaveKeyWithValue("CACHE_STATUS_HEADER", "X-Cache-Test"))
			Expect(envMap).To(HaveKeyWithValue("CWM_CDN_POLICY_PATH", "/etc/cwm-cdn/policy.json"))
			Expect(envMap).NotTo(HaveKey("TENANT_POLICY_JSON"))
			Expect(envMap).NotTo(HaveKey("CWM_CDN_POP_ID"))
			Expect(envMap).To(HaveKeyWithValue("ENABLE_PLATFORM_LOGS", "true"))
			Expect(envMap).To(HaveKeyWithValue("ENABLE_TENANT_ACCESS_LOGS", "true"))
			Expect(envMap).To(HaveKeyWithValue("POP_ID", "pop-test"))
			Expect(envMap).To(HaveKeyWithValue("CUSTOM_VAR", "custom_value"))

			policy := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: policyConfigMapName}, policy)).To(Succeed())
			Expect(policy.Data[policyConfigMapKey]).To(ContainSubstring("\"edgeTtlSeconds\": 3600"))
			Expect(deploy.Spec.Template.Annotations).To(HaveKey("cdn.cloudwm-cdn.com/policy-hash"))
		})
	})

	Context("When testing CDN security and captcha policy", func() {
		ctx := context.Background()

		AfterEach(func() {
			tenantList := &cdnv1.CdnTenantList{}
			_ = k8sClient.List(ctx, tenantList)
			for _, t := range tenantList.Items {
				t.Finalizers = nil
				_ = k8sClient.Update(ctx, &t)
				_ = k8sClient.Delete(ctx, &t)
			}
			_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: configSecretName, Namespace: "default"}})
		})

		It("should fail closed and set conditions for controller policy validation failures", func() {
			const resourceName = "tenant-invalid-policy"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}
			var body map[string]interface{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				payload, err := io.ReadAll(r.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(stdjson.Unmarshal(payload, &body)).To(Succeed())
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			existingReplicas := int32(1)
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: resourceName}})).To(Succeed())
			Expect(k8sClient.Create(ctx, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "tenant", Namespace: resourceName},
				Spec: appsv1.DeploymentSpec{
					Replicas: &existingReplicas,
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "tenant"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "tenant"}},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "nginx", Image: "old-image"}}},
					},
				},
			})).To(Succeed())
			scaledObject := newScaledObject(&cdnv1.CdnTenant{ObjectMeta: metav1.ObjectMeta{Name: resourceName}})
			scaledObject.Object["spec"] = map[string]interface{}{
				"scaleTargetRef": map[string]interface{}{"name": "tenant"},
				"triggers":       []interface{}{},
			}
			Expect(k8sClient.Create(ctx, scaledObject)).To(Succeed())
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains:     []cdnv1.Domain{{Name: "invalid.example.com", Cert: "cert", Key: "key"}},
					Origins:     []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Autoscaling: &cdnv1.CdnTenantAutoscalingSpec{Enabled: true, MinReplicas: 1, MaxReplicas: 5},
					Redirects: []cdnv1.RedirectRule{{
						Name: "self",
						When: cdnv1.RedirectWhen{Path: &cdnv1.RedirectPathMatch{Type: "glob", Value: "/same"}},
						To:   "/same",
					}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			secondaries := []byte(`{"secondary-a":{"url":"` + server.URL + `","user":"u","pass":"p"}}`)
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: configSecretName, Namespace: "default"},
				Data: map[string][]byte{
					"primaryKey":       []byte("primary"),
					"secondaries.json": secondaries,
				},
			})).To(Succeed())
			controllerReconciler := &CdnTenantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &record.FakeRecorder{}}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, deploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(deploy.Spec.Replicas).NotTo(BeNil())
			Expect(*deploy.Spec.Replicas).To(Equal(int32(0)))
			Expect(deploy.Spec.Template.Annotations).To(HaveKey("cdn.cloudwm-cdn.com/invalid-policy-hash"))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: scaledObjectName}, scaledObject)).To(MatchError(ContainSubstring("not found")))
			Expect(body).To(HaveKey("redirects"))

			tenant := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())
			readyCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Reason).To(Equal(cdnv1.ReasonPolicyValidationFailed))
		})

		It("should reject incomplete typed security, captcha, redirect policy fields", func() {
			manyRules := make([]cdnv1.NamedPathRule, maxPolicyRules+1)
			for i := range manyRules {
				manyRules[i] = cdnv1.NamedPathRule{Name: fmt.Sprintf("rule-%d", i), Match: cdnv1.PathMatch{Type: "glob", Path: fmt.Sprintf("/rule-%d", i)}}
			}
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: "tenant-invalid-extra-policy", Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "invalid-extra.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Security: &cdnv1.SecurityConfig{
						Methods:   &cdnv1.MethodsConfig{Allow: []cdnv1.HTTPMethod{"GET", "GET1"}, Block: []cdnv1.HTTPMethod{"GET"}},
						RateLimit: &cdnv1.RateLimitConfig{Enabled: true, Requests: 1, Period: "90m", Key: "clientIp", Action: "captcha"},
						Request:   &cdnv1.RequestSecurityConfig{MaxBodySize: "2g"},
						URLs:      &cdnv1.URLSecurityConfig{Block: append([]cdnv1.NamedPathRule{{Name: "encoded", Match: cdnv1.PathMatch{Type: "glob", Path: "/%2e%2e/admin"}}}, manyRules...)},
					},
					Captcha: &cdnv1.CaptchaConfig{Enabled: true, Provider: "bad", CookieTtl: "30x", Rules: []cdnv1.NamedPathRule{{Name: "protected", Match: cdnv1.PathMatch{Type: "glob", Path: "/protected/*"}}}},
					Redirects: []cdnv1.RedirectRule{
						{
							Name: "bad-redirect",
							When: cdnv1.RedirectWhen{
								Path:           &cdnv1.RedirectPathMatch{Type: "glob", Value: "relative/*"},
								OriginStatus:   []cdnv1.HTTPStatusCode{200, 200},
								UpstreamStatus: []cdnv1.HTTPStatusCode{700},
							},
							To: "//evil.example.com",
						},
						{
							Name: "missing-path",
							To:   "/ok",
						},
						{
							Name: "self",
							When: cdnv1.RedirectWhen{Path: &cdnv1.RedirectPathMatch{Type: "glob", Value: "/same"}},
							To:   "/same?x=1",
						},
					},
				},
			}
			_, sizeErr := parseMaxBodySize("9223372036854775807g")
			Expect(sizeErr).To(HaveOccurred())
			errs := validateTenantPolicy(ctx, k8sClient, resource)
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("security.methods.allow contains invalid HTTP method"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("security.methods method"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("security.request.maxBodySize"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("security.urls.block may contain at most"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("encoded path traversal"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("captcha.cookieTtl"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("captcha.provider must be turnstile"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("captcha.siteKey is required"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("captcha.secret is required"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("invalid HTTP status"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("requires at least one matcher"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("when.path.value is invalid"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("to is invalid"))
			Expect(strings.Join(errs, "; ")).To(ContainSubstring("direct self-redirects are not allowed"))

			protectedConfig := &cdnv1.CdnTenant{Spec: cdnv1.CdnTenantSpec{Config: map[string]string{"POP_ID": "tenant-pop"}}}
			Expect(strings.Join(validateTenantPolicy(ctx, k8sClient, protectedConfig), "; ")).To(ContainSubstring("platform-managed"))

			ipResource := &cdnv1.CdnTenant{Spec: cdnv1.CdnTenantSpec{Security: &cdnv1.SecurityConfig{IPAccess: &cdnv1.IPAccessConfig{AllowCidrs: []string{"10.0.0.0/8"}}}}}
			Expect(strings.Join(validateTenantPolicy(ctx, k8sClient, ipResource), "; ")).To(ContainSubstring("trusted client IP"))

			captchaRateLimit := &cdnv1.CdnTenant{Spec: cdnv1.CdnTenantSpec{Security: &cdnv1.SecurityConfig{RateLimit: &cdnv1.RateLimitConfig{Enabled: true, Requests: 1, Period: "1m", Key: "clientIp", Action: "captcha"}}}}
			Expect(strings.Join(validateTenantPolicy(ctx, k8sClient, captchaRateLimit), "; ")).To(ContainSubstring("security.rateLimit.action=captcha requires captcha.enabled=true"))

			invalid303 := &cdnv1.CdnTenant{Spec: cdnv1.CdnTenantSpec{Redirects: []cdnv1.RedirectRule{{Name: "see-other", When: cdnv1.RedirectWhen{Path: &cdnv1.RedirectPathMatch{Type: "glob", Value: "/old"}}, To: "/new", Status: 303}}}}
			Expect(strings.Join(validateTenantPolicy(ctx, k8sClient, invalid303), "; ")).To(ContainSubstring("status must be one of"))
		})

		It("should render captcha secret from CRD and mount signing-key secret when captcha is enabled", func() {
			const resourceName = "tenant-captcha-policy"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: resourceName}})).To(Succeed())
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "captcha.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Captcha: &cdnv1.CaptchaConfig{
						Enabled:  true,
						Provider: "turnstile",
						SiteKey:  "site-key",
						Secret:   "provider-secret",
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			controllerReconciler := &CdnTenantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &record.FakeRecorder{}}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, deploy)).To(Succeed())
			envMap := map[string]string{}
			for _, env := range deploy.Spec.Template.Spec.Containers[0].Env {
				envMap[env.Name] = env.Value
			}
			Expect(envMap).NotTo(HaveKey("CAPTCHA_SECRET_PATH"))
			Expect(envMap).To(HaveKeyWithValue("CAPTCHA_SIGNING_KEY_PATH", "/etc/cwm-cdn/signing-key/signing-key"))
			volumeSecrets := map[string]string{}
			for _, volume := range deploy.Spec.Template.Spec.Volumes {
				if volume.Secret != nil {
					volumeSecrets[volume.Name] = volume.Secret.SecretName
				}
			}
			Expect(volumeSecrets).NotTo(HaveKey("captcha-secret"))

			policyConfigMap := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: policyConfigMapName}, policyConfigMap)).To(Succeed())
			Expect(policyConfigMap.Data[policyConfigMapKey]).To(ContainSubstring(`"secret": "provider-secret"`))

			signingKey := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: signingKeySecretName}, signingKey)).To(Succeed())
			Expect(signingKey.Data).To(HaveKey(signingKeySecretKey))
			oldSigningHash := deploy.Spec.Template.Annotations["cdn.cloudwm-cdn.com/signing-key-rollout-hash"]
			Expect(deploy.Spec.Template.Annotations).NotTo(HaveKey("cdn.cloudwm-cdn.com/captcha-secret-rollout-hash"))
			Expect(oldSigningHash).NotTo(BeEmpty())

			Expect(k8sClient.Get(ctx, typeNamespacedName, resource)).To(Succeed())
			resource.Spec.Captcha.Secret = "rotated-provider-secret"
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: policyConfigMapName}, policyConfigMap)).To(Succeed())
			Expect(policyConfigMap.Data[policyConfigMapKey]).To(ContainSubstring(`"secret": "rotated-provider-secret"`))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceName, Name: "tenant"}, deploy)).To(Succeed())
			Expect(deploy.Spec.Template.Annotations["cdn.cloudwm-cdn.com/signing-key-rollout-hash"]).To(Equal(oldSigningHash))
		})
	})

	// ============================================
	// Elasticsearch Configuration Tests
	// ============================================
	Context("When testing Elasticsearch configuration", func() {
		ctx := context.Background()

		AfterEach(func() {
			tenantList := &cdnv1.CdnTenantList{}
			_ = k8sClient.List(ctx, tenantList)
			for _, t := range tenantList.Items {
				t.Finalizers = nil
				_ = k8sClient.Update(ctx, &t)
				_ = k8sClient.Delete(ctx, &t)
			}
		})

		It("should set ES environment variables when elasticsearch is enabled", func() {
			const resourceName = "tenant-es-enabled"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "es.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Elasticsearch: &cdnv1.ElasticsearchConfig{
						Enabled: true,
						Config: map[string]string{
							"endpoints": "http://elasticsearch:9200",
							"auth":      "user:pass",
							"bulk":      "100",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			envVars := deploy.Spec.Template.Spec.Containers[0].Env
			envMap := make(map[string]string)
			for _, env := range envVars {
				envMap[env.Name] = env.Value
			}

			Expect(envMap).To(HaveKeyWithValue("ENABLE_TENANT_ACCESS_LOGS", "true"))
			Expect(envMap).To(HaveKeyWithValue("ENABLE_ES_SINK", "true"))
			Expect(envMap).To(HaveKeyWithValue("ES_ENDPOINTS", "http://elasticsearch:9200"))
			Expect(envMap).To(HaveKeyWithValue("ES_AUTH", "user:pass"))
			Expect(envMap).To(HaveKeyWithValue("ES_BULK", "100"))
		})

		It("should NOT set ES environment variables when elasticsearch is disabled", func() {
			const resourceName = "tenant-es-disabled"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "no-es.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Elasticsearch: &cdnv1.ElasticsearchConfig{
						Enabled: false,
						Config: map[string]string{
							"endpoints": "http://elasticsearch:9200",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			envVars := deploy.Spec.Template.Spec.Containers[0].Env
			envMap := make(map[string]string)
			for _, env := range envVars {
				envMap[env.Name] = env.Value
			}

			Expect(envMap).NotTo(HaveKey("ENABLE_TENANT_ACCESS_LOGS"))
			Expect(envMap).NotTo(HaveKey("ENABLE_ES_SINK"))
			Expect(envMap).NotTo(HaveKey("ES_ENDPOINTS"))
		})

		It("should NOT set ES environment variables when elasticsearch is nil", func() {
			const resourceName = "tenant-es-nil"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains:       []cdnv1.Domain{{Name: "nil-es.example.com", Cert: "cert", Key: "key"}},
					Origins:       []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Elasticsearch: nil,
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			envVars := deploy.Spec.Template.Spec.Containers[0].Env
			envMap := make(map[string]string)
			for _, env := range envVars {
				envMap[env.Name] = env.Value
			}

			Expect(envMap).NotTo(HaveKey("ENABLE_TENANT_ACCESS_LOGS"))
			Expect(envMap).NotTo(HaveKey("ENABLE_ES_SINK"))
		})
	})

	// ============================================
	// Domain and Origin Configuration Tests
	// ============================================
	Context("When testing domain and origin configuration", func() {
		ctx := context.Background()

		AfterEach(func() {
			tenantList := &cdnv1.CdnTenantList{}
			_ = k8sClient.List(ctx, tenantList)
			for _, t := range tenantList.Items {
				t.Finalizers = nil
				_ = k8sClient.Update(ctx, &t)
				_ = k8sClient.Delete(ctx, &t)
			}
		})

		It("should correctly set domain environment variables", func() {
			const resourceName = "tenant-domain-config"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{
						{
							Name: "domain1.example.com",
							Cert: "cert1-content",
							Key:  "key1-content",
							Config: map[string]string{
								"cache_ttl":     "3600",
								"custom_header": "X-Custom-1",
							},
						},
					},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			envVars := deploy.Spec.Template.Spec.Containers[0].Env
			envMap := make(map[string]string)
			for _, env := range envVars {
				envMap[env.Name] = env.Value
			}

			Expect(envMap).To(HaveKeyWithValue("D0_NAME", "domain1.example.com"))
			Expect(envMap).To(HaveKeyWithValue("D0_CERT", "cert1-content"))
			Expect(envMap).To(HaveKeyWithValue("D0_KEY", "key1-content"))
			Expect(envMap).To(HaveKeyWithValue("D0_CACHE_TTL", "3600"))
			Expect(envMap).To(HaveKeyWithValue("D0_CUSTOM_HEADER", "X-Custom-1"))
		})

		It("should correctly set origin environment variables", func() {
			const resourceName = "tenant-origin-config"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "origin.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{
						{
							Url: "http://backend1.example.com:8080",
							Config: map[string]string{
								"timeout":     "30",
								"retry_count": "3",
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			envVars := deploy.Spec.Template.Spec.Containers[0].Env
			envMap := make(map[string]string)
			for _, env := range envVars {
				envMap[env.Name] = env.Value
			}

			Expect(envMap).To(HaveKeyWithValue("O0_URL", "http://backend1.example.com:8080"))
			Expect(envMap).To(HaveKeyWithValue("O0_NAME", "origin-0"))
			Expect(envMap).To(HaveKeyWithValue("O0_WEIGHT", "1"))
			Expect(envMap).To(HaveKeyWithValue("O0_TIMEOUT", "30"))
			Expect(envMap).To(HaveKeyWithValue("O0_RETRY_COUNT", "3"))
		})

		It("should correctly set multiple origins and health check environment variables", func() {
			const resourceName = "tenant-multi-origin-config"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}
			weight := int32(2)
			healthEnabled := true
			expectedStatus := int32(204)
			healthyThreshold := int32(1)
			unhealthyThreshold := int32(2)

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "multi.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{
						{
							Name:   "origin-a",
							Url:    "http://origin-a.example.com",
							Weight: &weight,
							HealthCheck: &cdnv1.OriginHealthCheck{
								Enabled:            &healthEnabled,
								Path:               "/healthz",
								ExpectedStatus:     &expectedStatus,
								Interval:           "5s",
								Timeout:            "500ms",
								HealthyThreshold:   &healthyThreshold,
								UnhealthyThreshold: &unhealthyThreshold,
							},
						},
						{
							Url: "https://origin-b.example.com",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			envMap := make(map[string]string)
			for _, env := range deploy.Spec.Template.Spec.Containers[0].Env {
				envMap[env.Name] = env.Value
			}

			Expect(envMap).To(HaveKeyWithValue("O0_URL", "http://origin-a.example.com"))
			Expect(envMap).To(HaveKeyWithValue("O0_NAME", "origin-a"))
			Expect(envMap).To(HaveKeyWithValue("O0_WEIGHT", "2"))
			Expect(envMap).To(HaveKeyWithValue("O0_HEALTHCHECK_ENABLED", "true"))
			Expect(envMap).To(HaveKeyWithValue("O0_HEALTHCHECK_PATH", "/healthz"))
			Expect(envMap).To(HaveKeyWithValue("O0_HEALTHCHECK_EXPECTEDSTATUS", "204"))
			Expect(envMap).To(HaveKeyWithValue("O0_HEALTHCHECK_INTERVAL", "5s"))
			Expect(envMap).To(HaveKeyWithValue("O0_HEALTHCHECK_TIMEOUT", "500ms"))
			Expect(envMap).To(HaveKeyWithValue("O0_HEALTHCHECK_HEALTHYTHRESHOLD", "1"))
			Expect(envMap).To(HaveKeyWithValue("O0_HEALTHCHECK_UNHEALTHYTHRESHOLD", "2"))
			Expect(envMap).To(HaveKeyWithValue("O1_URL", "https://origin-b.example.com"))
			Expect(envMap).To(HaveKeyWithValue("O1_NAME", "origin-1"))
			Expect(envMap).To(HaveKeyWithValue("O1_WEIGHT", "1"))
		})

		It("should set TENANT_NAME environment variable", func() {
			const resourceName = "tenant-name-env"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "name.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			envVars := deploy.Spec.Template.Spec.Containers[0].Env
			envMap := make(map[string]string)
			for _, env := range envVars {
				envMap[env.Name] = env.Value
			}

			Expect(envMap).To(HaveKeyWithValue("TENANT_NAME", resourceName))
		})
	})

	// ============================================
	// Edge Cases Tests
	// ============================================
	Context("When testing edge cases", func() {
		ctx := context.Background()

		AfterEach(func() {
			tenantList := &cdnv1.CdnTenantList{}
			_ = k8sClient.List(ctx, tenantList)
			for _, t := range tenantList.Items {
				t.Finalizers = nil
				_ = k8sClient.Update(ctx, &t)
				_ = k8sClient.Delete(ctx, &t)
			}
		})

		It("should handle reconciling a non-existent tenant gracefully", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "non-existent-tenant",
					Namespace: "default",
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
			Expect(result.RequeueAfter).To(BeZero())
		})

		It("should handle tenant with empty config map", func() {
			const resourceName = "tenant-empty-config"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "empty.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Config:  map[string]string{}, // Empty config
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify deployment was created with defaults
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(*deploy.Spec.Replicas).To(Equal(int32(1)))
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/cloudwebmanage/cwm-cdn-api-tenant-nginx:latest"))
		})

		It("should handle tenant with nil config map", func() {
			const resourceName = "tenant-nil-config"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "nil.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Config:  nil, // Nil config
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify deployment was created with defaults
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(*deploy.Spec.Replicas).To(Equal(int32(1)))
		})

		It("should handle invalid replicas config value gracefully", func() {
			const resourceName = "tenant-invalid-replicas"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "invalid.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Config: map[string]string{
						"REPLICAS": "not-a-number", // Invalid value
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Should fall back to default replicas
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(*deploy.Spec.Replicas).To(Equal(int32(1))) // Default value
		})
	})

	// ============================================
	// Idempotency Tests
	// ============================================
	Context("When testing idempotency", func() {
		const resourceName = "tenant-idempotent"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "idempotent.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &cdnv1.CdnTenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				resource.Finalizers = nil
				_ = k8sClient.Update(ctx, resource)
				_ = k8sClient.Delete(ctx, resource)
			}
		})

		It("should be idempotent - multiple reconciles produce same result", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			By("First reconcile")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Make deployment ready
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())
			deploy.Status.Replicas = 1
			deploy.Status.ReadyReplicas = 1
			deploy.Status.AvailableReplicas = 1
			Expect(k8sClient.Status().Update(ctx, deploy)).To(Succeed())

			By("Second reconcile")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Get state after second reconcile
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())
			firstDeployResourceVersion := deploy.ResourceVersion

			By("Third reconcile - should be no-op")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify deployment wasn't modified
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())
			Expect(deploy.ResourceVersion).To(Equal(firstDeployResourceVersion), "Deployment should not be modified on subsequent reconciles")
		})

		It("should maintain stable conditions when nothing changes", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			By("Initial reconcile")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Make deployment ready
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())
			deploy.Status.Replicas = 1
			deploy.Status.ReadyReplicas = 1
			deploy.Status.AvailableReplicas = 1
			Expect(k8sClient.Status().Update(ctx, deploy)).To(Succeed())

			By("Reconcile to set success conditions")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Get conditions after successful reconcile
			tenant := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())
			readyCondition := meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
			initialTransitionTime := readyCondition.LastTransitionTime

			By("Another reconcile - conditions should stay stable")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify conditions haven't changed
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())
			readyCondition = meta.FindStatusCondition(tenant.Status.Conditions, cdnv1.TypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition.LastTransitionTime).To(Equal(initialTransitionTime), "LastTransitionTime should not change when status remains the same")
		})
	})

	// ============================================
	// Service Configuration Tests
	// ============================================
	Context("When testing service configuration", func() {
		const resourceName = "tenant-service"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "service.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &cdnv1.CdnTenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				resource.Finalizers = nil
				_ = k8sClient.Update(ctx, resource)
				_ = k8sClient.Delete(ctx, resource)
			}
		})

		It("should create service with correct ports", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, svc)).To(Succeed())

			Expect(svc.Spec.Ports).To(HaveLen(2))

			// Find HTTP port
			var httpPort, httpsPort *corev1.ServicePort
			for i := range svc.Spec.Ports {
				if svc.Spec.Ports[i].Name == "http" {
					httpPort = &svc.Spec.Ports[i]
				}
				if svc.Spec.Ports[i].Name == "https" {
					httpsPort = &svc.Spec.Ports[i]
				}
			}

			Expect(httpPort).NotTo(BeNil())
			Expect(httpPort.Port).To(Equal(int32(80)))
			Expect(httpPort.TargetPort.IntVal).To(Equal(int32(80)))
			Expect(httpPort.Protocol).To(Equal(corev1.ProtocolTCP))

			Expect(httpsPort).NotTo(BeNil())
			Expect(httpsPort.Port).To(Equal(int32(443)))
			Expect(httpsPort.TargetPort.IntVal).To(Equal(int32(443)))
			Expect(httpsPort.Protocol).To(Equal(corev1.ProtocolTCP))
		})

		It("should create service with correct selector", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, svc)).To(Succeed())

			Expect(svc.Spec.Selector).To(HaveKeyWithValue("app", "tenant"))
		})

		It("should create service with tenant annotation", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, svc)).To(Succeed())

			Expect(svc.Annotations).To(HaveKeyWithValue("cdn.cloudwm-cdn.com/tenant", "default/"+resourceName))
		})
	})

	// ============================================
	// Namespace Configuration Tests
	// ============================================
	Context("When testing namespace creation", func() {
		const resourceName = "tenant-namespace"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "ns.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &cdnv1.CdnTenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				resource.Finalizers = nil
				_ = k8sClient.Update(ctx, resource)
				_ = k8sClient.Delete(ctx, resource)
			}
		})

		It("should create namespace with same name as tenant", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName}, ns)).To(Succeed())
			Expect(ns.Name).To(Equal(resourceName))
		})
	})

	// ============================================
	// Deployment Configuration Tests
	// ============================================
	Context("When testing deployment configuration", func() {
		const resourceName = "tenant-deployment"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "deploy.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &cdnv1.CdnTenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				resource.Finalizers = nil
				_ = k8sClient.Update(ctx, resource)
				_ = k8sClient.Delete(ctx, resource)
			}
		})

		It("should create deployment with correct labels", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(deploy.Labels).To(HaveKeyWithValue("app", "tenant"))
			Expect(deploy.Spec.Template.Labels).To(HaveKeyWithValue("app", "tenant"))
		})

		It("should create deployment with correct tolerations", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			expectedTolerations := []corev1.Toleration{
				{
					Key:      "cwm-iac-worker-role",
					Operator: corev1.TolerationOpEqual,
					Value:    "cdn",
					Effect:   corev1.TaintEffectNoExecute,
				},
			}
			Expect(deploy.Spec.Template.Spec.Tolerations).To(Equal(expectedTolerations))
		})

		It("should create deployment with correct container name", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(deploy.Spec.Template.Spec.Containers[0].Name).To(Equal("nginx"))
		})

		It("should create deployment with tenant annotation", func() {
			controllerReconciler := &CdnTenantReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: resourceName,
				Name:      "tenant",
			}, deploy)).To(Succeed())

			Expect(deploy.Annotations).To(HaveKeyWithValue("cdn.cloudwm-cdn.com/tenant", "default/"+resourceName))
		})
	})

	Context("When syncing secondaries", func() {
		ctx := context.Background()

		AfterEach(func() {
			tenantList := &cdnv1.CdnTenantList{}
			_ = k8sClient.List(ctx, tenantList)
			for _, t := range tenantList.Items {
				t.Finalizers = nil
				_ = k8sClient.Update(ctx, &t)
				_ = k8sClient.Delete(ctx, &t)
			}
			_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: configSecretName, Namespace: "default"}})
		})

		It("should include typed spec fields in secondary sync and update status", func() {
			const resourceName = "tenant-secondary-status"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}
			var body map[string]interface{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				payload, err := io.ReadAll(r.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(stdjson.Unmarshal(payload, &body)).To(Succeed())
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			cacheTTL := "1h"
			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default", Annotations: map[string]string{}},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "secondary.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
					Cache:   &cdnv1.CacheConfig{Mode: "cache_everything", EdgeTtl: cacheTTL},
					Redirects: []cdnv1.RedirectRule{{
						Name: "docs",
						When: cdnv1.RedirectWhen{Path: &cdnv1.RedirectPathMatch{Type: "glob", Value: "/docs/*"}},
						To:   "/new-docs/",
					}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			secondaries := []byte(`{"secondary-a":{"url":"` + server.URL + `","user":"u","pass":"p"}}`)
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: configSecretName, Namespace: "default"},
				Data: map[string][]byte{
					"primaryKey":       []byte("primary"),
					"secondaries.json": secondaries,
				},
			})).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &record.FakeRecorder{}}
			tenant := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())

			requeueAfter, err := controllerReconciler.syncSecondaries(reconcile.Request{NamespacedName: typeNamespacedName}, ctx, tenant)
			Expect(err).NotTo(HaveOccurred())
			Expect(requeueAfter).To(BeZero())
			Expect(body).To(HaveKey("cache"))
			Expect(body).To(HaveKey("redirects"))

			updated := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.SecondarySync).To(HaveLen(1))
			Expect(updated.Status.SecondarySync[0].Name).To(Equal("secondary-a"))
			Expect(updated.Status.SecondarySync[0].Status).To(Equal("synced"))
			Expect(updated.Status.SecondarySync[0].DesiredHash).NotTo(BeEmpty())
			Expect(updated.Status.SecondarySync[0].SyncedHash).To(Equal(updated.Status.SecondarySync[0].DesiredHash))
			Expect(updated.Status.SecondarySync[0].LatencySeconds).NotTo(BeEmpty())
			lastSuccess := updated.Status.SecondarySync[0].LastSuccessTime
			latencySeconds := updated.Status.SecondarySync[0].LatencySeconds
			Expect(lastSuccess).NotTo(BeNil())
			updated.Status.SecondarySync[0].LastError = "previous failure"
			Expect(k8sClient.Status().Update(ctx, updated)).To(Succeed())
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())

			requeueAfter, err = controllerReconciler.syncSecondaries(reconcile.Request{NamespacedName: typeNamespacedName}, ctx, updated)
			Expect(err).NotTo(HaveOccurred())
			Expect(requeueAfter).To(BeZero())
			reconciledAgain := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, reconciledAgain)).To(Succeed())
			Expect(reconciledAgain.Status.SecondarySync).To(HaveLen(1))
			Expect(reconciledAgain.Status.SecondarySync[0].LastSuccessTime).To(Equal(lastSuccess))
			Expect(reconciledAgain.Status.SecondarySync[0].LatencySeconds).To(Equal(latencySeconds))
			Expect(reconciledAgain.Status.SecondarySync[0].LastError).To(BeEmpty())
		})

		It("should retry a conflict during secondary sync status updates", func() {
			const resourceName = "tenant-secondary-status-conflict"
			typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: "default"}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			resource := &cdnv1.CdnTenant{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default", Annotations: map[string]string{}},
				Spec: cdnv1.CdnTenantSpec{
					Domains: []cdnv1.Domain{{Name: "secondary-conflict.example.com", Cert: "cert", Key: "key"}},
					Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			secondaries := []byte(`{"secondary-a":{"url":"` + server.URL + `","user":"u","pass":"p"}}`)
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: configSecretName, Namespace: "default"},
				Data: map[string][]byte{
					"primaryKey":       []byte("primary"),
					"secondaries.json": secondaries,
				},
			})).To(Succeed())

			controllerReconciler := &CdnTenantReconciler{
				Client: beforeStatusUpdateClient{
					Client:             k8sClient,
					beforeStatusUpdate: updateTenantAnnotationOnce(k8sClient, "secondary-status"),
				},
				Scheme:   k8sClient.Scheme(),
				Recorder: &record.FakeRecorder{},
			}
			tenant := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, tenant)).To(Succeed())

			requeueAfter, err := controllerReconciler.syncSecondaries(reconcile.Request{NamespacedName: typeNamespacedName}, ctx, tenant)
			Expect(requeueAfter).To(BeZero())
			Expect(err).NotTo(HaveOccurred())

			latest := &cdnv1.CdnTenant{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, latest)).To(Succeed())
			Expect(latest.Annotations).To(HaveKeyWithValue("cdn.cloudwm-cdn.com/status-conflict-test", "secondary-status"))
			Expect(latest.Annotations).To(HaveKeyWithValue("cdn.cloudwm-cdn.com/secondaries--secondary-a-status", "synced"))
			Expect(latest.Status.SecondarySync).To(HaveLen(1))
			Expect(latest.Status.SecondarySync[0].Name).To(Equal("secondary-a"))
			Expect(latest.Status.SecondarySync[0].Status).To(Equal("synced"))
			Expect(latest.Status.SecondarySync[0].SyncedHash).To(Equal(latest.Status.SecondarySync[0].DesiredHash))
		})

		It("should URL-escape primary keys when syncing deletes", func() {
			const resourceName = "tenant-secondary-delete-query"
			primaryKey := "a+b&c=d"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/delete"))
				Expect(r.URL.Query().Get("cdn_tenant_name")).To(Equal(resourceName))
				Expect(r.URL.Query().Get("primary_key")).To(Equal(primaryKey))
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			deletionTime := metav1.Now()
			tenant := &cdnv1.CdnTenant{ObjectMeta: metav1.ObjectMeta{Name: resourceName, DeletionTimestamp: &deletionTime}}
			controllerReconciler := &CdnTenantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: &record.FakeRecorder{}}

			requeue, err := controllerReconciler.syncSecondary(ctx, tenant, nil, "secondary-a", map[string]string{"url": server.URL, "user": "u", "pass": "p"}, primaryKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(requeue).To(BeFalse())
		})
	})

	It("should get tenant hash", func() {
		controllerReconciler := &CdnTenantReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: &record.FakeRecorder{},
		}

		hash1, err := controllerReconciler.getTenantHash(&cdnv1.CdnTenant{
			Spec: cdnv1.CdnTenantSpec{
				Domains: []cdnv1.Domain{{Name: "example.com", Cert: "cert", Key: "key"}},
				Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
			},
		})
		Expect(err).NotTo(HaveOccurred())
		log.Printf("Hash1: %s", hash1)
		Expect(hash1).To(Equal("d1f647f4a28f265f991c63769448b5d82db1714d8df27f73bc227ea4554e658d"))
	})

	It("should include propagated certificate data in secondary tenant hash", func() {
		baseSpec := cdnv1.CdnTenantSpec{
			Domains: []cdnv1.Domain{{
				Name: "example.com",
				TLS:  &cdnv1.DomainTLS{Mode: "provided"},
				Cert: "cert-1",
				Key:  "key-1",
			}},
			Origins: []cdnv1.Origin{{Url: "http://origin.example.com"}},
		}
		renewedSpec, err := cloneTenantSpec(baseSpec)
		Expect(err).NotTo(HaveOccurred())
		renewedSpec.Domains[0].Cert = "cert-2"
		renewedSpec.Domains[0].Key = "key-2"

		baseHash, err := getTenantSpecHash(baseSpec)
		Expect(err).NotTo(HaveOccurred())
		renewedHash, err := getTenantSpecHash(renewedSpec)
		Expect(err).NotTo(HaveOccurred())

		Expect(renewedHash).NotTo(Equal(baseHash))
	})
})
