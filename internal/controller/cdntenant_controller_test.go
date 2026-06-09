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
	"log"

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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cdnv1 "github.com/CloudWebManage/cwm-cdn-operator/api/v1"
)

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
				{Name: "D0_NAME", Value: "test.example.com"},
				{Name: "D0_KEY", Value: "bbb"},
				{Name: "D0_CERT", Value: "aaa"},
				{Name: "D0_EXAMPLE", Value: "value"},
				{Name: "D0_FOO", Value: "BAR"},
				{Name: "O0_URL", Value: "http://example.com"},
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
			Expect(envMap).To(HaveKeyWithValue("O0_TIMEOUT", "30"))
			Expect(envMap).To(HaveKeyWithValue("O0_RETRY_COUNT", "3"))
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
})
