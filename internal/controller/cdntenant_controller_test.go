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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cdnv1 "github.com/CloudWebManage/cwm-cdn-operator/api/v1"
)

var _ = Describe("CdnTenant Controller", func() {
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
})
