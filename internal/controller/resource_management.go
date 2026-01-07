package controller

import (
	"context"
	"fmt"

	n8nv1alpha1 "github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// reconcileResource handles the common pattern of getting/creating a resource
func (r *N8nReconciler) reconcileResource(ctx context.Context, n8n *n8nv1alpha1.N8n, resource client.Object, createFn func() error) error {
	err := r.Get(ctx, types.NamespacedName{Name: n8n.Name, Namespace: n8n.Namespace}, resource)
	if err != nil && apierrors.IsNotFound(err) {
		if err := createFn(); err != nil {
			return fmt.Errorf("failed to create resource: %w", err)
		}
		return nil
	} else if err != nil {
		return err
	}
	// Resource exists, no need to create it
	return nil
}

// updateStatus handles updating the status conditions of the N8n resource
func (r *N8nReconciler) updateStatus(ctx context.Context, n8n *n8nv1alpha1.N8n, conditionType string, status metav1.ConditionStatus, reason, message string) error {
	meta.SetStatusCondition(&n8n.Status.Conditions, metav1.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
	return r.Status().Update(ctx, n8n)
}

// setStatusCondition sets a status condition without updating the resource
func (r *N8nReconciler) setStatusCondition(n8n *n8nv1alpha1.N8n, conditionType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&n8n.Status.Conditions, metav1.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// updateResourceWithRetry updates the resource with retry logic for optimistic concurrency
func (r *N8nReconciler) updateResourceWithRetry(ctx context.Context, n8n *n8nv1alpha1.N8n) error {
	const maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		// Try to update the resource spec/metadata first
		if err := r.Update(ctx, n8n); err != nil {
			if apierrors.IsConflict(err) && i < maxRetries-1 {
				// Resource was modified, re-fetch and retry
				fresh := &n8nv1alpha1.N8n{}
				if fetchErr := r.Get(ctx, client.ObjectKeyFromObject(n8n), fresh); fetchErr != nil {
					return fetchErr
				}
				// Copy our changes to the fresh resource
				fresh.Status = n8n.Status
				fresh.Finalizers = n8n.Finalizers
				*n8n = *fresh
				continue
			}
			return err
		}
		break // Success, exit retry loop
	}

	// Now update the status separately
	for i := 0; i < maxRetries; i++ {
		if err := r.Status().Update(ctx, n8n); err != nil {
			if apierrors.IsConflict(err) && i < maxRetries-1 {
				// Resource was modified, re-fetch and retry
				fresh := &n8nv1alpha1.N8n{}
				if fetchErr := r.Get(ctx, client.ObjectKeyFromObject(n8n), fresh); fetchErr != nil {
					return fetchErr
				}
				// Copy our status changes to the fresh resource
				fresh.Status = n8n.Status
				*n8n = *fresh
				continue
			}
			return err
		}
		return nil // Success
	}
	return fmt.Errorf("failed to update resource status after %d retries", maxRetries)
}

// handleResourceError updates the status condition when an error occurs
func (r *N8nReconciler) handleResourceError(ctx context.Context, n8n *n8nv1alpha1.N8n, err error, resourceType string) error {
	if err := r.updateStatus(ctx, n8n, typeAvailableN8n,
		metav1.ConditionFalse,
		"Reconciling",
		fmt.Sprintf("Failed to manage %s for the custom resource (%s): %v", resourceType, n8n.Name, err)); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}
	return err
}

// createOrUpdateDeployment handles the deployment reconciliation
func (r *N8nReconciler) createOrUpdateDeployment(ctx context.Context, n8n *n8nv1alpha1.N8n) error {
	log := log.FromContext(ctx)
	existingDep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: n8n.Name, Namespace: n8n.Namespace}, existingDep)

	if err != nil && apierrors.IsNotFound(err) {
		// Deployment doesn't exist, create it
		dep, err := r.deploymentForN8n(n8n)
		if err != nil {
			return r.handleResourceError(ctx, n8n, err, "Deployment")
		}
		log.Info("Creating new deployment", "name", dep.Name)
		return r.Create(ctx, dep)
	} else if err != nil {
		return r.handleResourceError(ctx, n8n, err, "Deployment")
	}

	// Deployment exists - check if it needs to be updated
	desiredDep, err := r.deploymentForN8n(n8n)
	if err != nil {
		return r.handleResourceError(ctx, n8n, err, "Deployment")
	}

	needsUpdate := r.deploymentNeedsUpdate(existingDep, desiredDep)

	if needsUpdate {
		log.Info("Deployment configuration has changed, updating", "name", existingDep.Name)

		// Preserve the existing deployment's metadata
		desiredDep.ObjectMeta.ResourceVersion = existingDep.ObjectMeta.ResourceVersion
		desiredDep.ObjectMeta.UID = existingDep.ObjectMeta.UID
		desiredDep.ObjectMeta.CreationTimestamp = existingDep.ObjectMeta.CreationTimestamp
		desiredDep.ObjectMeta.Generation = existingDep.ObjectMeta.Generation

		// Update the deployment
		if err := r.Update(ctx, desiredDep); err != nil {
			return r.handleResourceError(ctx, n8n, err, "Deployment")
		}
		log.Info("Deployment updated successfully", "name", desiredDep.Name)
	}

	return nil
}

// deploymentNeedsUpdate compares the existing deployment with the desired state
// and returns true if an update is needed
func (r *N8nReconciler) deploymentNeedsUpdate(existing, desired *appsv1.Deployment) bool {
	// Check if the image has changed
	if len(existing.Spec.Template.Spec.Containers) > 0 && len(desired.Spec.Template.Spec.Containers) > 0 {
		if existing.Spec.Template.Spec.Containers[0].Image != desired.Spec.Template.Spec.Containers[0].Image {
			return true
		}
	}

	// Check if environment variables have changed
	if !envVarsEqual(existing.Spec.Template.Spec.Containers[0].Env, desired.Spec.Template.Spec.Containers[0].Env) {
		return true
	}

	// Check if volumes have changed
	if !volumesEqual(existing.Spec.Template.Spec.Volumes, desired.Spec.Template.Spec.Volumes) {
		return true
	}

	// Check if volume mounts have changed
	if len(existing.Spec.Template.Spec.Containers) > 0 && len(desired.Spec.Template.Spec.Containers) > 0 {
		if !volumeMountsEqual(existing.Spec.Template.Spec.Containers[0].VolumeMounts, desired.Spec.Template.Spec.Containers[0].VolumeMounts) {
			return true
		}
	}

	// Check if init containers have changed
	if !initContainersEqual(existing.Spec.Template.Spec.InitContainers, desired.Spec.Template.Spec.InitContainers) {
		return true
	}

	return false
}

// envVarsEqual compares two slices of environment variables
func envVarsEqual(existing, desired []corev1.EnvVar) bool {
	if len(existing) != len(desired) {
		return false
	}

	// Create maps for easier comparison
	existingMap := make(map[string]corev1.EnvVar)
	for _, env := range existing {
		existingMap[env.Name] = env
	}

	for _, desiredEnv := range desired {
		existingEnv, exists := existingMap[desiredEnv.Name]
		if !exists {
			return false
		}

		// Compare values
		if desiredEnv.Value != existingEnv.Value {
			return false
		}

		// Compare ValueFrom (for secret references)
		if (desiredEnv.ValueFrom == nil) != (existingEnv.ValueFrom == nil) {
			return false
		}

		if desiredEnv.ValueFrom != nil && existingEnv.ValueFrom != nil {
			if !envVarSourceEqual(existingEnv.ValueFrom, desiredEnv.ValueFrom) {
				return false
			}
		}
	}

	return true
}

// envVarSourceEqual compares two EnvVarSource objects
func envVarSourceEqual(existing, desired *corev1.EnvVarSource) bool {
	// Compare SecretKeyRef
	if (existing.SecretKeyRef == nil) != (desired.SecretKeyRef == nil) {
		return false
	}

	if existing.SecretKeyRef != nil && desired.SecretKeyRef != nil {
		if existing.SecretKeyRef.Name != desired.SecretKeyRef.Name ||
			existing.SecretKeyRef.Key != desired.SecretKeyRef.Key {
			return false
		}
	}

	// Add more comparisons for ConfigMapKeyRef, FieldRef, etc. if needed

	return true
}

// volumesEqual compares two slices of volumes
func volumesEqual(existing, desired []corev1.Volume) bool {
	if len(existing) != len(desired) {
		return false
	}

	existingMap := make(map[string]corev1.Volume)
	for _, vol := range existing {
		existingMap[vol.Name] = vol
	}

	for _, desiredVol := range desired {
		existingVol, exists := existingMap[desiredVol.Name]
		if !exists {
			return false
		}

		// Compare PVC volumes
		if (desiredVol.VolumeSource.PersistentVolumeClaim == nil) != (existingVol.VolumeSource.PersistentVolumeClaim == nil) {
			return false
		}

		if desiredVol.VolumeSource.PersistentVolumeClaim != nil && existingVol.VolumeSource.PersistentVolumeClaim != nil {
			if desiredVol.VolumeSource.PersistentVolumeClaim.ClaimName != existingVol.VolumeSource.PersistentVolumeClaim.ClaimName {
				return false
			}
		}

		// Compare Secret volumes
		if (desiredVol.VolumeSource.Secret == nil) != (existingVol.VolumeSource.Secret == nil) {
			return false
		}

		if desiredVol.VolumeSource.Secret != nil && existingVol.VolumeSource.Secret != nil {
			if desiredVol.VolumeSource.Secret.SecretName != existingVol.VolumeSource.Secret.SecretName {
				return false
			}
		}
	}

	return true
}

// volumeMountsEqual compares two slices of volume mounts
func volumeMountsEqual(existing, desired []corev1.VolumeMount) bool {
	if len(existing) != len(desired) {
		return false
	}

	existingMap := make(map[string]corev1.VolumeMount)
	for _, mount := range existing {
		existingMap[mount.Name] = mount
	}

	for _, desiredMount := range desired {
		existingMount, exists := existingMap[desiredMount.Name]
		if !exists {
			return false
		}

		if desiredMount.MountPath != existingMount.MountPath {
			return false
		}
	}

	return true
}

// initContainersEqual compares two slices of init containers
func initContainersEqual(existing, desired []corev1.Container) bool {
	if len(existing) != len(desired) {
		return false
	}

	// For simplicity, we'll just check if the number and names match
	// A more thorough comparison could check images, commands, etc.
	existingMap := make(map[string]bool)
	for _, container := range existing {
		existingMap[container.Name] = true
	}

	for _, desiredContainer := range desired {
		if !existingMap[desiredContainer.Name] {
			return false
		}
	}

	return true
}

// createOrUpdateService handles the service reconciliation
func (r *N8nReconciler) createOrUpdateService(ctx context.Context, n8n *n8nv1alpha1.N8n) error {
	return r.reconcileResource(ctx, n8n, &corev1.Service{}, func() error {
		svc := r.serviceForN8n(n8n)
		return r.Create(ctx, svc)
	})
}

// createOrUpdateIngress handles the ingress reconciliation
func (r *N8nReconciler) createOrUpdateIngress(ctx context.Context, n8n *n8nv1alpha1.N8n) error {
	if n8n.Spec.Ingress == nil || !n8n.Spec.Ingress.Enable {
		return nil
	}
	return r.reconcileResource(ctx, n8n, &networkingv1.Ingress{}, func() error {
		ing := r.ingressForN8n(n8n)
		return r.Create(ctx, ing)
	})
}

// createOrUpdateHTTPRoute handles the HTTPRoute reconciliation
func (r *N8nReconciler) createOrUpdateHTTPRoute(ctx context.Context, n8n *n8nv1alpha1.N8n) error {
	if n8n.Spec.HTTPRoute == nil || !n8n.Spec.HTTPRoute.Enable {
		return nil
	}
	return r.reconcileResource(ctx, n8n, &gatewayv1.HTTPRoute{}, func() error {
		route := r.httpRouteForN8n(n8n)
		return r.Create(ctx, route)
	})
}

func (r *N8nReconciler) createOrUpdateServiceMonitor(ctx context.Context, n8n *n8nv1alpha1.N8n) error {
	// ServiceMonitor support disabled - we don't use Prometheus
	// If metrics support is needed in the future, this can be re-enabled
	log := log.FromContext(ctx)
	log.V(1).Info("ServiceMonitor support is disabled (Prometheus not used)")
	return nil
}

// createOrUpdateAPICredentialsSecret handles the API credentials secret reconciliation
func (r *N8nReconciler) createOrUpdateAPICredentialsSecret(ctx context.Context, n8n *n8nv1alpha1.N8n) error {
	secretName := getAPICredentialsSecretName(n8n.Name)
	secret := &corev1.Secret{}

	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: n8n.Namespace}, secret)
	if err != nil && apierrors.IsNotFound(err) {
		// Secret doesn't exist, create it
		apiKey, err := generateAPIKey()
		if err != nil {
			return r.handleResourceError(ctx, n8n, err, "API Credentials Secret")
		}

		secret, err := r.apiCredentialsSecretForN8n(n8n, apiKey)
		if err != nil {
			return r.handleResourceError(ctx, n8n, err, "API Credentials Secret")
		}

		if err := r.Create(ctx, secret); err != nil {
			return r.handleResourceError(ctx, n8n, err, "API Credentials Secret")
		}

		// Update status with secret name
		n8n.Status.APICredentialsSecretName = secretName
		if err := r.Status().Update(ctx, n8n); err != nil {
			return fmt.Errorf("failed to update status with API credentials secret name: %w", err)
		}

		return nil
	} else if err != nil {
		return r.handleResourceError(ctx, n8n, err, "API Credentials Secret")
	}

	// Secret exists, ensure status is updated
	if n8n.Status.APICredentialsSecretName != secretName {
		n8n.Status.APICredentialsSecretName = secretName
		if err := r.Status().Update(ctx, n8n); err != nil {
			return fmt.Errorf("failed to update status with API credentials secret name: %w", err)
		}
	}

	return nil
}
