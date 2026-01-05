package credentialmanager

import (
	"context"
	"fmt"
	"time"

	"github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ESOIntegration provides integration with External Secrets Operator
type ESOIntegration struct {
	client client.Client
}

// NewESOIntegration creates a new ESO integration
func NewESOIntegration(client client.Client) *ESOIntegration {
	return &ESOIntegration{
		client: client,
	}
}

// GetSecretDataWithESO retrieves secret data with ESO support
// It handles both regular Kubernetes secrets and ESO-managed secrets
func (eso *ESOIntegration) GetSecretDataWithESO(ctx context.Context, secretRef v1alpha1.SecretReference, namespace string) (map[string][]byte, error) {
	logger := log.FromContext(ctx)
	
	// Determine the namespace to use
	secretNamespace := secretRef.Namespace
	if secretNamespace == "" {
		secretNamespace = namespace
	}
	
	// Get the secret from Kubernetes
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      secretRef.Name,
		Namespace: secretNamespace,
	}
	
	err := eso.client.Get(ctx, secretKey, secret)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", secretNamespace, secretRef.Name, err)
	}
	
	// Check if this is an ESO-managed secret
	if eso.isESOManaged(secret) {
		logger.Info("Detected ESO-managed secret", "secret", secretRef.Name, "namespace", secretNamespace)
		
		// Wait for ESO to populate the secret if it's not ready
		err = eso.waitForESOSecret(ctx, secretKey)
		if err != nil {
			return nil, fmt.Errorf("ESO-managed secret %s/%s is not ready: %w", secretNamespace, secretRef.Name, err)
		}
		
		// Re-fetch the secret after waiting
		err = eso.client.Get(ctx, secretKey, secret)
		if err != nil {
			return nil, fmt.Errorf("failed to re-fetch ESO-managed secret %s/%s: %w", secretNamespace, secretRef.Name, err)
		}
	}
	
	// Validate that the secret has data
	if len(secret.Data) == 0 {
		return nil, fmt.Errorf("secret %s/%s has no data", secretNamespace, secretRef.Name)
	}
	
	logger.Info("Retrieved secret data", "secret", secretRef.Name, "namespace", secretNamespace, "keys", len(secret.Data), "eso-managed", eso.isESOManaged(secret))
	return secret.Data, nil
}

// isESOManaged checks if a secret is managed by External Secrets Operator
func (eso *ESOIntegration) isESOManaged(secret *corev1.Secret) bool {
	// Check for ESO annotations and labels
	annotations := secret.GetAnnotations()
	labels := secret.GetLabels()
	
	// Common ESO annotations and labels
	esoIndicators := []string{
		"external-secrets.io/backend-type",
		"external-secrets.io/last-sync-time",
		"external-secrets.io/store-name",
	}
	
	for _, indicator := range esoIndicators {
		if _, exists := annotations[indicator]; exists {
			return true
		}
		if _, exists := labels[indicator]; exists {
			return true
		}
	}
	
	// Check for ESO owner references
	for _, ownerRef := range secret.GetOwnerReferences() {
		if ownerRef.APIVersion == "external-secrets.io/v1beta1" {
			return true
		}
	}
	
	return false
}

// waitForESOSecret waits for an ESO-managed secret to be populated with data
func (eso *ESOIntegration) waitForESOSecret(ctx context.Context, secretKey types.NamespacedName) error {
	logger := log.FromContext(ctx)
	logger.Info("Waiting for ESO-managed secret to be populated", "secret", secretKey.Name, "namespace", secretKey.Namespace)
	
	// Wait up to 30 seconds for the secret to be populated
	err := wait.PollImmediate(2*time.Second, 30*time.Second, func() (bool, error) {
		secret := &corev1.Secret{}
		err := eso.client.Get(ctx, secretKey, secret)
		if err != nil {
			logger.V(1).Info("Secret not found while waiting", "error", err)
			return false, nil // Continue waiting
		}
		
		// Check if secret has data
		if len(secret.Data) > 0 {
			logger.Info("ESO-managed secret is now populated", "secret", secretKey.Name, "keys", len(secret.Data))
			return true, nil
		}
		
		// Check ESO sync status from annotations
		if eso.checkESOSyncStatus(secret) {
			logger.Info("ESO sync completed but secret still empty", "secret", secretKey.Name)
			return true, nil // ESO has synced, even if empty
		}
		
		logger.V(1).Info("ESO-managed secret not yet populated, continuing to wait", "secret", secretKey.Name)
		return false, nil // Continue waiting
	})
	
	if err != nil {
		return fmt.Errorf("timeout waiting for ESO-managed secret to be populated: %w", err)
	}
	
	return nil
}

// checkESOSyncStatus checks the ESO sync status from secret annotations
func (eso *ESOIntegration) checkESOSyncStatus(secret *corev1.Secret) bool {
	annotations := secret.GetAnnotations()
	
	// Check for last sync time annotation
	if lastSync, exists := annotations["external-secrets.io/last-sync-time"]; exists && lastSync != "" {
		// Parse the sync time to ensure it's recent
		syncTime, err := time.Parse(time.RFC3339, lastSync)
		if err == nil {
			// Consider sync successful if it happened within the last 5 minutes
			if time.Since(syncTime) < 5*time.Minute {
				return true
			}
		}
	}
	
	// Check for sync status annotation
	if syncStatus, exists := annotations["external-secrets.io/sync-status"]; exists {
		return syncStatus == "success" || syncStatus == "Success"
	}
	
	return false
}

// WatchESOSecretUpdates sets up watching for ESO secret updates
// This can be used by controllers to react to secret changes
func (eso *ESOIntegration) WatchESOSecretUpdates(ctx context.Context, secretRef v1alpha1.SecretReference, namespace string, callback func(map[string][]byte) error) error {
	logger := log.FromContext(ctx)
	
	// Determine the namespace to use
	secretNamespace := secretRef.Namespace
	if secretNamespace == "" {
		secretNamespace = namespace
	}
	
	logger.Info("Setting up ESO secret update watch", "secret", secretRef.Name, "namespace", secretNamespace)
	
	// This is a simplified implementation - in a real controller, you would use
	// controller-runtime's watch functionality or informers
	// For now, we'll implement a polling mechanism
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	var lastResourceVersion string
	
	for {
		select {
		case <-ctx.Done():
			logger.Info("ESO secret watch cancelled", "secret", secretRef.Name)
			return ctx.Err()
		case <-ticker.C:
			secret := &corev1.Secret{}
			secretKey := types.NamespacedName{
				Name:      secretRef.Name,
				Namespace: secretNamespace,
			}
			
			err := eso.client.Get(ctx, secretKey, secret)
			if err != nil {
				logger.Error(err, "Failed to get secret during watch", "secret", secretRef.Name)
				continue
			}
			
			// Check if the secret has been updated
			if secret.ResourceVersion != lastResourceVersion {
				lastResourceVersion = secret.ResourceVersion
				logger.Info("Detected ESO secret update", "secret", secretRef.Name, "resourceVersion", lastResourceVersion)
				
				// Call the callback with updated secret data
				if len(secret.Data) > 0 {
					err = callback(secret.Data)
					if err != nil {
						logger.Error(err, "Callback failed for ESO secret update", "secret", secretRef.Name)
					}
				}
			}
		}
	}
}