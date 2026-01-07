package credentialmanager

import (
	"context"
	"fmt"

	"github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	"github.com/jakub-k-slys/n8n-operator/internal/n8nclient"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CredentialLifecycleManager provides methods for managing credential lifecycle
type CredentialLifecycleManager struct {
	manager CredentialManager
}

// NewCredentialLifecycleManager creates a new credential lifecycle manager
func NewCredentialLifecycleManager(manager CredentialManager) *CredentialLifecycleManager {
	return &CredentialLifecycleManager{
		manager: manager,
	}
}

// SyncCredentials synchronizes credentials between Kubernetes secrets and n8n
// It handles creation, updates, and deletion of credentials as needed
func (clm *CredentialLifecycleManager) SyncCredentials(ctx context.Context, workflow *v1alpha1.N8nWorkflow, n8nClient *n8nclient.Client, existingCredentials map[string]string) (map[string]string, error) {
	logger := log.FromContext(ctx)
	logger.Info("Syncing credentials for workflow", "workflow", workflow.Name, "existing", len(existingCredentials), "desired", len(workflow.Spec.Credentials))

	updatedCredentials := make(map[string]string)

	// Process each credential specification
	for _, credSpec := range workflow.Spec.Credentials {
		existingCredID, exists := existingCredentials[credSpec.Name]

		// Get current secret data
		secretData, err := clm.manager.GetSecretData(ctx, credSpec.SecretRef, workflow.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to get secret data for credential %s: %w", credSpec.Name, err)
		}

		if exists {
			// Update existing credential
			logger.Info("Updating existing credential", "name", credSpec.Name, "id", existingCredID)
			err = clm.manager.UpdateN8nCredential(ctx, n8nClient, existingCredID, credSpec, secretData)
			if err != nil {
				return nil, fmt.Errorf("failed to update credential %s: %w", credSpec.Name, err)
			}
			updatedCredentials[credSpec.Name] = existingCredID
		} else {
			// Create new credential
			logger.Info("Creating new credential", "name", credSpec.Name)
			credID, err := clm.manager.CreateN8nCredential(ctx, n8nClient, credSpec, secretData)
			if err != nil {
				return nil, fmt.Errorf("failed to create credential %s: %w", credSpec.Name, err)
			}
			updatedCredentials[credSpec.Name] = credID
		}
	}

	// Delete credentials that are no longer needed
	for credName, credID := range existingCredentials {
		if _, stillNeeded := updatedCredentials[credName]; !stillNeeded {
			logger.Info("Deleting unused credential", "name", credName, "id", credID)
			err := clm.manager.DeleteN8nCredential(ctx, n8nClient, credID)
			if err != nil {
				logger.Error(err, "Failed to delete unused credential", "name", credName, "id", credID)
				// Continue with other credentials even if one fails to delete
			}
		}
	}

	logger.Info("Credential sync completed", "workflow", workflow.Name, "final", len(updatedCredentials))
	return updatedCredentials, nil
}

// CleanupCredentials removes all credentials associated with a workflow from n8n
func (clm *CredentialLifecycleManager) CleanupCredentials(ctx context.Context, n8nClient *n8nclient.Client, credentialIDs map[string]string) error {
	logger := log.FromContext(ctx)
	logger.Info("Cleaning up credentials", "count", len(credentialIDs))

	var lastError error
	for credName, credID := range credentialIDs {
		logger.Info("Deleting credential", "name", credName, "id", credID)
		err := clm.manager.DeleteN8nCredential(ctx, n8nClient, credID)
		if err != nil {
			logger.Error(err, "Failed to delete credential during cleanup", "name", credName, "id", credID)
			lastError = err
			// Continue with other credentials even if one fails
		}
	}

	if lastError != nil {
		return fmt.Errorf("some credentials failed to delete during cleanup: %w", lastError)
	}

	logger.Info("Credential cleanup completed successfully")
	return nil
}

// ValidateCredentialReferences validates that all credential references in workflow nodes are satisfied
func (clm *CredentialLifecycleManager) ValidateCredentialReferences(ctx context.Context, workflow *v1alpha1.N8nWorkflow, credentialIDs map[string]string) error {
	logger := log.FromContext(ctx)

	// Collect all credential references from workflow nodes
	referencedCredentials := make(map[string]bool)
	for _, node := range workflow.Spec.Workflow.Nodes {
		for _, credName := range node.Credentials {
			referencedCredentials[credName] = true
		}
	}

	// Check that all referenced credentials are available
	for credName := range referencedCredentials {
		if _, exists := credentialIDs[credName]; !exists {
			return fmt.Errorf("workflow node references credential '%s' but it is not defined in the credential specifications", credName)
		}
	}

	logger.Info("Credential reference validation passed", "workflow", workflow.Name, "referenced", len(referencedCredentials))
	return nil
}
