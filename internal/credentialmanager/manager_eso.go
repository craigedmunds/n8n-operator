package credentialmanager

import (
	"context"
	"fmt"

	"github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	"github.com/jakub-k-slys/n8n-operator/internal/n8nclient"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ESOAwareManager extends the basic Manager with ESO support
type ESOAwareManager struct {
	*Manager
	esoIntegration *ESOIntegration
}

// NewESOAwareManager creates a new credential manager with ESO support
func NewESOAwareManager(client client.Client) CredentialManager {
	return &ESOAwareManager{
		Manager:        &Manager{client: client},
		esoIntegration: NewESOIntegration(client),
	}
}

// GetSecretData retrieves secret data from Kubernetes with ESO support
// This overrides the base Manager's GetSecretData method
func (m *ESOAwareManager) GetSecretData(ctx context.Context, secretRef v1alpha1.SecretReference, namespace string) (map[string][]byte, error) {
	logger := log.FromContext(ctx)
	logger.Info("Getting secret data with ESO support", "secret", secretRef.Name, "namespace", secretRef.Namespace)
	
	// Use ESO-aware secret retrieval
	return m.esoIntegration.GetSecretDataWithESO(ctx, secretRef, namespace)
}

// ProcessCredentialsWithESO processes all credentials for a workflow with ESO support
// This provides enhanced credential processing with ESO integration
func (m *ESOAwareManager) ProcessCredentialsWithESO(ctx context.Context, workflow *v1alpha1.N8nWorkflow, n8nClient *n8nclient.Client) (map[string]string, error) {
	logger := log.FromContext(ctx)
	logger.Info("Processing credentials with ESO support", "workflow", workflow.Name, "credentials", len(workflow.Spec.Credentials))
	
	credentialIDs := make(map[string]string)
	
	for _, credSpec := range workflow.Spec.Credentials {
		logger.Info("Processing credential with ESO support", "name", credSpec.Name, "type", credSpec.Type)
		
		// Get secret data using ESO-aware method
		secretData, err := m.GetSecretData(ctx, credSpec.SecretRef, workflow.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to get ESO secret data for credential %s: %w", credSpec.Name, err)
		}
		
		// Validate that we have the required data
		if len(secretData) == 0 {
			return nil, fmt.Errorf("ESO secret %s/%s is empty", credSpec.SecretRef.Namespace, credSpec.SecretRef.Name)
		}
		
		// Create credential in n8n (using the base manager's method)
		credID, err := m.Manager.CreateN8nCredential(ctx, n8nClient, credSpec, secretData)
		if err != nil {
			return nil, fmt.Errorf("failed to create credential %s in n8n with ESO data: %w", credSpec.Name, err)
		}
		
		credentialIDs[credSpec.Name] = credID
		logger.Info("Successfully created credential with ESO data", "name", credSpec.Name, "id", credID)
	}
	
	return credentialIDs, nil
}

// SetupESOSecretWatching sets up watching for ESO secret updates
// This allows the credential manager to react to secret changes automatically
func (m *ESOAwareManager) SetupESOSecretWatching(ctx context.Context, workflow *v1alpha1.N8nWorkflow, updateCallback func(credName string, secretData map[string][]byte) error) error {
	logger := log.FromContext(ctx)
	logger.Info("Setting up ESO secret watching", "workflow", workflow.Name, "credentials", len(workflow.Spec.Credentials))
	
	for _, credSpec := range workflow.Spec.Credentials {
		// Create a callback for this specific credential
		credName := credSpec.Name
		callback := func(secretData map[string][]byte) error {
			logger.Info("ESO secret updated, triggering credential update", "credential", credName)
			return updateCallback(credName, secretData)
		}
		
		// Start watching this secret in a goroutine
		go func(cred v1alpha1.CredentialSpec) {
			err := m.esoIntegration.WatchESOSecretUpdates(ctx, cred.SecretRef, workflow.Namespace, callback)
			if err != nil && err != context.Canceled {
				logger.Error(err, "ESO secret watch failed", "credential", cred.Name)
			}
		}(credSpec)
	}
	
	return nil
}

// ValidateESOSecrets validates that all ESO secrets are properly configured and accessible
func (m *ESOAwareManager) ValidateESOSecrets(ctx context.Context, workflow *v1alpha1.N8nWorkflow) error {
	logger := log.FromContext(ctx)
	logger.Info("Validating ESO secrets", "workflow", workflow.Name, "credentials", len(workflow.Spec.Credentials))
	
	for _, credSpec := range workflow.Spec.Credentials {
		// Try to get the secret data to validate it's accessible
		secretData, err := m.GetSecretData(ctx, credSpec.SecretRef, workflow.Namespace)
		if err != nil {
			return fmt.Errorf("ESO secret validation failed for credential %s: %w", credSpec.Name, err)
		}
		
		// Validate that the secret has the required fields for the credential type
		err = m.validateSecretDataForCredentialType(credSpec.Type, secretData)
		if err != nil {
			return fmt.Errorf("ESO secret %s does not contain required fields for credential type %s: %w", credSpec.SecretRef.Name, credSpec.Type, err)
		}
		
		logger.Info("ESO secret validation passed", "credential", credSpec.Name, "type", credSpec.Type)
	}
	
	return nil
}

// validateSecretDataForCredentialType validates that secret data contains required fields
func (m *ESOAwareManager) validateSecretDataForCredentialType(credType string, secretData map[string][]byte) error {
	switch credType {
	case "httpBasicAuth":
		if _, hasUser := secretData["username"]; !hasUser {
			if _, hasUser := secretData["user"]; !hasUser {
				return fmt.Errorf("httpBasicAuth requires 'username' or 'user' field")
			}
		}
		if _, hasPassword := secretData["password"]; !hasPassword {
			return fmt.Errorf("httpBasicAuth requires 'password' field")
		}
		
	case "apiKey":
		if _, hasKey := secretData["apiKey"]; !hasKey {
			if _, hasKey := secretData["api-key"]; !hasKey {
				if _, hasKey := secretData["key"]; !hasKey {
					return fmt.Errorf("apiKey requires 'apiKey', 'api-key', or 'key' field")
				}
			}
		}
		
	case "oauth2":
		hasClientId := false
		hasAccessToken := false
		
		if _, exists := secretData["clientId"]; exists {
			hasClientId = true
		} else if _, exists := secretData["client_id"]; exists {
			hasClientId = true
		}
		
		if _, exists := secretData["accessToken"]; exists {
			hasAccessToken = true
		} else if _, exists := secretData["access_token"]; exists {
			hasAccessToken = true
		}
		
		if !hasClientId && !hasAccessToken {
			return fmt.Errorf("oauth2 requires either 'clientId' or 'accessToken' field")
		}
		
	case "httpHeaderAuth", "httpQueryAuth":
		if _, hasName := secretData["name"]; !hasName {
			if _, hasName := secretData["header-name"]; !hasName && credType == "httpHeaderAuth" {
				return fmt.Errorf("%s requires 'name' or 'header-name' field", credType)
			}
			if _, hasName := secretData["query-name"]; !hasName && credType == "httpQueryAuth" {
				return fmt.Errorf("%s requires 'name' or 'query-name' field", credType)
			}
		}
		if _, hasValue := secretData["value"]; !hasValue {
			if _, hasValue := secretData["header-value"]; !hasValue && credType == "httpHeaderAuth" {
				return fmt.Errorf("%s requires 'value' or 'header-value' field", credType)
			}
			if _, hasValue := secretData["query-value"]; !hasValue && credType == "httpQueryAuth" {
				return fmt.Errorf("%s requires 'value' or 'query-value' field", credType)
			}
		}
		
	default:
		return fmt.Errorf("unsupported credential type: %s", credType)
	}
	
	return nil
}