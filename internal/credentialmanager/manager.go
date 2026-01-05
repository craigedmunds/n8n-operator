package credentialmanager

import (
	"context"
	"fmt"

	"github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	"github.com/jakub-k-slys/n8n-operator/internal/n8nclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CredentialManager defines the interface for managing n8n credentials
type CredentialManager interface {
	// ProcessCredentials processes all credentials for a workflow and creates them in n8n
	ProcessCredentials(ctx context.Context, workflow *v1alpha1.N8nWorkflow, n8nClient *n8nclient.Client) (map[string]string, error)
	
	// CreateN8nCredential creates a single credential in n8n
	CreateN8nCredential(ctx context.Context, n8nClient *n8nclient.Client, cred v1alpha1.CredentialSpec, secretData map[string][]byte) (string, error)
	
	// UpdateN8nCredential updates an existing credential in n8n
	UpdateN8nCredential(ctx context.Context, n8nClient *n8nclient.Client, credID string, cred v1alpha1.CredentialSpec, secretData map[string][]byte) error
	
	// DeleteN8nCredential deletes a credential from n8n
	DeleteN8nCredential(ctx context.Context, n8nClient *n8nclient.Client, credID string) error
	
	// GetSecretData retrieves secret data from Kubernetes
	GetSecretData(ctx context.Context, secretRef v1alpha1.SecretReference, namespace string) (map[string][]byte, error)
}

// Manager implements the CredentialManager interface
type Manager struct {
	client client.Client
}

// NewManager creates a new credential manager
func NewManager(client client.Client) CredentialManager {
	return &Manager{
		client: client,
	}
}

// ProcessCredentials processes all credentials for a workflow and creates them in n8n
func (m *Manager) ProcessCredentials(ctx context.Context, workflow *v1alpha1.N8nWorkflow, n8nClient *n8nclient.Client) (map[string]string, error) {
	logger := log.FromContext(ctx)
	credentialIDs := make(map[string]string)
	
	for _, credSpec := range workflow.Spec.Credentials {
		logger.Info("Processing credential", "name", credSpec.Name, "type", credSpec.Type)
		
		// Get secret data from Kubernetes
		secretData, err := m.GetSecretData(ctx, credSpec.SecretRef, workflow.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to get secret data for credential %s: %w", credSpec.Name, err)
		}
		
		// Create credential in n8n
		credID, err := m.CreateN8nCredential(ctx, n8nClient, credSpec, secretData)
		if err != nil {
			return nil, fmt.Errorf("failed to create credential %s in n8n: %w", credSpec.Name, err)
		}
		
		credentialIDs[credSpec.Name] = credID
		logger.Info("Successfully created credential", "name", credSpec.Name, "id", credID)
	}
	
	return credentialIDs, nil
}
// CreateN8nCredential creates a single credential in n8n
func (m *Manager) CreateN8nCredential(ctx context.Context, n8nClient *n8nclient.Client, cred v1alpha1.CredentialSpec, secretData map[string][]byte) (string, error) {
	logger := log.FromContext(ctx)
	
	// Transform secret data based on credential type
	credentialData, err := m.transformSecretData(cred.Type, secretData)
	if err != nil {
		return "", fmt.Errorf("failed to transform secret data for credential type %s: %w", cred.Type, err)
	}
	
	// Create credential definition for n8n API
	credDef := n8nclient.CredentialDefinition{
		Name: cred.Name,
		Type: cred.Type,
		Data: credentialData,
	}
	
	// Create credential in n8n
	response, err := n8nClient.CreateCredential(ctx, credDef)
	if err != nil {
		return "", fmt.Errorf("failed to create credential in n8n: %w", err)
	}
	
	logger.Info("Created credential in n8n", "name", cred.Name, "id", response.ID, "type", cred.Type)
	return response.ID, nil
}

// UpdateN8nCredential updates an existing credential in n8n
func (m *Manager) UpdateN8nCredential(ctx context.Context, n8nClient *n8nclient.Client, credID string, cred v1alpha1.CredentialSpec, secretData map[string][]byte) error {
	logger := log.FromContext(ctx)
	
	// Transform secret data based on credential type
	credentialData, err := m.transformSecretData(cred.Type, secretData)
	if err != nil {
		return fmt.Errorf("failed to transform secret data for credential type %s: %w", cred.Type, err)
	}
	
	// Create credential definition for n8n API
	credDef := n8nclient.CredentialDefinition{
		Name: cred.Name,
		Type: cred.Type,
		Data: credentialData,
	}
	
	// Update credential in n8n
	_, err = n8nClient.UpdateCredential(ctx, credID, credDef)
	if err != nil {
		return fmt.Errorf("failed to update credential in n8n: %w", err)
	}
	
	logger.Info("Updated credential in n8n", "name", cred.Name, "id", credID, "type", cred.Type)
	return nil
}

// DeleteN8nCredential deletes a credential from n8n
func (m *Manager) DeleteN8nCredential(ctx context.Context, n8nClient *n8nclient.Client, credID string) error {
	logger := log.FromContext(ctx)
	
	err := n8nClient.DeleteCredential(ctx, credID)
	if err != nil {
		return fmt.Errorf("failed to delete credential from n8n: %w", err)
	}
	
	logger.Info("Deleted credential from n8n", "id", credID)
	return nil
}

// GetSecretData retrieves secret data from Kubernetes
func (m *Manager) GetSecretData(ctx context.Context, secretRef v1alpha1.SecretReference, namespace string) (map[string][]byte, error) {
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
	
	err := m.client.Get(ctx, secretKey, secret)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", secretNamespace, secretRef.Name, err)
	}
	
	logger.Info("Retrieved secret data", "secret", secretRef.Name, "namespace", secretNamespace, "keys", len(secret.Data))
	return secret.Data, nil
}
// transformSecretData transforms Kubernetes secret data into n8n credential format
func (m *Manager) transformSecretData(credType string, secretData map[string][]byte) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	
	switch credType {
	case "httpBasicAuth":
		// HTTP Basic Auth expects 'user' and 'password' fields
		if user, exists := secretData["username"]; exists {
			result["user"] = string(user)
		} else if user, exists := secretData["user"]; exists {
			result["user"] = string(user)
		} else {
			return nil, fmt.Errorf("httpBasicAuth credential requires 'username' or 'user' field in secret")
		}
		
		if password, exists := secretData["password"]; exists {
			result["password"] = string(password)
		} else {
			return nil, fmt.Errorf("httpBasicAuth credential requires 'password' field in secret")
		}
		
	case "apiKey":
		// API Key expects 'apiKey' field
		if apiKey, exists := secretData["apiKey"]; exists {
			result["apiKey"] = string(apiKey)
		} else if apiKey, exists := secretData["api-key"]; exists {
			result["apiKey"] = string(apiKey)
		} else if apiKey, exists := secretData["key"]; exists {
			result["apiKey"] = string(apiKey)
		} else {
			return nil, fmt.Errorf("apiKey credential requires 'apiKey', 'api-key', or 'key' field in secret")
		}
		
	case "oauth2":
		// OAuth2 expects various fields depending on the flow
		if clientId, exists := secretData["clientId"]; exists {
			result["clientId"] = string(clientId)
		} else if clientId, exists := secretData["client_id"]; exists {
			result["clientId"] = string(clientId)
		}
		
		if clientSecret, exists := secretData["clientSecret"]; exists {
			result["clientSecret"] = string(clientSecret)
		} else if clientSecret, exists := secretData["client_secret"]; exists {
			result["clientSecret"] = string(clientSecret)
		}
		
		if accessToken, exists := secretData["accessToken"]; exists {
			result["accessToken"] = string(accessToken)
		} else if accessToken, exists := secretData["access_token"]; exists {
			result["accessToken"] = string(accessToken)
		}
		
		if refreshToken, exists := secretData["refreshToken"]; exists {
			result["refreshToken"] = string(refreshToken)
		} else if refreshToken, exists := secretData["refresh_token"]; exists {
			result["refreshToken"] = string(refreshToken)
		}
		
		// OAuth2 requires at least clientId and clientSecret or accessToken
		if result["clientId"] == nil && result["accessToken"] == nil {
			return nil, fmt.Errorf("oauth2 credential requires either 'clientId' or 'accessToken' field in secret")
		}
		
	case "httpHeaderAuth":
		// HTTP Header Auth expects 'name' and 'value' fields
		if name, exists := secretData["name"]; exists {
			result["name"] = string(name)
		} else if name, exists := secretData["header-name"]; exists {
			result["name"] = string(name)
		} else {
			return nil, fmt.Errorf("httpHeaderAuth credential requires 'name' or 'header-name' field in secret")
		}
		
		if value, exists := secretData["value"]; exists {
			result["value"] = string(value)
		} else if value, exists := secretData["header-value"]; exists {
			result["value"] = string(value)
		} else {
			return nil, fmt.Errorf("httpHeaderAuth credential requires 'value' or 'header-value' field in secret")
		}
		
	case "httpQueryAuth":
		// HTTP Query Auth expects 'name' and 'value' fields
		if name, exists := secretData["name"]; exists {
			result["name"] = string(name)
		} else if name, exists := secretData["query-name"]; exists {
			result["name"] = string(name)
		} else {
			return nil, fmt.Errorf("httpQueryAuth credential requires 'name' or 'query-name' field in secret")
		}
		
		if value, exists := secretData["value"]; exists {
			result["value"] = string(value)
		} else if value, exists := secretData["query-value"]; exists {
			result["value"] = string(value)
		} else {
			return nil, fmt.Errorf("httpQueryAuth credential requires 'value' or 'query-value' field in secret")
		}
		
	default:
		return nil, fmt.Errorf("unsupported credential type: %s", credType)
	}
	
	return result, nil
}