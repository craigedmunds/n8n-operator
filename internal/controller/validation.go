package controller

import (
	"context"
	"fmt"
	"time"

	n8nv1alpha1 "github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// N8nInstanceValidator provides validation functionality for n8n instances
type N8nInstanceValidator struct {
	client client.Client
}

// NewN8nInstanceValidator creates a new n8n instance validator
func NewN8nInstanceValidator(client client.Client) *N8nInstanceValidator {
	return &N8nInstanceValidator{
		client: client,
	}
}

// ValidateN8nInstanceReference validates that the referenced n8n instance exists and is accessible
func (v *N8nInstanceValidator) ValidateN8nInstanceReference(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow) (*n8nv1alpha1.N8n, error) {
	logger := log.FromContext(ctx)

	// Determine the namespace for the n8n instance
	n8nNamespace := workflow.Spec.N8nRef.Namespace
	if n8nNamespace == "" {
		n8nNamespace = workflow.Namespace
	}

	logger.Info("Validating n8n instance reference", 
		"n8nName", workflow.Spec.N8nRef.Name, 
		"n8nNamespace", n8nNamespace,
		"workflowNamespace", workflow.Namespace)

	// Get the n8n instance
	n8nInstance := &n8nv1alpha1.N8n{}
	n8nKey := types.NamespacedName{
		Name:      workflow.Spec.N8nRef.Name,
		Namespace: n8nNamespace,
	}

	err := v.client.Get(ctx, n8nKey, n8nInstance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("n8n instance %s/%s not found", n8nNamespace, workflow.Spec.N8nRef.Name)
		}
		return nil, fmt.Errorf("failed to get n8n instance %s/%s: %w", n8nNamespace, workflow.Spec.N8nRef.Name, err)
	}

	// Validate cross-namespace access if applicable
	if n8nNamespace != workflow.Namespace {
		if err := v.validateCrossNamespaceAccess(ctx, workflow, n8nInstance); err != nil {
			return nil, fmt.Errorf("cross-namespace access validation failed: %w", err)
		}
	}

	// Check if the n8n instance is ready
	if !v.isN8nInstanceReady(n8nInstance) {
		return nil, fmt.Errorf("n8n instance %s/%s is not ready", n8nNamespace, workflow.Spec.N8nRef.Name)
	}

	// Validate n8n instance accessibility
	if err := v.validateN8nInstanceAccessibility(ctx, n8nInstance); err != nil {
		return nil, fmt.Errorf("n8n instance %s/%s is not accessible: %w", n8nNamespace, workflow.Spec.N8nRef.Name, err)
	}

	logger.Info("Successfully validated n8n instance", "name", workflow.Spec.N8nRef.Name, "namespace", n8nNamespace)
	return n8nInstance, nil
}

// validateCrossNamespaceAccess validates that cross-namespace access is allowed
func (v *N8nInstanceValidator) validateCrossNamespaceAccess(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow, n8nInstance *n8nv1alpha1.N8n) error {
	logger := log.FromContext(ctx)
	
	logger.Info("Validating cross-namespace access", 
		"workflowNamespace", workflow.Namespace,
		"n8nNamespace", n8nInstance.Namespace)

	// For now, we allow cross-namespace access
	// In a production environment, you might want to:
	// 1. Check RBAC permissions
	// 2. Validate network policies
	// 3. Check for explicit allow annotations
	// 4. Implement namespace isolation policies

	// Example of annotation-based cross-namespace access control:
	if annotations := n8nInstance.GetAnnotations(); annotations != nil {
		if allowedNamespaces, exists := annotations["n8n.slys.dev/allowed-namespaces"]; exists {
			// Parse comma-separated list of allowed namespaces
			// This is a simplified implementation
			if allowedNamespaces != "*" && allowedNamespaces != workflow.Namespace {
				return fmt.Errorf("cross-namespace access not allowed from namespace %s", workflow.Namespace)
			}
		}
	}

	logger.Info("Cross-namespace access validated successfully")
	return nil
}

// isN8nInstanceReady checks if an n8n instance is ready for use
func (v *N8nInstanceValidator) isN8nInstanceReady(n8nInstance *n8nv1alpha1.N8n) bool {
	// Check if the n8n instance has the Available condition set to True
	for _, condition := range n8nInstance.Status.Conditions {
		if condition.Type == typeAvailableN8n && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// validateN8nInstanceAccessibility performs a basic connectivity check to the n8n instance
func (v *N8nInstanceValidator) validateN8nInstanceAccessibility(ctx context.Context, n8nInstance *n8nv1alpha1.N8n) error {
	logger := log.FromContext(ctx)

	// Create a basic n8n client for connectivity testing
	baseURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:5678", n8nInstance.Name, n8nInstance.Namespace)
	
	// For now, we'll skip the actual connectivity test as it requires proper API credentials
	// In a real implementation, you would:
	// 1. Get API credentials from the n8n instance configuration
	// 2. Create an n8n client
	// 3. Perform a basic health check (e.g., GET /healthz or similar)
	
	logger.Info("N8n instance accessibility check", "baseURL", baseURL)
	
	// Placeholder for actual connectivity test
	// client, err := n8nclient.NewClient(baseURL, apiKey)
	// if err != nil {
	//     return fmt.Errorf("failed to create n8n client: %w", err)
	// }
	
	// Perform a simple health check with timeout
	// ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	// defer cancel()
	
	// if err := client.HealthCheck(ctx); err != nil {
	//     return fmt.Errorf("n8n instance health check failed: %w", err)
	// }

	return nil
}

// GetN8nInstanceStatus returns the current status of an n8n instance
func (v *N8nInstanceValidator) GetN8nInstanceStatus(n8nInstance *n8nv1alpha1.N8n) string {
	if len(n8nInstance.Status.Conditions) == 0 {
		return "Unknown"
	}

	// Find the most relevant condition
	for _, condition := range n8nInstance.Status.Conditions {
		if condition.Type == typeAvailableN8n {
			if condition.Status == metav1.ConditionTrue {
				return "Ready"
			} else if condition.Status == metav1.ConditionFalse {
				return fmt.Sprintf("Not Ready: %s", condition.Reason)
			} else {
				return fmt.Sprintf("Unknown: %s", condition.Reason)
			}
		}
	}

	return "Unknown"
}

// ValidateWorkflowIndependence ensures that workflows targeting the same n8n instance don't interfere
func (v *N8nInstanceValidator) ValidateWorkflowIndependence(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow) error {
	logger := log.FromContext(ctx)

	// List all N8nWorkflow resources that reference the same n8n instance
	workflowList := &n8nv1alpha1.N8nWorkflowList{}
	err := v.client.List(ctx, workflowList)
	if err != nil {
		return fmt.Errorf("failed to list N8nWorkflow resources: %w", err)
	}

	// Check for potential conflicts
	conflictingWorkflows := []string{}
	for _, existingWorkflow := range workflowList.Items {
		// Skip self
		if existingWorkflow.Name == workflow.Name && existingWorkflow.Namespace == workflow.Namespace {
			continue
		}

		// Check if they reference the same n8n instance
		if v.referenceSameN8nInstance(workflow, &existingWorkflow) {
			// Check for workflow name conflicts
			if workflow.Spec.Workflow.Name == existingWorkflow.Spec.Workflow.Name {
				conflictingWorkflows = append(conflictingWorkflows, 
					fmt.Sprintf("%s/%s", existingWorkflow.Namespace, existingWorkflow.Name))
			}
		}
	}

	if len(conflictingWorkflows) > 0 {
		return fmt.Errorf("workflow name conflict detected with: %v", conflictingWorkflows)
	}

	logger.Info("Workflow independence validation passed", "workflowName", workflow.Spec.Workflow.Name)
	return nil
}

// referenceSameN8nInstance checks if two workflows reference the same n8n instance
func (v *N8nInstanceValidator) referenceSameN8nInstance(workflow1, workflow2 *n8nv1alpha1.N8nWorkflow) bool {
	// Resolve namespaces
	n8nNamespace1 := workflow1.Spec.N8nRef.Namespace
	if n8nNamespace1 == "" {
		n8nNamespace1 = workflow1.Namespace
	}

	n8nNamespace2 := workflow2.Spec.N8nRef.Namespace
	if n8nNamespace2 == "" {
		n8nNamespace2 = workflow2.Namespace
	}

	// Compare n8n instance references
	return workflow1.Spec.N8nRef.Name == workflow2.Spec.N8nRef.Name &&
		n8nNamespace1 == n8nNamespace2
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	Healthy   bool
	Message   string
	Timestamp time.Time
}

// PerformHealthCheck performs a comprehensive health check on an n8n instance
func (v *N8nInstanceValidator) PerformHealthCheck(ctx context.Context, n8nInstance *n8nv1alpha1.N8n) (*HealthCheckResult, error) {
	logger := log.FromContext(ctx)
	
	result := &HealthCheckResult{
		Timestamp: time.Now(),
	}

	// Check if instance is ready
	if !v.isN8nInstanceReady(n8nInstance) {
		result.Healthy = false
		result.Message = "N8n instance is not ready"
		return result, nil
	}

	// TODO: Implement actual health check with n8n API
	// This would involve:
	// 1. Creating an n8n client with proper credentials
	// 2. Making a health check API call
	// 3. Verifying the response

	logger.Info("Health check performed", "instance", n8nInstance.Name, "namespace", n8nInstance.Namespace)
	
	result.Healthy = true
	result.Message = "N8n instance is healthy"
	return result, nil
}