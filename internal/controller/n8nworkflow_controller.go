package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	n8nv1alpha1 "github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	"github.com/jakub-k-slys/n8n-operator/internal/credentialmanager"
	"github.com/jakub-k-slys/n8n-operator/internal/n8nclient"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	n8nWorkflowFinalizer     = "n8nworkflow.slys.dev/finalizer"
	typeAvailableN8nWorkflow = "Available"
	typeDegradedN8nWorkflow  = "Degraded"
	typeReadyN8nWorkflow     = "Ready"
	
	// Sync status values
	syncStatusSynced   = "Synced"
	syncStatusSyncing  = "Syncing"
	syncStatusFailed   = "Failed"
	syncStatusUnknown  = "Unknown"
)

// N8nWorkflowReconciler reconciles a N8nWorkflow object
type N8nWorkflowReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Recorder          record.EventRecorder
	CredentialManager credentialmanager.CredentialManager
	Validator         *N8nInstanceValidator
}

// +kubebuilder:rbac:groups=n8n.slys.dev,resources=n8nworkflows,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=n8n.slys.dev,resources=n8nworkflows/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=n8n.slys.dev,resources=n8nworkflows/finalizers,verbs=update
// +kubebuilder:rbac:groups=n8n.slys.dev,resources=n8ns,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *N8nWorkflowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the N8nWorkflow instance
	workflow := &n8nv1alpha1.N8nWorkflow{}
	err := r.Get(ctx, req.NamespacedName, workflow)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("N8nWorkflow resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get N8nWorkflow")
		return ctrl.Result{}, err
	}

	// Initialize status conditions if not set
	if workflow.Status.Conditions == nil || len(workflow.Status.Conditions) == 0 {
		update := StatusUpdate{
			SyncStatus:      syncStatusSyncing,
			ConditionType:   typeAvailableN8nWorkflow,
			ConditionStatus: metav1.ConditionUnknown,
			Reason:          "Reconciling",
			Message:         "Starting reconciliation",
		}
		if err = r.updateComprehensiveStatus(ctx, workflow, update); err != nil {
			logger.Error(err, "Failed to update N8nWorkflow status")
			return ctrl.Result{Requeue: true}, nil
		}
		// Re-fetch after status update
		if err := r.Get(ctx, req.NamespacedName, workflow); err != nil {
			logger.Error(err, "Failed to re-fetch N8nWorkflow")
			return ctrl.Result{}, err
		}
	}

	// Handle finalizer
	if !controllerutil.ContainsFinalizer(workflow, n8nWorkflowFinalizer) {
		logger.Info("Adding Finalizer for N8nWorkflow")
		if ok := controllerutil.AddFinalizer(workflow, n8nWorkflowFinalizer); !ok {
			logger.Error(err, "Failed to add finalizer into the custom resource")
			return ctrl.Result{Requeue: true}, nil
		}
		if err = r.Update(ctx, workflow); err != nil {
			logger.Error(err, "Failed to update custom resource to add finalizer")
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Handle deletion
	if workflow.GetDeletionTimestamp() != nil {
		return r.handleDeletion(ctx, workflow)
	}

	// Validate n8n instance reference
	n8nInstance, err := r.Validator.ValidateN8nInstanceReference(ctx, workflow)
	if err != nil {
		logger.Error(err, "Failed to validate n8n instance")
		update := StatusUpdate{
			SyncStatus:      syncStatusFailed,
			ErrorMessage:    err.Error(),
			ConditionType:   typeDegradedN8nWorkflow,
			ConditionStatus: metav1.ConditionTrue,
			Reason:          "N8nInstanceNotFound",
			Message:         err.Error(),
		}
		if err = r.updateComprehensiveStatus(ctx, workflow, update); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
	}

	// Validate workflow independence
	if err := r.Validator.ValidateWorkflowIndependence(ctx, workflow); err != nil {
		logger.Error(err, "Workflow independence validation failed")
		update := StatusUpdate{
			SyncStatus:      syncStatusFailed,
			ErrorMessage:    err.Error(),
			ConditionType:   typeDegradedN8nWorkflow,
			ConditionStatus: metav1.ConditionTrue,
			Reason:          "WorkflowConflict",
			Message:         err.Error(),
		}
		if err = r.updateComprehensiveStatus(ctx, workflow, update); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
	}

	// Create n8n client
	n8nClient, err := r.createN8nClient(ctx, n8nInstance)
	if err != nil {
		logger.Error(err, "Failed to create n8n client")
		update := StatusUpdate{
			SyncStatus:      syncStatusFailed,
			ErrorMessage:    err.Error(),
			ConditionType:   typeDegradedN8nWorkflow,
			ConditionStatus: metav1.ConditionTrue,
			Reason:          "N8nClientError",
			Message:         err.Error(),
		}
		if err = r.updateComprehensiveStatus(ctx, workflow, update); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
	}

	// Process credentials
	credentialIDs, err := r.CredentialManager.ProcessCredentials(ctx, workflow, n8nClient)
	if err != nil {
		logger.Error(err, "Failed to process credentials")
		credentialsSynced := false
		update := StatusUpdate{
			SyncStatus:        syncStatusFailed,
			ErrorMessage:      err.Error(),
			CredentialsSynced: &credentialsSynced,
			ConditionType:     typeDegradedN8nWorkflow,
			ConditionStatus:   metav1.ConditionTrue,
			Reason:            "CredentialProcessingFailed",
			Message:           err.Error(),
		}
		if err = r.updateComprehensiveStatus(ctx, workflow, update); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
	}

	// Mark credentials as synced
	credentialsSynced := true

	// Sync workflow with n8n
	workflowID, err := r.syncWorkflow(ctx, workflow, n8nClient, credentialIDs)
	if err != nil {
		logger.Error(err, "Failed to sync workflow")
		update := StatusUpdate{
			SyncStatus:        syncStatusFailed,
			ErrorMessage:      err.Error(),
			CredentialsSynced: &credentialsSynced,
			CredentialIDs:     credentialIDs,
			ConditionType:     typeDegradedN8nWorkflow,
			ConditionStatus:   metav1.ConditionTrue,
			Reason:            "WorkflowSyncFailed",
			Message:           err.Error(),
		}
		if err = r.updateComprehensiveStatus(ctx, workflow, update); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
	}

	// Verify the actual activation state from n8n
	actualWorkflow, err := n8nClient.GetWorkflow(ctx, workflowID)
	if err != nil {
		logger.Error(err, "Failed to verify workflow state after sync", "workflowID", workflowID)
		// Don't fail the reconciliation, but log the issue
		r.Recorder.Event(workflow, "Warning", "VerificationFailed",
			fmt.Sprintf("Failed to verify workflow state after sync: %v", err))
		// Update status with what we expect, but note the verification failure
		active := workflow.Spec.Workflow.Active
		statusMessage := fmt.Sprintf("Workflow synced with n8n (ID: %s, Active: %t) but verification failed", workflowID, active)
		update := StatusUpdate{
			SyncStatus:        syncStatusSynced,
			ErrorMessage:      fmt.Sprintf("Verification failed: %v", err),
			CredentialsSynced: &credentialsSynced,
			CredentialIDs:     credentialIDs,
			WorkflowID:        workflowID,
			Active:            &active,
			ConditionType:     typeAvailableN8nWorkflow,
			ConditionStatus:   metav1.ConditionTrue,
			Reason:            "ReconciledWithWarning",
			Message:           statusMessage,
		}
		if err = r.updateComprehensiveStatus(ctx, workflow, update); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// Update status with actual state from n8n
		actualActive := actualWorkflow.Active
		if actualActive != workflow.Spec.Workflow.Active {
			logger.Info("Workflow activation state mismatch detected", 
				"workflowID", workflowID, 
				"desired", workflow.Spec.Workflow.Active, 
				"actual", actualActive)
		}

		statusMessage := fmt.Sprintf("Workflow successfully synced with n8n (ID: %s, Active: %t)", workflowID, actualActive)
		update := StatusUpdate{
			SyncStatus:        syncStatusSynced,
			ErrorMessage:      "", // Clear any previous error
			CredentialsSynced: &credentialsSynced,
			CredentialIDs:     credentialIDs,
			WorkflowID:        workflowID,
			Active:            &actualActive,
			ConditionType:     typeAvailableN8nWorkflow,
			ConditionStatus:   metav1.ConditionTrue,
			Reason:            "Reconciled",
			Message:           statusMessage,
		}
		if err = r.updateComprehensiveStatus(ctx, workflow, update); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("Successfully reconciled N8nWorkflow", "workflowID", workflowID, "active", workflow.Status.Active, "credentialsSynced", credentialsSynced)
	
	// Perform health check if we have a workflow ID
	if workflowID != "" {
		if err := r.performHealthCheck(ctx, workflow, n8nClient, workflowID); err != nil {
			logger.Error(err, "Health check failed", "workflowID", workflowID)
			// Don't fail reconciliation for health check failures, just log and record event
			r.Recorder.Event(workflow, "Warning", "HealthCheckFailed",
				fmt.Sprintf("Health check failed for workflow %s: %v", workflowID, err))
		}
	}
	
	return ctrl.Result{RequeueAfter: time.Minute * 10}, nil // Requeue for periodic health checks
}

// handleDeletion handles the deletion of a N8nWorkflow resource
func (r *N8nWorkflowReconciler) handleDeletion(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(workflow, n8nWorkflowFinalizer) {
		if err := r.updateStatus(ctx, workflow, typeDegradedN8nWorkflow, metav1.ConditionUnknown, "Finalizing",
			fmt.Sprintf("Performing finalizer operations for the custom resource: %s", workflow.Name)); err != nil {
			return ctrl.Result{}, err
		}

		// Perform cleanup operations
		if err := r.doFinalizerOperationsForN8nWorkflow(ctx, workflow); err != nil {
			logger.Error(err, "Failed to perform finalizer operations")
			return ctrl.Result{RequeueAfter: time.Minute * 2}, nil
		}

		if err := r.updateStatus(ctx, workflow, typeDegradedN8nWorkflow, metav1.ConditionTrue, "Finalizing",
			fmt.Sprintf("Finalizer operations for custom resource %s were successfully accomplished", workflow.Name)); err != nil {
			return ctrl.Result{}, err
		}

		if ok := controllerutil.RemoveFinalizer(workflow, n8nWorkflowFinalizer); !ok {
			logger.Error(nil, "Failed to remove finalizer for N8nWorkflow")
			return ctrl.Result{Requeue: true}, nil
		}
		if err := r.Update(ctx, workflow); err != nil {
			logger.Error(err, "Failed to remove finalizer for N8nWorkflow")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// doFinalizerOperationsForN8nWorkflow performs cleanup operations when a workflow is deleted
func (r *N8nWorkflowReconciler) doFinalizerOperationsForN8nWorkflow(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow) error {
	logger := log.FromContext(ctx)

	// Get n8n instance to create client (needed for both workflow and credential deletion)
	n8nInstance, err := r.Validator.ValidateN8nInstanceReference(ctx, workflow)
	if err != nil {
		logger.Info("N8n instance not found during cleanup, skipping deletion", "error", err)
		r.Recorder.Event(workflow, "Warning", "CleanupSkipped",
			fmt.Sprintf("N8n instance not found during cleanup: %v", err))
		return nil // Don't fail cleanup if n8n instance is gone
	}

	n8nClient, err := r.createN8nClient(ctx, n8nInstance)
	if err != nil {
		logger.Info("Failed to create n8n client during cleanup, skipping deletion", "error", err)
		r.Recorder.Event(workflow, "Warning", "CleanupSkipped",
			fmt.Sprintf("Failed to create n8n client during cleanup: %v", err))
		return nil // Don't fail cleanup if we can't connect to n8n
	}

	// Delete credentials from n8n
	if workflow.Status.CredentialIDs != nil && len(workflow.Status.CredentialIDs) > 0 {
		logger.Info("Deleting credentials from n8n", "count", len(workflow.Status.CredentialIDs))
		for credName, credID := range workflow.Status.CredentialIDs {
			err := n8nClient.DeleteCredential(ctx, credID)
			if err != nil {
				// Check if it's a "not found" error, which is acceptable during cleanup
				if apiErr, ok := err.(*n8nclient.APIError); ok && apiErr.IsNotFound() {
					logger.Info("Credential already deleted from n8n", "name", credName, "credentialID", credID)
					r.Recorder.Event(workflow, "Normal", "CredentialAlreadyDeleted",
						fmt.Sprintf("Credential %s (%s) was already deleted from n8n", credName, credID))
				} else {
					logger.Error(err, "Failed to delete credential from n8n during cleanup", "name", credName, "credentialID", credID)
					r.Recorder.Event(workflow, "Warning", "CredentialDeletionFailed",
						fmt.Sprintf("Failed to delete credential %s (%s) from n8n: %v", credName, credID, err))
					// Continue with other credentials even if one fails
				}
			} else {
				logger.Info("Successfully deleted credential from n8n", "name", credName, "credentialID", credID)
				r.Recorder.Event(workflow, "Normal", "CredentialDeleted",
					fmt.Sprintf("Successfully deleted credential %s (%s) from n8n", credName, credID))
			}
		}
	}

	// Delete workflow from n8n
	if workflow.Status.WorkflowID != "" {
		err = n8nClient.DeleteWorkflow(ctx, workflow.Status.WorkflowID)
		if err != nil {
			// Check if it's a "not found" error, which is acceptable during cleanup
			if apiErr, ok := err.(*n8nclient.APIError); ok && apiErr.IsNotFound() {
				logger.Info("Workflow already deleted from n8n", "workflowID", workflow.Status.WorkflowID)
				r.Recorder.Event(workflow, "Normal", "AlreadyDeleted",
					fmt.Sprintf("Workflow %s was already deleted from n8n", workflow.Status.WorkflowID))
			} else {
				logger.Error(err, "Failed to delete workflow from n8n during cleanup", "workflowID", workflow.Status.WorkflowID)
				r.Recorder.Event(workflow, "Warning", "DeletionFailed",
					fmt.Sprintf("Failed to delete workflow %s from n8n: %v", workflow.Status.WorkflowID, err))
				
				// For cleanup operations, we should be more lenient
				// Only fail if it's a configuration error that indicates we should retry
				if isConfigurationError(err) {
					return fmt.Errorf("configuration error during workflow deletion: %w", err)
				}
				
				// For other errors (network, transient), log but don't fail cleanup
				logger.Info("Ignoring error during cleanup to prevent resource from being stuck", 
					"workflowID", workflow.Status.WorkflowID, "error", err)
			}
		} else {
			logger.Info("Successfully deleted workflow from n8n", "workflowID", workflow.Status.WorkflowID)
			r.Recorder.Event(workflow, "Normal", "Deleted",
				fmt.Sprintf("Successfully deleted workflow %s from n8n", workflow.Status.WorkflowID))
		}
	}

	r.Recorder.Event(workflow, "Normal", "Finalizing",
		fmt.Sprintf("Custom Resource %s is being deleted from the namespace %s",
			workflow.Name, workflow.Namespace))

	return nil
}

// createN8nClient creates an n8n API client for the given n8n instance
func (r *N8nWorkflowReconciler) createN8nClient(ctx context.Context, n8nInstance *n8nv1alpha1.N8n) (*n8nclient.Client, error) {
	logger := log.FromContext(ctx)
	
	// Get the API credentials secret name from the N8n instance status
	if n8nInstance.Status.APICredentialsSecretName == "" {
		return nil, fmt.Errorf("n8n instance %s/%s does not have API credentials secret configured", 
			n8nInstance.Namespace, n8nInstance.Name)
	}
	
	// Retrieve the API credentials secret
	secretName := n8nInstance.Status.APICredentialsSecretName
	secret := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{
		Name:      secretName,
		Namespace: n8nInstance.Namespace,
	}, secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("API credentials secret %s not found in namespace %s", 
				secretName, n8nInstance.Namespace)
		}
		return nil, fmt.Errorf("failed to retrieve API credentials secret: %w", err)
	}
	
	// Extract the API key from the secret
	apiKeyBytes, ok := secret.Data["apiKey"]
	if !ok {
		return nil, fmt.Errorf("API credentials secret %s does not contain 'apiKey' field", secretName)
	}
	apiKey := string(apiKeyBytes)
	
	if apiKey == "" {
		return nil, fmt.Errorf("API key in secret %s is empty", secretName)
	}
	
	// Construct the base URL for the n8n instance
	// Use port 80 which is the standard HTTP port exposed by the n8n service
	baseURL := fmt.Sprintf("http://%s.%s.svc.cluster.local", n8nInstance.Name, n8nInstance.Namespace)
	
	logger.Info("Creating n8n client", 
		"baseURL", baseURL, 
		"secretName", secretName,
		"namespace", n8nInstance.Namespace)
	
	return n8nclient.NewClient(baseURL, apiKey)
}

// syncWorkflow synchronizes the workflow definition with n8n
func (r *N8nWorkflowReconciler) syncWorkflow(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow, n8nClient *n8nclient.Client, credentialIDs map[string]string) (string, error) {
	logger := log.FromContext(ctx)

	// Convert the workflow definition to n8n format
	workflowDef, err := r.ConvertWorkflowDefinition(workflow, credentialIDs)
	if err != nil {
		return "", fmt.Errorf("failed to convert workflow definition: %w", err)
	}

	// Log the workflow definition for debugging
	logger.Info("Converted workflow definition", 
		"nodeCount", len(workflowDef.Nodes),
		"hasConnections", workflowDef.Connections != nil,
		"hasSettings", workflowDef.Settings != nil)

	var workflowID string
	var response *n8nclient.WorkflowResponse

	if workflow.Status.WorkflowID == "" {
		// Create new workflow
		logger.Info("Creating new workflow in n8n", "name", workflow.Spec.Workflow.Name)
		response, err = n8nClient.CreateWorkflow(ctx, *workflowDef)
		if err != nil {
			return "", fmt.Errorf("failed to create workflow in n8n: %w", err)
		}
		workflowID = response.ID
		logger.Info("Successfully created workflow in n8n", "workflowID", workflowID)
	} else {
		// Update existing workflow
		logger.Info("Updating existing workflow in n8n", "workflowID", workflow.Status.WorkflowID)
		response, err = n8nClient.UpdateWorkflow(ctx, workflow.Status.WorkflowID, *workflowDef)
		if err != nil {
			// If workflow not found, create a new one
			if apiErr, ok := err.(*n8nclient.APIError); ok && apiErr.IsNotFound() {
				logger.Info("Workflow not found in n8n, creating new one", "oldWorkflowID", workflow.Status.WorkflowID)
				response, err = n8nClient.CreateWorkflow(ctx, *workflowDef)
				if err != nil {
					return "", fmt.Errorf("failed to create replacement workflow in n8n: %w", err)
				}
			} else {
				return "", fmt.Errorf("failed to update workflow in n8n: %w", err)
			}
		}
		workflowID = response.ID
		logger.Info("Successfully updated workflow in n8n", "workflowID", workflowID)
	}

	// Handle workflow activation/deactivation
	if err := r.handleWorkflowActivation(ctx, workflow, n8nClient, workflowID, response.Active); err != nil {
		return "", fmt.Errorf("failed to handle workflow activation: %w", err)
	}

	return workflowID, nil
}

// handleWorkflowActivation manages workflow activation/deactivation state
func (r *N8nWorkflowReconciler) handleWorkflowActivation(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow, n8nClient *n8nclient.Client, workflowID string, currentActive bool) error {
	logger := log.FromContext(ctx)
	desiredActive := workflow.Spec.Workflow.Active

	// Check if activation state needs to be changed
	if desiredActive == currentActive {
		logger.V(1).Info("Workflow activation state already matches desired state", 
			"workflowID", workflowID, "active", desiredActive)
		return nil
	}

	logger.Info("Updating workflow activation state", 
		"workflowID", workflowID, 
		"currentActive", currentActive, 
		"desiredActive", desiredActive)

	// Attempt to change activation state
	err := n8nClient.ActivateWorkflow(ctx, workflowID, desiredActive)
	if err != nil {
		// Record event for activation failure
		r.Recorder.Event(workflow, "Warning", "ActivationFailed",
			fmt.Sprintf("Failed to set workflow activation state to %t: %v", desiredActive, err))
		return fmt.Errorf("failed to set workflow activation state to %t: %w", desiredActive, err)
	}

	// Record successful activation change
	action := "Activated"
	if !desiredActive {
		action = "Deactivated"
	}
	r.Recorder.Event(workflow, "Normal", action,
		fmt.Sprintf("Workflow %s successfully %s", workflowID, action))

	logger.Info("Successfully updated workflow activation state", 
		"workflowID", workflowID, "active", desiredActive)

	return nil
}

// performHealthCheck verifies that the workflow exists and is in the expected state in n8n
func (r *N8nWorkflowReconciler) performHealthCheck(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow, n8nClient *n8nclient.Client, workflowID string) error {
	logger := log.FromContext(ctx)

	// Verify workflow exists in n8n
	actualWorkflow, err := n8nClient.GetWorkflow(ctx, workflowID)
	if err != nil {
		if apiErr, ok := err.(*n8nclient.APIError); ok && apiErr.IsNotFound() {
			// Workflow doesn't exist in n8n but we think it should
			logger.Error(err, "Workflow not found in n8n during health check", "workflowID", workflowID)
			
			// Update status to indicate health check failure
			update := StatusUpdate{
				SyncStatus:      syncStatusFailed,
				ErrorMessage:    fmt.Sprintf("Workflow %s not found in n8n", workflowID),
				ConditionType:   typeDegradedN8nWorkflow,
				ConditionStatus: metav1.ConditionTrue,
				Reason:          "WorkflowNotFound",
				Message:         fmt.Sprintf("Workflow %s not found in n8n during health check", workflowID),
			}
			if updateErr := r.updateComprehensiveStatus(ctx, workflow, update); updateErr != nil {
				logger.Error(updateErr, "Failed to update status after health check failure")
			}
			
			return fmt.Errorf("workflow %s not found in n8n", workflowID)
		}
		return fmt.Errorf("failed to get workflow during health check: %w", err)
	}

	// Check if activation state matches expectations
	if actualWorkflow.Active != workflow.Status.Active {
		logger.Info("Workflow activation state drift detected during health check",
			"workflowID", workflowID,
			"expected", workflow.Status.Active,
			"actual", actualWorkflow.Active)
		
		// Update status to reflect actual state
		update := StatusUpdate{
			Active: &actualWorkflow.Active,
		}
		if updateErr := r.updateComprehensiveStatus(ctx, workflow, update); updateErr != nil {
			logger.Error(updateErr, "Failed to update status after detecting activation drift")
		}
		
		r.Recorder.Event(workflow, "Warning", "ActivationDrift",
			fmt.Sprintf("Workflow activation state drifted from expected %t to actual %t", workflow.Status.Active, actualWorkflow.Active))
	}

	logger.V(1).Info("Health check passed", "workflowID", workflowID, "active", actualWorkflow.Active)
	return nil
}

// handleReconciliationError provides comprehensive error handling for reconciliation failures
func (r *N8nWorkflowReconciler) handleReconciliationError(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow, err error, operation string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	
	// Determine error type and appropriate response
	var errorMessage string
	var reason string
	var requeueAfter time.Duration = time.Minute * 5 // Default requeue time
	
	switch {
	case isTransientError(err):
		errorMessage = fmt.Sprintf("Transient error during %s: %v", operation, err)
		reason = "TransientError"
		requeueAfter = time.Minute * 2 // Shorter requeue for transient errors
		logger.Info("Transient error encountered", "operation", operation, "error", err)
		
	case isConfigurationError(err):
		errorMessage = fmt.Sprintf("Configuration error during %s: %v", operation, err)
		reason = "ConfigurationError"
		requeueAfter = time.Minute * 10 // Longer requeue for config errors
		logger.Error(err, "Configuration error encountered", "operation", operation)
		
	case isNetworkError(err):
		errorMessage = fmt.Sprintf("Network error during %s: %v", operation, err)
		reason = "NetworkError"
		requeueAfter = time.Minute * 3 // Medium requeue for network errors
		logger.Error(err, "Network error encountered", "operation", operation)
		
	default:
		errorMessage = fmt.Sprintf("Unknown error during %s: %v", operation, err)
		reason = "UnknownError"
		requeueAfter = time.Minute * 5 // Default requeue
		logger.Error(err, "Unknown error encountered", "operation", operation)
	}
	
	// Update status with error information
	update := StatusUpdate{
		SyncStatus:      syncStatusFailed,
		ErrorMessage:    errorMessage,
		ConditionType:   typeDegradedN8nWorkflow,
		ConditionStatus: metav1.ConditionTrue,
		Reason:          reason,
		Message:         errorMessage,
	}
	
	if statusErr := r.updateComprehensiveStatus(ctx, workflow, update); statusErr != nil {
		logger.Error(statusErr, "Failed to update status after error", "originalError", err)
		return ctrl.Result{}, statusErr
	}
	
	// Record event
	r.Recorder.Event(workflow, "Warning", reason, errorMessage)
	
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// Error classification helpers
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for API errors that are typically transient
	if apiErr, ok := err.(*n8nclient.APIError); ok {
		return apiErr.StatusCode >= 500 && apiErr.StatusCode < 600 // 5xx errors are typically transient
	}
	
	// Check for network-related errors that might be transient
	errStr := err.Error()
	transientPatterns := []string{
		"connection refused",
		"timeout",
		"temporary failure",
		"service unavailable",
	}
	
	for _, pattern := range transientPatterns {
		if contains(errStr, pattern) {
			return true
		}
	}
	
	return false
}

func isConfigurationError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for API errors that indicate configuration issues
	if apiErr, ok := err.(*n8nclient.APIError); ok {
		return apiErr.StatusCode == 400 || apiErr.StatusCode == 422 // Bad Request or Unprocessable Entity
	}
	
	// Check for validation errors
	errStr := err.Error()
	configPatterns := []string{
		"validation failed",
		"invalid configuration",
		"missing required field",
		"invalid credential",
	}
	
	for _, pattern := range configPatterns {
		if contains(errStr, pattern) {
			return true
		}
	}
	
	return false
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	networkPatterns := []string{
		"connection reset",
		"no route to host",
		"network unreachable",
		"dns resolution failed",
	}
	
	for _, pattern := range networkPatterns {
		if contains(errStr, pattern) {
			return true
		}
	}
	
	return false
}

// Helper function to check if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

// ConvertWorkflowDefinition converts the Kubernetes workflow definition to n8n format
// Exported for testing purposes
func (r *N8nWorkflowReconciler) ConvertWorkflowDefinition(workflow *n8nv1alpha1.N8nWorkflow, credentialIDs map[string]string) (*n8nclient.WorkflowDefinition, error) {
	logger := log.Log.WithName("convertWorkflowDefinition")
	
	// Convert nodes
	nodes := make([]interface{}, len(workflow.Spec.Workflow.Nodes))
	logger.Info("Converting workflow nodes", "nodeCount", len(workflow.Spec.Workflow.Nodes))
	
	for i, node := range workflow.Spec.Workflow.Nodes {
		nodeMap := map[string]interface{}{
			"id":   node.ID,
			"name": node.Name,
			"type": node.Type,
		}

		if node.TypeVersion != 0 {
			nodeMap["typeVersion"] = node.TypeVersion
		}

		if len(node.Position) > 0 {
			nodeMap["position"] = node.Position
		}

		if node.Parameters != nil {
			// Convert RawExtension to map[string]interface{}
			// For now, we'll use a simple JSON unmarshal approach
			var params map[string]interface{}
			if len(node.Parameters.Raw) > 0 {
				if err := json.Unmarshal(node.Parameters.Raw, &params); err != nil {
					return nil, fmt.Errorf("failed to unmarshal node parameters for node %s: %w", node.ID, err)
				}
				nodeMap["parameters"] = params
			}
		}

		// Map credential names to n8n credential IDs
		if len(node.Credentials) > 0 {
			credentials := make(map[string]string)
			for credType, credName := range node.Credentials {
				if credID, exists := credentialIDs[credName]; exists {
					credentials[credType] = credID
				} else {
					return nil, fmt.Errorf("credential %s not found for node %s", credName, node.ID)
				}
			}
			nodeMap["credentials"] = credentials
		}

		nodes[i] = nodeMap
		logger.V(1).Info("Converted node", "index", i, "id", node.ID, "name", node.Name)
	}

	logger.Info("Converted all nodes", "totalNodes", len(nodes))

	// Convert connections
	var connections map[string]interface{}
	if workflow.Spec.Workflow.Connections != nil && len(workflow.Spec.Workflow.Connections.Raw) > 0 {
		if err := json.Unmarshal(workflow.Spec.Workflow.Connections.Raw, &connections); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workflow connections: %w", err)
		}
		logger.Info("Converted connections", "connectionCount", len(connections))
	}

	// Convert settings
	var settings map[string]interface{}
	if workflow.Spec.Workflow.Settings != nil && len(workflow.Spec.Workflow.Settings.Raw) > 0 {
		if err := json.Unmarshal(workflow.Spec.Workflow.Settings.Raw, &settings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workflow settings: %w", err)
		}
		logger.Info("Converted settings", "settingCount", len(settings))
	}

	result := &n8nclient.WorkflowDefinition{
		Name:        workflow.Spec.Workflow.Name,
		Active:      workflow.Spec.Workflow.Active,
		Tags:        workflow.Spec.Workflow.Tags,
		Nodes:       nodes,
		Connections: connections,
		Settings:    settings,
	}
	
	logger.Info("Workflow definition converted", 
		"name", result.Name,
		"nodeCount", len(result.Nodes),
		"hasConnections", result.Connections != nil,
		"hasSettings", result.Settings != nil)
	
	return result, nil
}

// updateStatus updates the status of the N8nWorkflow resource
func (r *N8nWorkflowReconciler) updateStatus(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow, conditionType string, status metav1.ConditionStatus, reason, message string) error {
	// Update the condition
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}

	// Find and update existing condition or append new one
	updated := false
	for i, existingCondition := range workflow.Status.Conditions {
		if existingCondition.Type == conditionType {
			workflow.Status.Conditions[i] = condition
			updated = true
			break
		}
	}
	if !updated {
		workflow.Status.Conditions = append(workflow.Status.Conditions, condition)
	}

	// Update the status
	return r.Status().Update(ctx, workflow)
}

// updateComprehensiveStatus updates the workflow status with comprehensive information
func (r *N8nWorkflowReconciler) updateComprehensiveStatus(ctx context.Context, workflow *n8nv1alpha1.N8nWorkflow, update StatusUpdate) error {
	// Update observed generation
	workflow.Status.ObservedGeneration = workflow.Generation

	// Update sync status
	if update.SyncStatus != "" {
		workflow.Status.SyncStatus = update.SyncStatus
	}

	// Update error message
	workflow.Status.ErrorMessage = update.ErrorMessage

	// Update credentials synced status
	if update.CredentialsSynced != nil {
		workflow.Status.CredentialsSynced = *update.CredentialsSynced
	}

	// Update credential IDs if provided
	if update.CredentialIDs != nil {
		workflow.Status.CredentialIDs = update.CredentialIDs
	}

	// Update workflow ID if provided
	if update.WorkflowID != "" {
		workflow.Status.WorkflowID = update.WorkflowID
	}

	// Update active status if provided
	if update.Active != nil {
		workflow.Status.Active = *update.Active
	}

	// Update last sync time if this is a successful sync
	if update.SyncStatus == syncStatusSynced {
		now := metav1.NewTime(time.Now())
		workflow.Status.LastSync = &now
	}

	// Update condition if provided
	if update.ConditionType != "" {
		condition := metav1.Condition{
			Type:               update.ConditionType,
			Status:             update.ConditionStatus,
			Reason:             update.Reason,
			Message:            update.Message,
			LastTransitionTime: metav1.NewTime(time.Now()),
		}

		// Find and update existing condition or append new one
		updated := false
		for i, existingCondition := range workflow.Status.Conditions {
			if existingCondition.Type == update.ConditionType {
				workflow.Status.Conditions[i] = condition
				updated = true
				break
			}
		}
		if !updated {
			workflow.Status.Conditions = append(workflow.Status.Conditions, condition)
		}
	}

	// Update the status
	return r.Status().Update(ctx, workflow)
}

// StatusUpdate represents a comprehensive status update
type StatusUpdate struct {
	SyncStatus         string
	ErrorMessage       string
	CredentialsSynced  *bool
	CredentialIDs      map[string]string
	WorkflowID         string
	Active             *bool
	ConditionType      string
	ConditionStatus    metav1.ConditionStatus
	Reason             string
	Message            string
}

// SetupWithManager sets up the controller with the Manager.
func (r *N8nWorkflowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize credential manager if not provided
	if r.CredentialManager == nil {
		r.CredentialManager = credentialmanager.NewManager(mgr.GetClient())
	}

	// Initialize validator if not provided
	if r.Validator == nil {
		r.Validator = NewN8nInstanceValidator(mgr.GetClient())
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&n8nv1alpha1.N8nWorkflow{}).
		Complete(r)
}