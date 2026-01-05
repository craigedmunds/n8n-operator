package n8nclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/edenreich/n8n-cli/n8n"
)

// Client wraps the n8n-cli client with additional functionality needed by the operator
type Client struct {
	n8nClient *n8n.Client
	baseURL   string
	apiKey    string
}

// NewClient creates a new n8n API client using the n8n-cli library
func NewClient(baseURL, apiKey string) (*Client, error) {
	// Create the n8n client using the correct constructor
	n8nClient := n8n.NewClient(baseURL, apiKey)

	return &Client{
		n8nClient: n8nClient,
		baseURL:   baseURL,
		apiKey:    apiKey,
	}, nil
}

// WorkflowResponse represents the response from workflow operations
type WorkflowResponse struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

// WorkflowDefinition represents a workflow definition for creation/update
type WorkflowDefinition struct {
	Name        string                 `json:"name"`
	Active      bool                   `json:"active"`
	Tags        []string               `json:"tags,omitempty"`
	Nodes       []interface{}          `json:"nodes"`
	Connections map[string]interface{} `json:"connections"`
	Settings    map[string]interface{} `json:"settings,omitempty"`
}

// CreateWorkflow creates a new workflow in n8n
func (c *Client) CreateWorkflow(ctx context.Context, workflow WorkflowDefinition) (*WorkflowResponse, error) {
	// Convert our WorkflowDefinition to the n8n library's Workflow type
	n8nWorkflow := &n8n.Workflow{
		Name:        workflow.Name,
		Active:      &workflow.Active,
		Connections: workflow.Connections,
		// Note: We'll need to convert nodes and settings appropriately
		// For now, we'll use empty values to get the basic structure working
		Nodes:    []n8n.Node{},
		Settings: n8n.WorkflowSettings{},
	}

	result, err := c.n8nClient.CreateWorkflow(n8nWorkflow)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	response := &WorkflowResponse{
		ID:     getStringValue(result.Id),
		Name:   result.Name,
		Active: getBoolValue(result.Active),
	}

	return response, nil
}

// UpdateWorkflow updates an existing workflow in n8n
func (c *Client) UpdateWorkflow(ctx context.Context, workflowID string, workflow WorkflowDefinition) (*WorkflowResponse, error) {
	// Convert our WorkflowDefinition to the n8n library's Workflow type
	n8nWorkflow := &n8n.Workflow{
		Name:        workflow.Name,
		Active:      &workflow.Active,
		Connections: workflow.Connections,
		// Note: We'll need to convert nodes and settings appropriately
		Nodes:    []n8n.Node{},
		Settings: n8n.WorkflowSettings{},
	}

	result, err := c.n8nClient.UpdateWorkflow(workflowID, n8nWorkflow)
	if err != nil {
		return nil, fmt.Errorf("failed to update workflow: %w", err)
	}

	response := &WorkflowResponse{
		ID:     getStringValue(result.Id),
		Name:   result.Name,
		Active: getBoolValue(result.Active),
	}

	return response, nil
}

// DeleteWorkflow deletes a workflow from n8n
func (c *Client) DeleteWorkflow(ctx context.Context, workflowID string) error {
	err := c.n8nClient.DeleteWorkflow(workflowID)
	if err != nil {
		return fmt.Errorf("failed to delete workflow: %w", err)
	}
	return nil
}

// GetWorkflow retrieves a workflow from n8n
func (c *Client) GetWorkflow(ctx context.Context, workflowID string) (*WorkflowResponse, error) {
	result, err := c.n8nClient.GetWorkflow(workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	response := &WorkflowResponse{
		ID:     getStringValue(result.Id),
		Name:   result.Name,
		Active: getBoolValue(result.Active),
	}

	return response, nil
}

// ActivateWorkflow activates or deactivates a workflow in n8n
func (c *Client) ActivateWorkflow(ctx context.Context, workflowID string, active bool) error {
	var err error
	if active {
		_, err = c.n8nClient.ActivateWorkflow(workflowID)
	} else {
		_, err = c.n8nClient.DeactivateWorkflow(workflowID)
	}

	if err != nil {
		return fmt.Errorf("failed to set workflow active state to %t: %w", active, err)
	}
	return nil
}

// Helper functions to safely dereference pointers
func getStringValue(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return *ptr
}

func getBoolValue(ptr *bool) bool {
	if ptr == nil {
		return false
	}
	return *ptr
}

// CredentialResponse represents the response from credential operations
type CredentialResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// CredentialDefinition represents a credential definition for creation/update
type CredentialDefinition struct {
	Name string                 `json:"name"`
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

// CreateCredential creates a new credential in n8n
// Since the n8n-cli library doesn't have credential methods, we implement them with direct HTTP calls
func (c *Client) CreateCredential(ctx context.Context, credential CredentialDefinition) (*CredentialResponse, error) {
	// Make direct HTTP call to n8n API for credential creation
	resp, err := c.doHTTPRequest(ctx, "POST", "/api/v1/credentials", credential)
	if err != nil {
		return nil, fmt.Errorf("failed to create credential: %w", err)
	}
	defer resp.Body.Close()

	var result n8n.Credential
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode credential response: %w", err)
	}

	response := &CredentialResponse{
		ID:   getStringValue(result.Id),
		Name: result.Name,
		Type: result.Type,
	}

	return response, nil
}

// UpdateCredential updates an existing credential in n8n
func (c *Client) UpdateCredential(ctx context.Context, credentialID string, credential CredentialDefinition) (*CredentialResponse, error) {
	// Make direct HTTP call to n8n API for credential update
	path := fmt.Sprintf("/api/v1/credentials/%s", credentialID)
	resp, err := c.doHTTPRequest(ctx, "PUT", path, credential)
	if err != nil {
		return nil, fmt.Errorf("failed to update credential: %w", err)
	}
	defer resp.Body.Close()

	var result n8n.Credential
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode credential response: %w", err)
	}

	response := &CredentialResponse{
		ID:   getStringValue(result.Id),
		Name: result.Name,
		Type: result.Type,
	}

	return response, nil
}

// DeleteCredential deletes a credential from n8n
func (c *Client) DeleteCredential(ctx context.Context, credentialID string) error {
	// Make direct HTTP call to n8n API for credential deletion
	path := fmt.Sprintf("/api/v1/credentials/%s", credentialID)
	resp, err := c.doHTTPRequest(ctx, "DELETE", path, nil)
	if err != nil {
		return fmt.Errorf("failed to delete credential: %w", err)
	}
	defer resp.Body.Close()

	return nil
}

// GetCredential retrieves a credential from n8n
func (c *Client) GetCredential(ctx context.Context, credentialID string) (*CredentialResponse, error) {
	// Make direct HTTP call to n8n API for credential retrieval
	path := fmt.Sprintf("/api/v1/credentials/%s", credentialID)
	resp, err := c.doHTTPRequest(ctx, "GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get credential: %w", err)
	}
	defer resp.Body.Close()

	var result n8n.Credential
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode credential response: %w", err)
	}

	response := &CredentialResponse{
		ID:   getStringValue(result.Id),
		Name: result.Name,
		Type: result.Type,
	}

	return response, nil
}

// doHTTPRequest performs a direct HTTP request to the n8n API
// This is used for API endpoints not yet supported by the n8n-cli library
func (c *Client) doHTTPRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonBody)
	}

	fullURL, err := url.JoinPath(c.baseURL, path)
	if err != nil {
		return nil, fmt.Errorf("failed to build request URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-N8N-API-KEY", c.apiKey)
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	// Check for HTTP errors
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(bodyBytes),
		}
	}

	return resp, nil
}

// APIError represents an error response from the n8n API
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("n8n API error (status %d): %s", e.StatusCode, e.Message)
}

// IsNotFound returns true if the error is a 404 Not Found
func (e *APIError) IsNotFound() bool {
	return e.StatusCode == http.StatusNotFound
}

// IsConflict returns true if the error is a 409 Conflict
func (e *APIError) IsConflict() bool {
	return e.StatusCode == http.StatusConflict
}