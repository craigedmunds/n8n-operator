package integration

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jakub-k-slys/n8n-operator/internal/n8nclient"
)

// TestWorkflowCreation tests that workflows are created with nodes properly serialized
func TestWorkflowCreation(t *testing.T) {
	// Get API key from environment
	apiKey := os.Getenv("N8N_API_KEY")
	if apiKey == "" {
		t.Skip("N8N_API_KEY environment variable not set, skipping integration test")
	}

	baseURL := os.Getenv("N8N_BASE_URL")
	if baseURL == "" {
		baseURL = "https://n8n.lab.local.ctoaas.co"
	}

	t.Logf("Creating n8n client with baseURL: %s", baseURL)

	// Create n8n client
	client, err := n8nclient.NewClient(baseURL, apiKey)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Create workflow definition matching our sample
	nodes := []interface{}{
		map[string]interface{}{
			"id":          "schedule-trigger",
			"name":        "Schedule Trigger",
			"type":        "n8n-nodes-base.scheduleTrigger",
			"typeVersion": 1.3,
			"position":    []int{240, 300},
			"parameters": map[string]interface{}{
				"rule": map[string]interface{}{
					"interval": []map[string]interface{}{
						{
							"field": "hours",
						},
					},
				},
			},
		},
		map[string]interface{}{
			"id":          "http-request",
			"name":        "HTTP Request",
			"type":        "n8n-nodes-base.httpRequest",
			"typeVersion": 4.3,
			"position":    []int{460, 300},
			"parameters": map[string]interface{}{
				"method":  "GET",
				"url":     "https://httpbin.org/get",
				"options": map[string]interface{}{},
			},
		},
	}

	connections := map[string]interface{}{
		"Schedule Trigger": map[string]interface{}{
			"main": [][]map[string]interface{}{
				{
					{
						"node":  "HTTP Request",
						"type":  "main",
						"index": 0,
					},
				},
			},
		},
	}

	settings := map[string]interface{}{
		"executionOrder":  "v1",
		"availableInMCP": false,
	}

	workflow := n8nclient.WorkflowDefinition{
		Name:        "Integration Test Workflow",
		Active:      false, // Don't activate yet
		Tags:        []string{"test", "integration"},
		Nodes:       nodes,
		Connections: connections,
		Settings:    settings,
	}

	t.Logf("Workflow definition:")
	jsonData, _ := json.MarshalIndent(workflow, "", "  ")
	t.Logf("%s", string(jsonData))

	if len(workflow.Nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d", len(workflow.Nodes))
	}

	t.Logf("Creating workflow with %d nodes...", len(workflow.Nodes))

	// Create the workflow
	ctx := context.Background()
	response, err := client.CreateWorkflow(ctx, workflow)
	if err != nil {
		t.Fatalf("Failed to create workflow: %v", err)
	}

	t.Logf("✅ Successfully created workflow!")
	t.Logf("   ID: %s", response.ID)
	t.Logf("   Name: %s", response.Name)
	t.Logf("   Active: %v", response.Active)

	// Verify the workflow was created by fetching it back
	fetchedWorkflow, err := client.GetWorkflow(ctx, response.ID)
	if err != nil {
		t.Fatalf("Failed to fetch created workflow: %v", err)
	}

	if fetchedWorkflow.ID != response.ID {
		t.Errorf("Workflow ID mismatch: expected %s, got %s", response.ID, fetchedWorkflow.ID)
	}

	if fetchedWorkflow.Name != workflow.Name {
		t.Errorf("Workflow name mismatch: expected %s, got %s", workflow.Name, fetchedWorkflow.Name)
	}

	t.Logf("✅ Workflow verification passed")
	t.Logf("Check the workflow in n8n UI: %s/workflow/%s", baseURL, response.ID)

	// Clean up - delete the test workflow
	t.Logf("Cleaning up test workflow...")
	err = client.DeleteWorkflow(ctx, response.ID)
	if err != nil {
		t.Logf("Warning: Failed to delete test workflow: %v", err)
	} else {
		t.Logf("✅ Test workflow deleted")
	}
}

// TestWorkflowCreationWithRealData tests workflow creation using data from actual CRD
func TestWorkflowCreationWithRealData(t *testing.T) {
	// This test would parse the sample-workflow.yaml and create it
	// For now, we'll skip this and focus on the basic test above
	t.Skip("TODO: Implement test that reads from sample-workflow.yaml")
}
