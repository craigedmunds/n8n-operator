package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	n8nv1alpha1 "github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	"github.com/jakub-k-slys/n8n-operator/internal/controller"
	"github.com/jakub-k-slys/n8n-operator/internal/n8nclient"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// TestControllerConversionFromYAML tests that the controller correctly converts
// a workflow loaded from YAML to the n8n API format
func TestControllerConversionFromYAML(t *testing.T) {
	// Load the sample workflow YAML
	yamlPath := filepath.Join("..", "..", "..", "k8s-lab", "supporting-applications", "n8n", "sample-workflow.yaml")
	yamlData, err := os.ReadFile(yamlPath)
	if err != nil {
		t.Fatalf("Failed to read sample workflow YAML: %v", err)
	}

	// Create a scheme and decoder
	scheme := runtime.NewScheme()
	if err := n8nv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	decode := serializer.NewCodecFactory(scheme).UniversalDeserializer().Decode
	obj, _, err := decode(yamlData, nil, nil)
	if err != nil {
		t.Fatalf("Failed to decode YAML: %v", err)
	}

	workflow, ok := obj.(*n8nv1alpha1.N8nWorkflow)
	if !ok {
		t.Fatalf("Decoded object is not an N8nWorkflow")
	}

	t.Logf("Loaded workflow: %s", workflow.Spec.Workflow.Name)
	t.Logf("Node count: %d", len(workflow.Spec.Workflow.Nodes))

	// Verify the YAML was parsed correctly with proper types
	if len(workflow.Spec.Workflow.Nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d", len(workflow.Spec.Workflow.Nodes))
	}

	// Check first node (schedule trigger)
	node0 := workflow.Spec.Workflow.Nodes[0]
	t.Logf("Node 0: id=%s, type=%s, typeVersion=%v, position=%v", 
		node0.ID, node0.Type, node0.TypeVersion, node0.Position)
	
	if node0.TypeVersion == 0 {
		t.Errorf("Node 0 typeVersion is 0, expected a number like 1.3")
	}
	if len(node0.Position) != 2 {
		t.Errorf("Node 0 position length is %d, expected 2", len(node0.Position))
	}

	// Check second node (HTTP request)
	node1 := workflow.Spec.Workflow.Nodes[1]
	t.Logf("Node 1: id=%s, type=%s, typeVersion=%v, position=%v", 
		node1.ID, node1.Type, node1.TypeVersion, node1.Position)
	
	if node1.TypeVersion == 0 {
		t.Errorf("Node 1 typeVersion is 0, expected a number like 4.3")
	}
	if len(node1.Position) != 2 {
		t.Errorf("Node 1 position length is %d, expected 2", len(node1.Position))
	}

	// Create a mock reconciler to test the conversion
	reconciler := &controller.N8nWorkflowReconciler{}

	// Test the conversion (without credentials for now)
	credentialIDs := make(map[string]string)
	workflowDef, err := reconciler.ConvertWorkflowDefinition(workflow, credentialIDs)
	if err != nil {
		t.Fatalf("Failed to convert workflow: %v", err)
	}

	t.Logf("Converted workflow definition:")
	t.Logf("  Name: %s", workflowDef.Name)
	t.Logf("  Node count: %d", len(workflowDef.Nodes))
	t.Logf("  Has connections: %v", workflowDef.Connections != nil)
	t.Logf("  Has settings: %v", workflowDef.Settings != nil)

	// Verify the conversion produced the correct structure
	if len(workflowDef.Nodes) != 2 {
		t.Errorf("Converted workflow has %d nodes, expected 2", len(workflowDef.Nodes))
	}

	// Verify nodes are maps with the correct structure
	for i, node := range workflowDef.Nodes {
		nodeMap, ok := node.(map[string]interface{})
		if !ok {
			t.Errorf("Node %d is not a map[string]interface{}", i)
			continue
		}

		// Check required fields
		if _, ok := nodeMap["id"]; !ok {
			t.Errorf("Node %d missing 'id' field", i)
		}
		if _, ok := nodeMap["name"]; !ok {
			t.Errorf("Node %d missing 'name' field", i)
		}
		if _, ok := nodeMap["type"]; !ok {
			t.Errorf("Node %d missing 'type' field", i)
		}

		// Check typeVersion is a number
		if typeVersion, ok := nodeMap["typeVersion"]; ok {
			switch v := typeVersion.(type) {
			case float64:
				t.Logf("Node %d typeVersion is float64: %v ✓", i, v)
			case int:
				t.Logf("Node %d typeVersion is int: %v ✓", i, v)
			default:
				t.Errorf("Node %d typeVersion is %T, expected number (float64 or int)", i, v)
			}
		} else {
			t.Errorf("Node %d missing 'typeVersion' field", i)
		}

		// Check position is an array of numbers
		if position, ok := nodeMap["position"]; ok {
			switch v := position.(type) {
			case []int:
				if len(v) != 2 {
					t.Errorf("Node %d position has %d elements, expected 2", i, len(v))
				}
				t.Logf("Node %d position is []int: %v ✓", i, v)
			case []interface{}:
				if len(v) != 2 {
					t.Errorf("Node %d position has %d elements, expected 2", i, len(v))
				}
				// Check each element is a number
				for j, elem := range v {
					switch elem.(type) {
					case int, int64, float64:
						// OK
					default:
						t.Errorf("Node %d position[%d] is %T, expected number", i, j, elem)
					}
				}
				t.Logf("Node %d position is []interface{} with numbers: %v ✓", i, v)
			default:
				t.Errorf("Node %d position is %T, expected []int or []interface{}", i, v)
			}
		} else {
			t.Errorf("Node %d missing 'position' field", i)
		}
	}
}

// TestWorkflowUpdateScenario tests updating an existing workflow
func TestWorkflowUpdateScenario(t *testing.T) {
	// Get API key from environment
	apiKey := os.Getenv("N8N_API_KEY")
	if apiKey == "" {
		t.Skip("N8N_API_KEY environment variable not set, skipping integration test")
	}

	baseURL := os.Getenv("N8N_BASE_URL")
	if baseURL == "" {
		baseURL = "https://n8n.lab.local.ctoaas.co"
	}

	t.Logf("Testing workflow update scenario with baseURL: %s", baseURL)

	// Create n8n client
	client, err := n8nclient.NewClient(baseURL, apiKey)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Create initial workflow
	initialWorkflow := n8nclient.WorkflowDefinition{
		Name: "Update Test Workflow",
		Nodes: []interface{}{
			map[string]interface{}{
				"id":          "start",
				"name":        "Start",
				"type":        "n8n-nodes-base.start",
				"typeVersion": 1.0,
				"position":    []int{0, 0},
				"parameters":  map[string]interface{}{},
			},
		},
		Connections: map[string]interface{}{},
		Settings: map[string]interface{}{
			"executionOrder": "v1",
		},
	}

	t.Logf("Creating initial workflow with %d nodes...", len(initialWorkflow.Nodes))
	response, err := client.CreateWorkflow(ctx, initialWorkflow)
	if err != nil {
		t.Fatalf("Failed to create initial workflow: %v", err)
	}
	workflowID := response.ID
	t.Logf("✅ Created workflow with ID: %s", workflowID)

	// Defer cleanup
	defer func() {
		t.Logf("Cleaning up test workflow...")
		if err := client.DeleteWorkflow(ctx, workflowID); err != nil {
			t.Logf("Warning: Failed to delete test workflow: %v", err)
		} else {
			t.Logf("✅ Test workflow deleted")
		}
	}()

	// Update the workflow with more nodes
	updatedWorkflow := n8nclient.WorkflowDefinition{
		Name: "Update Test Workflow (Updated)",
		Nodes: []interface{}{
			map[string]interface{}{
				"id":          "start",
				"name":        "Start",
				"type":        "n8n-nodes-base.start",
				"typeVersion": 1.0,
				"position":    []int{0, 0},
				"parameters":  map[string]interface{}{},
			},
			map[string]interface{}{
				"id":          "http-request",
				"name":        "HTTP Request",
				"type":        "n8n-nodes-base.httpRequest",
				"typeVersion": 4.3,
				"position":    []int{200, 0},
				"parameters": map[string]interface{}{
					"method":  "GET",
					"url":     "https://httpbin.org/get",
					"options": map[string]interface{}{},
				},
			},
		},
		Connections: map[string]interface{}{
			"Start": map[string]interface{}{
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
		},
		Settings: map[string]interface{}{
			"executionOrder": "v1",
		},
	}

	t.Logf("Updating workflow to have %d nodes...", len(updatedWorkflow.Nodes))
	updateResponse, err := client.UpdateWorkflow(ctx, workflowID, updatedWorkflow)
	if err != nil {
		t.Fatalf("Failed to update workflow: %v", err)
	}

	t.Logf("✅ Updated workflow")
	t.Logf("   ID: %s", updateResponse.ID)
	t.Logf("   Name: %s", updateResponse.Name)

	// Fetch the workflow to verify the update
	fetchedWorkflow, err := client.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatalf("Failed to fetch updated workflow: %v", err)
	}

	if fetchedWorkflow.Name != updatedWorkflow.Name {
		t.Errorf("Workflow name not updated: expected %s, got %s", updatedWorkflow.Name, fetchedWorkflow.Name)
	}

	t.Logf("✅ Workflow update verified")
}
