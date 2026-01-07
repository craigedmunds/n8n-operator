# N8nWorkflow Examples

This directory contains example N8nWorkflow resources demonstrating various use cases and credential types.

## Prerequisites

Before deploying workflows, ensure you have:
1. An N8n instance deployed and managed by the N8n Operator
2. Required secrets created for workflow credentials
3. The N8nWorkflow CRD installed in your cluster

## Examples

### 1. Basic Scheduled HTTP Request (`v1alpha1_n8nworkflow.yaml`)

A simple workflow that makes HTTP requests on a schedule.

**Features:**
- Schedule trigger (every 5 minutes)
- HTTP request node
- HTTP header authentication

**Deploy:**
```bash
# Create the secret for credentials
kubectl create secret generic sample-api-secret \
  --from-literal=headerName=X-API-Key \
  --from-literal=headerValue=your-api-key-here

# Apply the workflow
kubectl apply -f config/samples/v1alpha1_n8nworkflow.yaml
```

### 2. Webhook with Basic Auth (`webhook-basic-auth.yaml`)

A workflow triggered by webhooks with HTTP Basic Authentication.

**Features:**
- Webhook trigger
- HTTP Basic Auth credentials
- Response node

**Deploy:**
```bash
# Create the secret for basic auth
kubectl create secret generic webhook-basic-auth \
  --from-literal=user=admin \
  --from-literal=password=secure-password

# Apply the workflow
kubectl apply -f config/samples/webhook-basic-auth.yaml
```

### 3. API Integration with OAuth2 (`api-oauth2.yaml`)

A workflow that integrates with an OAuth2-protected API.

**Features:**
- Manual trigger
- OAuth2 credentials
- HTTP request with OAuth2

**Deploy:**
```bash
# Create the secret for OAuth2
kubectl create secret generic oauth2-credentials \
  --from-literal=clientId=your-client-id \
  --from-literal=clientSecret=your-client-secret \
  --from-literal=accessTokenUrl=https://oauth.example.com/token \
  --from-literal=authUrl=https://oauth.example.com/authorize

# Apply the workflow
kubectl apply -f config/samples/api-oauth2.yaml
```

### 4. Multi-Credential Workflow (`multi-credential.yaml`)

A workflow using multiple credential types.

**Features:**
- Multiple credential types (API key, Basic Auth)
- Multiple HTTP requests
- Conditional logic

**Deploy:**
```bash
# Create secrets for multiple credentials
kubectl create secret generic api-key-secret \
  --from-literal=headerName=X-API-Key \
  --from-literal=headerValue=your-api-key

kubectl create secret generic basic-auth-secret \
  --from-literal=user=username \
  --from-literal=password=password

# Apply the workflow
kubectl apply -f config/samples/multi-credential.yaml
```

## Usage Patterns

### Cross-Namespace N8n Instance Reference

Workflows can reference N8n instances in different namespaces:

```yaml
spec:
  n8nRef:
    name: n8n-production
    namespace: n8n-prod
```

### Credential Management

Credentials are managed through Kubernetes secrets. The secret structure depends on the credential type:

#### HTTP Basic Auth
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: basic-auth-creds
stringData:
  user: "username"
  password: "password"
```

#### API Key (HTTP Header Auth)
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: api-key-creds
stringData:
  headerName: "X-API-Key"
  headerValue: "your-api-key"
```

#### OAuth2
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: oauth2-creds
stringData:
  clientId: "your-client-id"
  clientSecret: "your-client-secret"
  accessTokenUrl: "https://oauth.example.com/token"
  authUrl: "https://oauth.example.com/authorize"
```

### Workflow Activation

Control workflow activation through the `active` field:

```yaml
spec:
  workflow:
    active: true  # Workflow will be active in n8n
```

### Status Monitoring

Check workflow status:

```bash
# Get workflow status
kubectl get n8nworkflow -o wide

# Describe workflow for detailed status
kubectl describe n8nworkflow <workflow-name>
```

## Best Practices

1. **Secret Management**: Always use Kubernetes secrets for credentials, never hardcode sensitive data
2. **Namespace Organization**: Group related workflows and N8n instances in the same namespace
3. **Tagging**: Use tags to organize and categorize workflows
4. **Naming**: Use descriptive names for workflows and credentials
5. **Testing**: Test workflows in a development environment before deploying to production
6. **GitOps**: Store workflow definitions in Git for version control and automated deployment
7. **Monitoring**: Regularly check workflow status and sync conditions

## Troubleshooting

### Workflow Not Syncing

Check the workflow status:
```bash
kubectl describe n8nworkflow <workflow-name>
```

Common issues:
- N8n instance not found or not accessible
- Invalid credentials or missing secrets
- Invalid workflow definition
- Network connectivity issues

### Credential Issues

Verify secrets exist and have correct data:
```bash
kubectl get secret <secret-name> -o yaml
```

### N8n Instance Connection

Verify the N8n instance is running:
```bash
kubectl get n8n <instance-name> -n <namespace>
kubectl get pods -n <namespace>
```

## Additional Resources

- [N8n Documentation](https://docs.n8n.io/)
- [N8n Operator Documentation](../../../docs/)
- [Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)
