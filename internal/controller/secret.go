package controller

import (
	n8nv1alpha1 "github.com/jakub-k-slys/n8n-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// apiCredentialsSecretForN8n creates a Secret resource for API credentials
func (r *N8nReconciler) apiCredentialsSecretForN8n(n8n *n8nv1alpha1.N8n, apiKey string) (*corev1.Secret, error) {
	secretName := getAPICredentialsSecretName(n8n.Name)
	
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: n8n.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "n8n",
				"app.kubernetes.io/instance":   n8n.Name,
				"app.kubernetes.io/component":  "api-credentials",
				"app.kubernetes.io/managed-by": "n8n-operator",
			},
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"apiKey": apiKey,
		},
	}
	
	if err := ctrl.SetControllerReference(n8n, secret, r.Scheme); err != nil {
		return nil, err
	}
	
	return secret, nil
}
