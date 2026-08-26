package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type Notifier interface {
	Notify(context.Context, Alert) error
}

type WebhookNotifier struct {
	URL    string
	Token  string
	Client *http.Client
}

func (n WebhookNotifier) Notify(ctx context.Context, alert Alert) error {
	if n.URL == "" {
		return nil
	}
	payload, err := json.Marshal(alert)
	if err != nil {
		return err
	}
	client := n.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, n.URL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if n.Token != "" {
		request.Header.Set("Authorization", "Bearer "+n.Token)
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return &notificationError{status: response.StatusCode}
	}
	return nil
}

type notificationError struct{ status int }

func (e *notificationError) Error() string {
	return "owner notification endpoint returned HTTP status " + http.StatusText(e.status)
}
