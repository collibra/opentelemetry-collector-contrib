package client

import (
	"net/http"
	"time"
)

const (
	HeaderUserAgent     string = "User-Agent"
	HeaderDatadogAPIKey string = "DD-API-KEY"
	HeaderContentType   string = "Content-Type"

	ContentTypeApplicationJSON string = "application/json"
)

type DatadogClient struct {
	client    *http.Client
	apiKey    string
	userAgent string
}

func NewClient(apiKey, userAgent string) *DatadogClient {
	c := &http.Client{
		Transport: &http.Transport{
			// TODO extract to config
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: false,
		},
	}
	return &DatadogClient{
		client:    c,
		apiKey:    apiKey,
		userAgent: userAgent,
	}
}

func (c *DatadogClient) Close() {
	if c.client != nil {
		c.client.CloseIdleConnections()
	}
}

func (c *DatadogClient) Do(req *http.Request) (*http.Response, error) {
	c.setHeaders(req.Header)
	return c.client.Do(req)
}

func (c *DatadogClient) setHeaders(h http.Header) {
	h.Set(HeaderDatadogAPIKey, c.apiKey)
	h.Set(HeaderUserAgent, c.userAgent)
}
