package client

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatadogClient_setHeaders(t *testing.T) {
	c := NewClient("api-key", "user-agent")
	h := http.Header{}
	c.setHeaders(h)
	assert.Equal(t, "api-key", h.Get(HeaderDatadogAPIKey))
	assert.Equal(t, "user-agent", h.Get(HeaderUserAgent))
}
