package datadoglogexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	type fields struct {
		URL    string
		APIKey string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name:    "Empty",
			fields:  fields{},
			wantErr: errUnsetURL,
		},
		{
			name:    "Missing key",
			fields:  fields{URL: "http://some.url"},
			wantErr: errUnsetAPIKey,
		},
		{
			name:    "No errors",
			fields:  fields{URL: "http://some.url", APIKey: "here"},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				URL:    tt.fields.URL,
				APIKey: tt.fields.APIKey,
			}
			gotErr := config.Validate()
			assert.Equal(t, gotErr, tt.wantErr)
		})
	}
}
