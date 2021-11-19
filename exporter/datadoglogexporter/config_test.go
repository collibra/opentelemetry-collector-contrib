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
			assert.Equal(t, tt.wantErr, gotErr)
		})
	}
}

func TestConfig_InitDefaults(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   Config
	}{
		{
			name:   "Empty config",
			config: Config{},
			want: Config{
				HostnameFrom:    DefaultHostnameFrom,
				SourceFrom:      DefaultSourceFrom,
				ServiceNameFrom: DefaultServiceFrom,
				FileNameFrom:    DefaultFileNameFrom,
			},
		},
		{
			name: "Empty arrays",
			want: Config{
				HostnameFrom:    DefaultHostnameFrom,
				SourceFrom:      DefaultSourceFrom,
				ServiceNameFrom: DefaultServiceFrom,
				FileNameFrom:    DefaultFileNameFrom,
			},
		},
		{
			name: "Not empty",
			config: Config{
				HostnameFrom:    []string{"a"},
				SourceFrom:      []string{"b"},
				ServiceNameFrom: []string{"c"},
				FileNameFrom:    []string{"d"},
			},
			want: Config{
				HostnameFrom:    []string{"a"},
				SourceFrom:      []string{"b"},
				ServiceNameFrom: []string{"c"},
				FileNameFrom:    []string{"d"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.config.InitDefaults()
			assert.Equal(t, tt.config, tt.want)
		})
	}
}
