package sidecar

import (
	"bytes"
	"fmt"
	"os"
	"text/template"
)

const collectorTemplate = `receivers:
  otlp:
    protocols:
      http:
        endpoint: "{{.Listen}}"

processors:
  batch:
    timeout: 5s
    send_batch_size: 1024

exporters:
  otlphttp/parquet:
    endpoint: "http://localhost:4319"
{{- if .Backend}}
  otlphttp/backend:
    endpoint: "{{.Backend.Endpoint}}"
{{- if .Backend.Headers}}
    headers:
{{- range $k, $v := .Backend.Headers}}
      {{$k}}: "{{$v}}"
{{- end}}
{{- end}}
{{- end}}

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/parquet{{if .Backend}}, otlphttp/backend{{end}}]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/parquet{{if .Backend}}, otlphttp/backend{{end}}]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/parquet{{if .Backend}}, otlphttp/backend{{end}}]
`

var collectorTmpl = template.Must(
	template.New("collector").Parse(collectorTemplate),
)

// GenerateCollectorConfig produces OTel Collector YAML from
// the sidecar config. The Collector receives on OTLP/HTTP,
// batches, and exports to otlp2parquet (always) and an
// optional production backend.
func GenerateCollectorConfig(cfg *SidecarConfig) ([]byte, error) {
	if cfg == nil {
		panic("GenerateCollectorConfig: cfg is nil")
	}
	if cfg.Listen == "" {
		return nil, fmt.Errorf(
			"listen address must not be empty",
		)
	}

	var buf bytes.Buffer
	if err := collectorTmpl.Execute(&buf, cfg); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}

	return buf.Bytes(), nil
}

// WriteCollectorConfig generates and writes the config to path.
func WriteCollectorConfig(cfg *SidecarConfig, path string) error {
	if path == "" {
		panic("WriteCollectorConfig: path is empty")
	}

	data, err := GenerateCollectorConfig(cfg)
	if err != nil {
		return fmt.Errorf("generate config: %w", err)
	}

	const fileMode = 0o644
	if err := os.WriteFile(path, data, fileMode); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}
