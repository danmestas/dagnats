package main

import (
	"context"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// NewMCPServer creates a configured MCP server with all telemetry
// query tools and schema resources registered.
func NewMCPServer(db *DB) *server.MCPServer {
	s := server.NewMCPServer(
		"dagnats-mcp-duckdb",
		"0.1.0",
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(true, false),
	)

	registerTools(s, db)
	registerResources(s)
	return s
}

// registerTools adds all 7 telemetry query tools.
func registerTools(s *server.MCPServer, db *DB) {
	s.AddTool(queryTracesTool(), wrapTool(db, queryTraces))
	s.AddTool(getTraceTool(), wrapTool(db, getTrace))
	s.AddTool(queryLogsTool(), wrapTool(db, queryLogs))
	s.AddTool(queryMetricsTool(), wrapTool(db, queryMetrics))
	s.AddTool(getErrorsTool(), wrapTool(db, getErrors))
	s.AddTool(latencyPercentilesTool(), wrapTool(db, latencyPercentiles))
	s.AddTool(querySqlTool(), wrapTool(db, querySql))
}

// registerResources adds all schema description resources.
func registerResources(s *server.MCPServer) {
	for uri, res := range schemaResources {
		content := res.content
		s.AddResource(
			mcp.NewResource(
				uri,
				res.name,
				mcp.WithResourceDescription(res.desc),
				mcp.WithMIMEType("text/plain"),
			),
			staticResourceHandler(uri, content),
		)
	}
}

// wrapTool adapts a tool function to the mcp-go handler signature.
func wrapTool(
	db *DB,
	fn func(*DB, map[string]any) (string, error),
) server.ToolHandlerFunc {
	return func(
		_ context.Context, req mcp.CallToolRequest,
	) (*mcp.CallToolResult, error) {
		args := map[string]any{}
		if m, ok := req.Params.Arguments.(map[string]any); ok {
			args = m
		}
		result, err := fn(db, args)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(result), nil
	}
}

// staticResourceHandler returns a handler that serves static content.
func staticResourceHandler(
	uri string, content string,
) server.ResourceHandlerFunc {
	return func(
		_ context.Context, req mcp.ReadResourceRequest,
	) ([]mcp.ResourceContents, error) {
		return []mcp.ResourceContents{
			mcp.TextResourceContents{
				URI:      uri,
				MIMEType: "text/plain",
				Text:     content,
			},
		}, nil
	}
}

// Tool definitions follow. Each defines parameters for its tool.

func queryTracesTool() mcp.Tool {
	return mcp.NewTool("query_traces",
		mcp.WithDescription(
			"Search traces with optional filters",
		),
		mcp.WithString("service",
			mcp.Description("Filter by service name"),
		),
		mcp.WithString("operation",
			mcp.Description("Filter by span/operation name"),
		),
		mcp.WithString("status",
			mcp.Description("Filter by status code (0,1,2)"),
		),
		mcp.WithString("since",
			mcp.Description("Start time (ISO 8601)"),
		),
		mcp.WithString("until",
			mcp.Description("End time (ISO 8601)"),
		),
		mcp.WithNumber("limit",
			mcp.Description("Max results (default 100)"),
		),
	)
}

func getTraceTool() mcp.Tool {
	return mcp.NewTool("get_trace",
		mcp.WithDescription(
			"Get all spans for a specific trace ID",
		),
		mcp.WithString("trace_id",
			mcp.Required(),
			mcp.Description("The trace ID to look up"),
		),
	)
}

func queryLogsTool() mcp.Tool {
	return mcp.NewTool("query_logs",
		mcp.WithDescription(
			"Search logs with optional filters",
		),
		mcp.WithString("service",
			mcp.Description("Filter by service name"),
		),
		mcp.WithString("severity",
			mcp.Description("Filter by severity (DEBUG, INFO, etc.)"),
		),
		mcp.WithString("body",
			mcp.Description("Search log body (case-insensitive)"),
		),
		mcp.WithNumber("limit",
			mcp.Description("Max results (default 100)"),
		),
	)
}

func queryMetricsTool() mcp.Tool {
	return mcp.NewTool("query_metrics",
		mcp.WithDescription(
			"Search metrics across gauge, sum, histogram",
		),
		mcp.WithString("name",
			mcp.Description("Filter by metric name"),
		),
		mcp.WithString("service",
			mcp.Description("Filter by service name"),
		),
		mcp.WithNumber("limit",
			mcp.Description("Max results (default 100)"),
		),
	)
}

func getErrorsTool() mcp.Tool {
	return mcp.NewTool("get_errors",
		mcp.WithDescription(
			"Get traces with error status (StatusCode=2)",
		),
		mcp.WithString("service",
			mcp.Description("Filter by service name"),
		),
		mcp.WithString("since",
			mcp.Description("Start time (ISO 8601)"),
		),
		mcp.WithString("until",
			mcp.Description("End time (ISO 8601)"),
		),
		mcp.WithNumber("limit",
			mcp.Description("Max results (default 100)"),
		),
	)
}

func latencyPercentilesTool() mcp.Tool {
	return mcp.NewTool("latency_percentiles",
		mcp.WithDescription(
			"Compute p50/p90/p95/p99 latency for spans",
		),
		mcp.WithString("operation",
			mcp.Description("Filter by span/operation name"),
		),
		mcp.WithString("service",
			mcp.Description("Filter by service name"),
		),
		mcp.WithString("since",
			mcp.Description("Start time (ISO 8601)"),
		),
		mcp.WithString("until",
			mcp.Description("End time (ISO 8601)"),
		),
	)
}

func querySqlTool() mcp.Tool {
	return mcp.NewTool("query_sql",
		mcp.WithDescription(
			"Execute raw SQL against telemetry data",
		),
		mcp.WithString("sql",
			mcp.Required(),
			mcp.Description("SQL query to execute"),
		),
		mcp.WithNumber("limit",
			mcp.Description("Max results (default 100, max 10000)"),
		),
	)
}
