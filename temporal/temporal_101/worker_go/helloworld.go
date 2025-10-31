// Copyright 2025 Canonical Ltd.
// See LICENSE file for licensing details.

package helloworld

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// HelloWorld workflow
func Workflow(ctx workflow.Context, name string) (string, error) {
	activity_options := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, activity_options)

	logger := workflow.GetLogger(ctx)
	logger.Info("Executing HelloWorld workflow", "name", name)

	var result string
	err := workflow.ExecuteActivity(ctx, Activity, name).Get(ctx, &result)
	if err != nil {
		logger.Error("HelloWorld activity failed", "error", err)
		return "", err
	}

	logger.Info("HelloWorld workflow completed", "result", result)

	return result, nil
}

// HelloWorld activity
func Activity(ctx context.Context, name string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("HelloWorld activity executing", "name", name)
	return "Hello world to " + name + " in go!", nil
}
