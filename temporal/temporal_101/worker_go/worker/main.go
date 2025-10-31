// Copyright 2025 Canonical Ltd.
// See LICENSE file for licensing details.

package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/canonical/charmed-temporal-uats/helloworld"
)

func main() {
	host := os.Getenv("TEMPORAL_HOST")
	namespace := os.Getenv("TEMPORAL_NAMESPACE")
	taskQueue := os.Getenv("TEMPORAL_QUEUE")

	// The client and worker are heavyweight objects that should be created once per process.
	c, err := client.Dial(client.Options{
		HostPort: host,
		Namespace: namespace,
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})

	w.RegisterWorkflow(helloworld.Workflow)
	w.RegisterActivity(helloworld.Activity)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
