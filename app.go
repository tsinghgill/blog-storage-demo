package main

import (
	// Dependencies of the example data app

	// Dependencies of Turbine

	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/meroxa/turbine-go"
	"github.com/meroxa/turbine-go/runner"
)

func main() {
	runner.Start(App{})
}

var _ turbine.App = (*App)(nil)

type App struct{}

func (a App) Run(v turbine.Turbine) error {

	source, err := v.Resources("meroxas3")
	if err != nil {
		return err
	}

	rr, err := source.Records("meroxas3bucket", nil)
	if err != nil {
		return err
	}

	res := v.Process(rr, Anonymize{})

	dest, err := v.Resources("seconds3")
	if err != nil {
		return err
	}

	err = dest.Write(res, "imagesfrommeroxas3bucket")
	if err != nil {
		return err
	}

	return nil
}

type Anonymize struct{}

func (f Anonymize) Process(stream []turbine.Record) []turbine.Record {
	for i, record := range stream {
		// Convert the record struct to a JSON object
		recordJSON, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			fmt.Printf("Error converting record %d to JSON: %v\n", i+1, err)
			continue
		}

		// Decode the base64-encoded payload
		var key struct {
			Schema  struct{} `json:"schema"`
			Payload string   `json:"payload"`
		}
		if err := json.Unmarshal([]byte(record.Key), &key); err != nil {
			fmt.Printf("Error decoding payload in record %d: %v\n", i+1, err)
			continue
		}
		payload, err := base64.StdEncoding.DecodeString(key.Payload)
		if err != nil {
			fmt.Printf("Error decoding payload in record%d: %v\n", i+1, err)
			continue
		}

		// Log out the record and decoded payload
		fmt.Printf("Record %d:\n", i+1)
		fmt.Println(string(recordJSON))
		fmt.Printf("Decoded Payload for Record %d:\n", i+1)
		fmt.Println(string(payload))
	}
	return stream
}
