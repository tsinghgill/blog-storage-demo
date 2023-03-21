package main

import (
	// Dependencies of the example data app
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	// Dependencies of Turbine
	"github.com/ahamidi/kcschema"
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

	rr, err := source.Records("meroxas3bucket",
		turbine.ConnectionOptions{
			{Field: "compress", Value: "true"},
		})
	if err != nil {
		return err
	}

	res := v.Process(rr, Anonymize{})

	dest, err := v.Resources("pg_db")
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
	var outputStream []turbine.Record

	webhookURL := "https://webhook.site/ac38ce4e-c50a-4a56-b098-8b038890d94f"
	baseURL := "https://meroxas3bucket.s3.us-east-2.amazonaws.com/"

	for i, record := range stream {
		// Decode the base64-encoded payload
		var key struct {
			Schema  struct{} `json:"schema"`
			Payload string   `json:"payload"`
		}
		if err := json.Unmarshal([]byte(record.Key), &key); err != nil {
			fmt.Printf("Error Unmarshalling key in record %d: %v\n", i+1, err)
			continue
		}

		keyPayload, err := base64.StdEncoding.DecodeString(key.Payload)
		// Log out the decoded keyPayload
		fmt.Printf("Decoded keyPayload for Record %s:", string(keyPayload))
		if err != nil {
			fmt.Printf("Error decoding key.Payload in record%d: %v\n", i+1, err)
			continue
		}

		var recordPayloadUnmarshalled map[string]interface{}

		if err := json.Unmarshal(record.Payload, &recordPayloadUnmarshalled); err != nil {
			fmt.Printf("Error Unmarshalling recordPayloadUnmarshalled in record%d: %v\n", i+1, err)
			continue
		}
		// fmt.Printf("recordPayloadUnmarshalled >>> %+v\n", recordPayloadUnmarshalled)
		fmt.Printf("recordPayloadUnmarshalled.op >>> %s\n", recordPayloadUnmarshalled["payload"].(map[string]interface{})["op"].(string))

		// Construct the full URL by concatenating the baseURL and the decoded payload
		fullURL := baseURL + string(keyPayload)
		operation := recordPayloadUnmarshalled["payload"].(map[string]interface{})["op"].(string)

		switch operation {
		case "c":
			operation = "CREATE"
		case "u":
			operation = "UPDATE"
		case "d":
			operation = "DELETE"
		case "r":
			operation = "SNAPSHOT"
		default:
			operation = "UNKNOWN"
		}

		// Create a map with a single key-value pair, where the key is "url" and the value is the fullURL
		postData := map[string]string{"url": fullURL, "operation": operation}

		// Marshal the postData map into JSON data
		jsonData, err := json.Marshal(postData)
		if err != nil {
			fmt.Printf("Error marshalling JSON data in record %d: %v\n", i+1, err)
			continue
		}

		// Create a new POST request with the webhookURL and jsonData as the request body
		req, err := http.NewRequest("POST", webhookURL, bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("Error creating request for record %d: %v\n", i+1, err)
			continue
		}
		// Set the request's Content-Type header to "application/json"
		req.Header.Set("Content-Type", "application/json")

		// Create an HTTP client with a 10-second timeout
		client := &http.Client{Timeout: time.Second * 3}

		// Send the POST request using the client
		fmt.Printf("Sending req to webhook.site: %v\n", req)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error sending request for record %d: %v\n", i+1, err)
			continue
		}
		// Close the response body to prevent resource leaks
		defer resp.Body.Close()

		// Check if the response status code is OK (200)
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Error: non-OK HTTP status for record %d: %d\n", i+1, resp.StatusCode)
			continue
		}

		// Print a message indicating that the record was successfully posted to webhook.site
		fmt.Printf("Successfully posted Record %d to webhook.site\n", i+1)

		// Create the desired schema and payload structures
		structuredPayload := kcschema.StructuredPayload{
			"url": kcschema.Field{
				Type:  kcschema.StringField,
				Value: fullURL,
			},
			"operation": kcschema.Field{
				Type:  kcschema.StringField,
				Value: operation,
			},
		}

		// Marshal the StructuredPayload into a JSON array of bytes using AsKCSchemaJSON
		jsonValue, err := structuredPayload.AsKCSchemaJSON("resource.public.collection_name.Value")
		if err != nil {
			fmt.Printf("Error marshalling new value in record: %v\n", err)
			continue
		}

		// Create a new turbine.Record object with the desired structure
		newRecord := turbine.Record{
			Key:       record.Key,
			Payload:   jsonValue,
			Timestamp: record.Timestamp,
		}

		// Add the new record to the outputStream
		outputStream = append(outputStream, newRecord)
	}

	return outputStream
}
