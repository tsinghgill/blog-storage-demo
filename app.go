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

	res := v.Process(rr, Transform{})

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

type Transform struct{}

func (f Transform) Process(stream []turbine.Record) []turbine.Record {
	var outputStream []turbine.Record

	webhookURL := "https://webhook.site/ac38ce4e-c50a-4a56-b098-8b038890d94f"
	baseURL := "https://meroxas3bucket.s3.us-east-2.amazonaws.com/"

	for i, record := range stream {
		keyPayload, err := decodeKeyPayload([]byte(record.Key))
		if err != nil {
			fmt.Printf("Error decoding key.Payload in record %d: %v\n", i+1, err)
			continue
		}
		fmt.Printf("Decoded keyPayload for Record %s:", keyPayload)

		recordPayloadUnmarshalled, err := unmarshalRecordPayload(record.Payload)
		if err != nil {
			fmt.Printf("Error Unmarshalling recordPayloadUnmarshalled in record %d: %v\n", i+1, err)
			continue
		}

		fullURL := baseURL + keyPayload
		operation := getOperation(recordPayloadUnmarshalled["payload"].(map[string]interface{})["op"].(string))

		postData := map[string]string{"url": fullURL, "operation": operation}
		jsonData, err := json.Marshal(postData)
		if err != nil {
			fmt.Printf("Error marshalling JSON data in record %d: %v\n", i+1, err)
			continue
		}

		err = sendPOSTRequest(webhookURL, jsonData)
		if err != nil {
			fmt.Printf("Error sending request for record %d: %v\n", i+1, err)
			continue
		}

		fmt.Printf("Successfully posted Record %d to webhook.site\n", i+1)

		newRecord, err := createNewRecord([]byte(record.Key), record.Timestamp, fullURL, operation)
		if err != nil {
			fmt.Printf("Error creating new record for record %d: %v\n", i+1, err)
			continue
		}

		outputStream = append(outputStream, newRecord)
	}

	return outputStream
}

func decodeKeyPayload(recordKey []byte) (string, error) {
	var key struct {
		Schema  struct{} `json:"schema"`
		Payload string   `json:"payload"`
	}
	if err := json.Unmarshal(recordKey, &key); err != nil {
		return "", err
	}
	keyPayload, err := base64.StdEncoding.DecodeString(key.Payload)
	if err != nil {
		return "", err
	}
	return string(keyPayload), nil
}

func unmarshalRecordPayload(recordPayload []byte) (map[string]interface{}, error) {
	var recordPayloadUnmarshalled map[string]interface{}
	err := json.Unmarshal(recordPayload, &recordPayloadUnmarshalled)
	if err != nil {
		return nil, err
	}
	return recordPayloadUnmarshalled, nil
}

func getOperation(operation string) string {
	switch operation {
	case "c":
		return "CREATE"
	case "u":
		return "UPDATE"
	case "d":
		return "DELETE"
	case "r":
		return "SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}

func sendPOSTRequest(webhookURL string, jsonData []byte) error {
	req, err := http.NewRequest("POST", webhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Second * 3}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-OK HTTP status: %d", resp.StatusCode)
	}

	return nil
}

func createNewRecord(recordKey []byte, recordTimestamp time.Time, fullURL string, operation string) (turbine.Record, error) {
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

	jsonValue, err := structuredPayload.AsKCSchemaJSON("resource.public.collection_name.Value")
	if err != nil {
		return turbine.Record{}, err
	}

	newRecord := turbine.Record{
		Key:       string(recordKey),
		Payload:   jsonValue,
		Timestamp: recordTimestamp,
	}

	return newRecord, nil
}
