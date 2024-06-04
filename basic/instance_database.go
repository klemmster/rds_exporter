package basic

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
)

//go:embed data/rds-max-memory.json
var databaseData embed.FS
var ErrUnknownInstanceType = errors.New("UnknownInstanceType")

// Create a singleton class to store the instance and database information
var memoryLookup map[string]float64

func init() {
	memoryLookup = make(map[string]float64)

	// Read the embedded JSON file
	data, err := databaseData.ReadFile("data/rds-max-memory.json")
	if err != nil {
		log.Fatal(err)
	}
	// Unmarshal the JSON into the map
	err = json.Unmarshal([]byte(data), &memoryLookup)
	if err != nil {
		log.Fatal(err)
	}
}

func GetInstanceMaxMemory(instance string) (float64, error) {
	i, ok := memoryLookup[instance]
	if ok {
		return i, nil
	}

	return 0.0, fmt.Errorf("%w: %s", ErrUnknownInstanceType, instance)
}
