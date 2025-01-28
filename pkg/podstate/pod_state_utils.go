package podstate

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type SizeEntry struct {
	ID   string
	Size string
}

func parseSizeEntry(entry string) (*SizeEntry, error) {
	parts := strings.Split(entry, "=")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid entry format: %s", entry)
	}
	return &SizeEntry{
		ID:   parts[0],
		Size: parts[1],
	}, nil
}

// Sorts data inputted from vdisk_usage according to size and returns CVM with large size extents.
func extractCVMidBasedOnSize(data string) (string, error) {
	entries := strings.Split(data, "\n")

	var sizeEntries []*SizeEntry
	for _, entry := range entries {
		if entry != "" {
			sizeEntry, err := parseSizeEntry(entry)
			if err != nil {
				fmt.Println("Error parsing entry:", err)
				continue
			}
			sizeEntries = append(sizeEntries, sizeEntry)
		}
	}

	// Sort sizeEntries based on size
	sort.Slice(sizeEntries, func(i, j int) bool {
		size1, _ := strconv.ParseFloat(strings.TrimSuffix(sizeEntries[i].Size, " GB"), 64)
		size2, _ := strconv.ParseFloat(strings.TrimSuffix(sizeEntries[j].Size, " GB"), 64)
		return size1 > size2 // Sort in descending order (largest size first)
	})

	return sizeEntries[0].ID, nil
}
