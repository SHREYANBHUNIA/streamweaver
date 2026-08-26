package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type Manifest struct {
	PipelineID string           `json:"pipelineId"`
	Watermark  time.Time        `json:"watermark"`
	Offsets    map[string]int64 `json:"offsets"`
	CreatedAt  time.Time        `json:"createdAt"`
}

type Manager struct{ directory string }

func New(directory string) (*Manager, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("create checkpoint directory: %w", err)
	}
	return &Manager{directory: directory}, nil
}

// Save atomically advances latest.json. Operators write durable RocksDB state
// first, then save this manifest, and only then allow Kafka offset commits.
func (m *Manager) Save(manifest Manifest) error {
	manifest.CreatedAt = time.Now().UTC()
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	temporary := filepath.Join(m.directory, ".latest.json.tmp")
	latest := filepath.Join(m.directory, "latest.json")
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, latest); err != nil {
		return err
	}
	return nil
}

func (m *Manager) Latest() (Manifest, error) {
	encoded, err := os.ReadFile(filepath.Join(m.directory, "latest.json"))
	if errors.Is(err, os.ErrNotExist) {
		return Manifest{}, nil
	}
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode checkpoint manifest: %w", err)
	}
	return manifest, nil
}

func (m *Manager) Directory() string { return m.directory }

func (m *Manager) History() ([]string, error) {
	entries, err := os.ReadDir(m.directory)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0)
	for _, entry := range entries {
		if !entry.IsDir() && entry.Name() == "latest.json" {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}
