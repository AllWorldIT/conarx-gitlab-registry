package meta

// Blob is used to collect meta data related to blob (pulled/pushed) actions for notification events
type Blob struct {
	StorageBackend string `json:"storageBackend,omitempty"`
	Redirected     bool   `json:"redirected,omitempty"`
}
