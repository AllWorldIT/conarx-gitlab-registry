package datastore

// ConfigSizeLimit safety limit to decide when to retrieve and cache configuration payloads on database based on their size in bytes
const ConfigSizeLimit = 256000 // 256KB

// MinPostgresqlVersion defines minimal Postgresql version supported by the
// container registry/that is guaranteed to work.
const MinPostgresqlVersion = "15.0"
