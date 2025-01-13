package common

import "strings"

// ListMax is the largest number of objects we request from Azure in a single
// list call. This value aligns Azure driver with S3 and GCS drivers and
// enables testing/exercising of underlying Azure pagination in the List()
// function.
const ListMax = 1000

type Pather struct {
	rootDirectory string

	// The Azure driver as it was originally released did not strip the leading
	// slash from directories, resulting in a directory structure containing an
	// extra leading slash compared to other object storage drivers. For example:
	// `//docker/registry/v2`. We need to preserve this behavior by default to
	// support historical deployments of the registry using azure.
	legacyPath bool
}

func NewPather(rootDirectory string, legacyPath bool) Pather {
	return Pather{
		rootDirectory: rootDirectory,
		legacyPath:    legacyPath,
	}
}

func (d *Pather) PathToKey(path string) string {
	p := strings.TrimSpace(strings.TrimRight(d.rootDirectory+strings.TrimLeft(path, "/"), "/"))

	if d.legacyPath {
		return "/" + p
	}

	return p
}

func (d *Pather) PathToDirKey(path string) string {
	return d.PathToKey(path) + "/"
}

func (d *Pather) KeyToPath(key string) string {
	root := d.rootDirectory
	if d.legacyPath {
		root = "/" + root
	}

	return "/" + strings.Trim(strings.TrimPrefix(key, root), "/")
}

func (d *Pather) HasRootDirectory() bool {
	return d.rootDirectory != ""
}
