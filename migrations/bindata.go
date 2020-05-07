package migrations

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
)

func bindata_read(data []byte, name string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, gz)
	gz.Close()

	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	return buf.Bytes(), nil
}

var __20200319122755_create_repositories_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\x28\x4a\x2d\xc8\x2f\xce\x2c\xc9\x2f\xca\x4c\x2d\xb6\x06\x04\x00\x00\xff\xff\x3c\x53\x72\x9a\x22\x00\x00\x00")

func _20200319122755_create_repositories_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319122755_create_repositories_table_down_sql,
		"20200319122755_create_repositories_table.down.sql",
	)
}

var __20200319122755_create_repositories_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x94\x91\x41\x6f\xa3\x30\x10\x85\xef\xfc\x8a\x77\x04\x69\x4f\x2b\xe5\xb4\xbb\x07\x07\x86\xac\x15\x6a\x5a\x63\xa4\x72\x42\x34\xb8\xc1\x6a\x02\x94\xb8\x4a\xd5\x5f\x5f\x01\x4d\xd2\x84\x48\x6d\xe7\xc6\xe0\xef\xbd\x37\x33\xbe\x24\xa6\x08\x8a\xcd\x23\x02\x0f\x21\x62\x05\xba\xe7\x89\x4a\xd0\xe9\xb6\xd9\x19\xdb\x74\x46\xef\x1c\xd7\x01\x00\x53\xe2\x50\x0f\x66\x6d\x6a\x8b\x69\xf5\x0a\x22\x8d\x22\x2c\x48\x90\x64\x8a\x02\xcc\x33\x04\x14\xb2\x34\x52\x60\x09\x78\x40\x42\x71\x95\xfd\x1a\x24\xeb\x62\xab\x3f\x48\xab\x5f\xaf\x09\x9e\x24\x47\xa2\x2d\x6c\xf5\x53\xa2\xd3\xb5\xcd\xfb\xf4\x63\xec\xb1\xbd\xea\x74\x61\x75\x99\x17\x16\xd6\x6c\xf5\xce\x16\xdb\x16\x7b\x63\xab\xe1\x13\x6f\x4d\xad\x4f\xd3\x1c\x06\xa8\x9b\xbd\xeb\x8d\x7c\xa9\x37\xfa\x0b\x7e\x7c\xe8\xc7\x22\x51\x92\x71\xa1\xd0\x3e\xe5\x9f\xf7\x8a\x5b\xc9\x6f\x98\xcc\xb0\xa4\x0c\xae\x29\xbd\x09\xf0\x78\x0e\xe4\xa7\x59\xc2\x58\x12\x5f\x88\x11\x3d\xb6\x3d\xe7\xb0\x03\x49\x21\x49\x12\x3e\x9d\x9f\x72\xb0\x41\x2c\x10\x50\x44\x8a\xe0\xb3\xc4\x67\x01\x4d\x8c\x5f\x9e\x2f\x8d\x6d\x85\x54\xf0\xbb\x94\x7a\x3b\x5b\x4d\xb3\xae\xaa\x8b\xb0\xc3\x71\xfd\xff\xe4\x2f\xe1\xba\xab\xaa\xe8\xf2\x8d\xae\xd7\xb6\x72\xfb\x1f\x1e\xfe\xfe\xc3\xef\xd9\xcc\xfb\x86\xd0\x60\x7e\x4d\x68\x08\x72\x14\x72\xbc\x3f\xef\x01\x00\x00\xff\xff\xf5\xfa\xaa\x5d\xd1\x02\x00\x00")

func _20200319122755_create_repositories_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319122755_create_repositories_table_up_sql,
		"20200319122755_create_repositories_table.up.sql",
	)
}

var __20200319130108_create_manifests_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x29\xb6\x06\x04\x00\x00\xff\xff\x22\x39\x5a\x0e\x1f\x00\x00\x00")

func _20200319130108_create_manifests_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319130108_create_manifests_table_down_sql,
		"20200319130108_create_manifests_table.down.sql",
	)
}

var __20200319130108_create_manifests_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x8c\x51\xc1\x4e\xeb\x30\x10\xbc\xf7\x2b\xf6\xe8\x48\xef\xf4\xa4\x9e\x80\x83\x9b\xba\x60\x35\xb8\x90\x38\x12\x39\x59\x26\x59\x12\xab\x8d\x13\x12\x43\x5b\xbe\x1e\xd1\x94\x26\x22\x54\x74\x6f\xf6\xce\xcc\xee\xcc\xfa\x21\xa3\x92\x81\xa4\xb3\x80\x01\x5f\x80\x58\x49\x60\x4f\x3c\x92\x11\x94\xda\x9a\x17\x6c\x5d\x3b\x21\x13\x00\x00\x93\xc1\xb0\x9e\x4d\x6e\xac\x83\x71\x7d\x49\x88\x38\x08\xe0\x96\x09\x16\x52\xc9\xe6\x30\x4b\x60\xce\x16\x34\x0e\x24\xd0\x08\xf8\x9c\x09\xc9\x65\xf2\xef\x20\xdb\xa6\x05\x96\x5a\xbd\x63\xd3\x9a\xca\x82\xb1\x0e\x73\x6c\xce\xcb\x76\xac\x12\x33\xa3\x95\xdb\xd7\x78\x68\x3a\xdc\xfd\xb6\xca\x4f\x56\x66\x72\x6c\x9d\x2a\x70\xd7\x59\xd8\x3b\xd4\x7f\xb3\x6a\xbd\xdf\x54\xfa\xe4\xfe\x42\x56\xda\xa0\x76\x98\x29\xdd\x2d\xe6\x4c\x89\xad\xd3\x65\x0d\x5b\xe3\x8a\xc3\x13\x3e\x2a\x8b\x7d\x5c\xdf\x09\xd9\x6a\x4b\xbc\xa3\x4b\xdd\xac\x7b\x89\xb3\x1a\x47\x73\xb8\xc1\x0b\x06\x76\x60\x7f\x25\x22\x19\x52\x2e\x24\xd4\x6b\x75\x3a\x35\x3c\x84\xfc\x9e\x86\x09\x2c\x59\x02\xc4\x64\xde\x08\xfd\xf6\xda\xa3\xd5\x20\xcf\x58\xf0\xc7\x98\x01\xe9\xbf\xc6\xdc\xb4\x18\x8c\x52\x83\x13\xfa\x77\xcc\x5f\x02\x21\x69\xa1\x1b\xb5\x41\x9b\xbb\x82\xf4\x6d\x0f\xae\x6f\xe0\xff\x74\xea\x79\x13\xef\xea\x33\x00\x00\xff\xff\xf7\xbb\x42\x13\xb1\x02\x00\x00")

func _20200319130108_create_manifests_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319130108_create_manifests_table_up_sql,
		"20200319130108_create_manifests_table.up.sql",
	)
}

var __20200319131222_create_manifest_configurations_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x89\x4f\xce\xcf\x4b\xcb\x4c\x2f\x2d\x4a\x2c\xc9\xcc\xcf\x2b\xb6\x06\x04\x00\x00\xff\xff\xd0\x67\xff\x10\x2d\x00\x00\x00")

func _20200319131222_create_manifest_configurations_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319131222_create_manifest_configurations_table_down_sql,
		"20200319131222_create_manifest_configurations_table.down.sql",
	)
}

var __20200319131222_create_manifest_configurations_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x8c\x92\xc1\x6e\x9c\x30\x18\x84\xef\x3c\xc5\x1c\x41\xea\xa9\x52\x4e\x6d\x0f\x0e\xfc\xa4\x56\xa8\x69\x8d\x91\xca\x09\x39\xe0\x05\xab\xbb\x40\x17\x47\xc9\xe6\xe9\xab\x2c\xd9\x80\x4a\x36\x1b\xdf\xc0\x9e\x6f\x3c\xf3\x3b\x94\xc4\x14\x41\xb1\xeb\x84\xc0\x63\x88\x54\x81\x7e\xf3\x4c\x65\xd8\xe9\xce\x6e\xcc\xe8\xca\xaa\xef\x36\xb6\xb9\xdf\x6b\x67\xfb\x6e\xf4\x7c\x0f\x00\x6c\x8d\xd7\x75\x67\x1b\xdb\x39\xac\xd7\x33\x4d\xe4\x49\x82\x1b\x12\x24\x99\xa2\x08\xd7\x05\x22\x8a\x59\x9e\x28\xb0\x0c\x3c\x22\xa1\xb8\x2a\x3e\x1d\x99\xaf\x8e\xb6\xbe\xcc\x7c\x91\x98\xda\xea\xd2\x1d\x06\x03\x38\xf3\xf8\x96\xe0\x7f\x49\x6d\x9b\x67\x8f\xd6\x3c\x02\x77\x07\x67\xf4\x65\xc9\x68\x9f\xcc\x87\xc3\x4e\x92\x41\x1f\xb6\xbd\x9e\x4a\xfa\xa0\x4b\xb5\x37\xda\x99\xba\xd4\x0e\x70\x76\x67\x46\xa7\x77\x03\x1e\xac\x6b\x8f\x9f\x78\xea\x3b\x33\x57\x7a\x6a\xb1\xeb\x1f\xfc\xe0\x25\x99\xd9\x9a\x4b\x80\xe9\x64\x98\x8a\x4c\x49\xc6\x85\xc2\xf0\xa7\x3c\x33\x69\xfc\x94\xfc\x07\x93\x05\x6e\xa9\x80\x6f\xeb\x60\xa5\xdd\x9c\xd5\x96\xcb\x59\xc6\xa9\x24\x7e\x23\x26\xce\x62\x23\xf0\x4e\x45\x48\x8a\x49\x92\x08\x69\x7e\x76\xe3\xd1\x12\xa9\x40\x44\x09\x29\x42\xc8\xb2\x90\x45\xb4\xba\xc4\xfd\xdf\xb3\x97\x58\x8c\x3a\x17\xfc\x57\x4e\xf0\xe7\x5f\xeb\x38\xef\x90\x96\x71\x4e\xa8\x65\x92\x15\xab\x6a\xdf\xe9\x66\x7e\xb4\xe1\x77\x0a\x6f\xe1\xfb\x55\xab\xf7\xe5\xd6\x74\x8d\x6b\xfd\x79\x3b\xc0\xd7\x6f\xf8\x7c\x75\x15\x04\x5e\xf0\xe5\x5f\x00\x00\x00\xff\xff\xf0\x34\xc5\x0f\xab\x03\x00\x00")

func _20200319131222_create_manifest_configurations_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319131222_create_manifest_configurations_table_up_sql,
		"20200319131222_create_manifest_configurations_table.up.sql",
	)
}

var __20200319131542_create_layers_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x49\xac\x4c\x2d\x2a\xb6\x06\x04\x00\x00\xff\xff\x04\xc2\x07\x1b\x1c\x00\x00\x00")

func _20200319131542_create_layers_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319131542_create_layers_table_down_sql,
		"20200319131542_create_layers_table.down.sql",
	)
}

var __20200319131542_create_layers_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x8c\x91\xcd\x6e\xc2\x30\x10\x84\xef\x3c\xc5\x1c\x6d\xa9\xa7\x4a\x9c\xda\x1e\x4c\x30\xad\x45\x6a\xda\xc4\x91\x9a\x93\x65\xc8\x8a\x58\x40\xa0\x89\x2b\x7e\x9e\xbe\x2a\x01\x05\x89\x56\x74\x6f\xb6\xe7\x9b\xf5\xec\x46\x89\x14\x46\xc2\x88\x41\x2c\xa1\x46\xd0\x13\x03\xf9\xa1\x52\x93\x62\xe9\xf6\x54\x37\x3d\xd6\x03\x00\x5f\xe0\x5c\x53\x3f\xf7\x55\xc0\x75\xfd\xb0\x3a\x8b\x63\x3c\x4b\x2d\x13\x61\xe4\x10\x83\x1c\x43\x39\x12\x59\x6c\x20\x52\xa8\xa1\xd4\x46\x99\xfc\xee\x68\xb9\xa2\xc2\x3b\x1b\xf6\x1b\x42\xa0\xdd\x6f\x86\x9d\x65\x4b\x14\x7e\x4e\x4d\xb0\x25\xed\x30\xdd\x07\x72\xb7\x89\xc6\x1f\xe8\xbf\xdf\x6e\x89\x59\x4d\x2e\x50\x61\x5d\x40\xf0\x2b\x6a\x82\x5b\x6d\xb0\xf5\xa1\x3c\x1e\x71\x58\x57\xd4\x05\x3d\x67\xab\xd6\x5b\xc6\x4f\xa9\x5c\xbd\x68\xf1\x3f\xf9\x53\x18\x5a\xd2\x8d\x46\xad\x30\x9a\xe8\xd4\x24\x42\x69\x83\xcd\xc2\xb6\x5b\xc1\x5b\xa2\x5e\x45\x92\x63\x2c\x73\x30\x5f\xf0\x2b\xe9\xd7\xe7\x49\x6a\x2f\xa6\x96\x69\xf5\x9e\x49\xb0\xee\xea\x1a\x9c\x95\xe7\x26\xf6\x62\x43\xd1\x8b\x8c\xc6\x60\x6c\x56\xba\xda\x2e\xa9\x9a\x87\x92\x75\xcf\x1c\x8f\x4f\xb8\xef\xf7\x39\xef\xf1\x87\xef\x00\x00\x00\xff\xff\x68\xb4\xd0\x44\x53\x02\x00\x00")

func _20200319131542_create_layers_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319131542_create_layers_table_up_sql,
		"20200319131542_create_layers_table.up.sql",
	)
}

var __20200319131632_create_manifest_layers_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x89\xcf\x49\xac\x4c\x2d\x2a\xb6\x06\x04\x00\x00\xff\xff\xda\x9b\xec\x68\x25\x00\x00\x00")

func _20200319131632_create_manifest_layers_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319131632_create_manifest_layers_table_down_sql,
		"20200319131632_create_manifest_layers_table.down.sql",
	)
}

var __20200319131632_create_manifest_layers_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x8c\x91\xc1\x4e\xb3\x40\x14\x85\xf7\x7d\x8a\xb3\x2c\x49\xdf\xe0\x5f\x4d\xe1\xd2\x4c\x7e\x1c\x74\xb8\x24\xb2\x22\x28\x53\x9d\x58\x68\x2d\x63\x1a\x7d\x7a\x23\x08\xb4\x62\x2c\x77\x07\x33\xe7\x7c\xe7\xdc\xf1\x35\x09\x26\xb0\x58\x47\x04\x19\x42\xc5\x0c\xba\x97\x09\x27\xa8\x8a\xda\x6e\x4d\xe3\xf2\x5d\xf1\x6e\x8e\xcd\x62\xb9\x00\x00\x5b\x62\x98\x07\xfb\x64\x6b\x87\xe9\x7c\xb9\xa8\x34\x8a\xb0\x21\x45\x5a\x30\x05\x58\x67\x08\x28\x14\x69\xc4\x10\x09\x64\x40\x8a\x25\x67\xab\xd6\x73\x20\xd9\xf2\xba\x67\x27\x69\x33\xe5\x5d\x98\x99\x92\xc7\xa3\x29\x9c\x29\xf3\xc2\x01\xce\x56\xa6\x71\x45\x75\xc0\xc9\xba\xe7\xf6\x13\x1f\xfb\xda\x8c\xc9\xfb\xb0\xf5\xfe\xb4\xf4\x3a\x83\xd2\xec\xcc\x35\x83\xee\xa6\x1f\xab\x84\xb5\x90\x8a\x71\x78\xc9\x7f\x2c\x12\xb7\x5a\xde\x08\x9d\xe1\x3f\x65\x58\xda\xd2\x9b\x68\xb6\x13\x4d\x7e\xbe\xa2\x30\xd6\x24\x37\xaa\xd3\x9f\x1d\x78\x8b\xbe\xb8\xa6\x90\x34\x29\x9f\xc6\x57\x6c\x5a\x14\x62\x85\x80\x22\x62\x82\x2f\x12\x5f\x04\x34\x07\x3e\x2c\xfb\x82\xdc\xff\xfd\x15\xfb\x5d\x75\x26\xf3\xed\xf5\xaf\xc2\x23\x3f\x55\xf2\x2e\xa5\x8b\xd2\x2b\x8c\x39\xbc\x7f\x9f\x01\x00\x00\xff\xff\xcf\x49\xc2\x0b\xcf\x02\x00\x00")

func _20200319131632_create_manifest_layers_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319131632_create_manifest_layers_table_up_sql,
		"20200319131632_create_manifest_layers_table.up.sql",
	)
}

var __20200319131907_create_manifest_lists_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x89\xcf\xc9\x2c\x2e\x29\xb6\x06\x04\x00\x00\xff\xff\xce\xc8\x77\x1d\x24\x00\x00\x00")

func _20200319131907_create_manifest_lists_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319131907_create_manifest_lists_table_down_sql,
		"20200319131907_create_manifest_lists_table.down.sql",
	)
}

var __20200319131907_create_manifest_lists_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x8c\x91\x41\x53\x83\x30\x10\x85\xef\xfd\x15\x7b\x0c\x33\x9e\x9c\xe9\x49\x3d\xa4\x34\xd5\x4c\x31\x55\x08\x33\x72\xca\x44\x58\x21\x53\x08\x08\xd1\xb6\xfe\x7a\xa7\xa5\x4a\x6d\xed\x4c\xf7\x06\x9b\xf7\xed\xbe\xb7\x7e\xc8\xa8\x64\x20\xe9\x24\x60\xc0\x67\x20\x16\x12\xd8\x0b\x8f\x64\x04\x95\xb6\xe6\x0d\x3b\xa7\x4a\xd3\xb9\x6e\x44\x46\x00\x00\x26\x83\xc3\x7a\x35\xb9\xb1\x0e\x4e\x6b\xcb\x11\x71\x10\xc0\x3d\x13\x2c\xa4\x92\x4d\x61\x92\xc0\x94\xcd\x68\x1c\x48\xa0\x11\xf0\x29\x13\x92\xcb\xe4\x6a\x87\xed\xd2\x02\x2b\xad\x3e\xb1\xed\x4c\x6d\xc1\x58\x87\x39\xb6\xe7\xb1\xbd\xaa\xc2\xcc\x68\xe5\x36\x0d\xee\x9a\x0e\xd7\xae\x6f\x64\x26\xdf\x2e\x5e\xe0\xba\xdf\x72\xe3\x50\xff\xb3\xe4\x11\xae\xd1\x9b\xb2\xd6\xbf\x06\x2f\x54\xa5\x2d\x6a\x87\x99\xd2\x7d\x0c\xce\x54\xd8\x39\x5d\x35\xb0\x32\xae\xd8\x7d\xc2\x57\x6d\x71\x48\xe4\x27\x04\x5b\xaf\x88\xb7\x37\xa2\xdb\xe5\x80\x38\xcb\xd8\x9b\xc3\x12\x2f\x18\xd8\x3f\xf6\x17\x22\x92\x21\xe5\x42\x42\xb3\x54\x7f\x4f\x0a\x4f\x21\x7f\xa4\x61\x02\x73\x96\x00\x31\x99\x77\x22\xf9\x78\x3f\x92\xa8\x83\x64\x63\xc1\x9f\x63\x06\x64\xf8\x75\x0a\x48\x8b\xe3\xa1\xea\xe0\x68\xfe\x03\xf3\xe7\x40\x48\x5a\xe8\x56\x95\x68\x73\x57\x90\xa1\xed\xc1\xed\x1d\x5c\x8f\xc7\x9e\x37\xf2\x6e\xbe\x03\x00\x00\xff\xff\xf8\xdb\xef\x60\xa8\x02\x00\x00")

func _20200319131907_create_manifest_lists_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319131907_create_manifest_lists_table_up_sql,
		"20200319131907_create_manifest_lists_table.up.sql",
	)
}

var __20200319132010_create_manifest_list_items_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x89\xcf\xc9\x2c\x2e\x89\xcf\x2c\x49\xcd\x2d\xb6\x06\x04\x00\x00\xff\xff\xcf\x55\xcb\x31\x29\x00\x00\x00")

func _20200319132010_create_manifest_list_items_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319132010_create_manifest_list_items_table_down_sql,
		"20200319132010_create_manifest_list_items_table.down.sql",
	)
}

var __20200319132010_create_manifest_list_items_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x94\x91\x51\x4f\x83\x30\x14\x85\xdf\xf7\x2b\xce\xe3\x48\xf6\x0f\x7c\xea\xe0\xb2\x34\x62\xd1\x72\x49\xe4\x89\xa0\xed\xb4\x71\xb0\x29\x35\x4b\xfc\xf5\x46\x08\x31\xc0\xcc\xf0\xbe\xb5\xbd\xe7\xdc\xaf\xe7\x86\x9a\x04\x13\x58\x6c\x13\x82\x8c\xa1\x52\x06\x3d\xca\x8c\x33\xd4\x55\xe3\xf6\xb6\xf5\xe5\xc1\xb5\xbe\x74\xde\xd6\xed\x6a\xbd\x02\x00\x67\x30\xae\x27\xf7\xe2\x1a\x8f\x79\xfd\xd8\xa9\x3c\x49\xb0\x23\x45\x5a\x30\x45\xd8\x16\x88\x28\x16\x79\xc2\x10\x19\x64\x44\x8a\x25\x17\x9b\xce\x78\x32\xd2\x5c\x37\x9e\xe8\x06\xb2\x85\xba\xe7\x0f\x5b\x79\x6b\xca\x6a\x68\xf5\xae\xb6\xad\xaf\xea\x13\xce\xce\xbf\x76\x47\x7c\x1d\x1b\xfb\xfb\x91\x81\xbd\x39\x9e\xd7\x41\xef\x62\xec\xc1\x2e\x72\xe9\xdb\xc3\x54\x65\xac\x85\x54\x8c\xd3\x5b\x79\x21\x65\xdc\x6b\x79\x27\x74\x81\x5b\x2a\xb0\x76\x26\x98\xe9\xf6\x17\x75\xd3\x3b\x83\x38\xd5\x24\x77\xaa\x37\x9a\xbe\x06\xab\x21\x14\x4d\x31\x69\x52\x21\x4d\x96\xde\x76\xd3\x91\x2a\x44\x94\x10\x13\x42\x91\x85\x22\xa2\x7f\xf3\xfc\x89\x72\x85\x62\x31\xc0\xe7\xfb\xa2\x40\x46\x44\xb9\x92\x0f\x39\xcd\x73\xd9\x60\x84\x17\xdc\x7c\x07\x00\x00\xff\xff\x9d\xc7\xd6\xee\x24\x03\x00\x00")

func _20200319132010_create_manifest_list_items_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319132010_create_manifest_list_items_table_up_sql,
		"20200319132010_create_manifest_list_items_table.up.sql",
	)
}

var __20200319132237_create_tags_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\x28\x49\x4c\x2f\xb6\x06\x04\x00\x00\xff\xff\x83\x0d\x99\xe1\x1a\x00\x00\x00")

func _20200319132237_create_tags_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319132237_create_tags_table_down_sql,
		"20200319132237_create_tags_table.down.sql",
	)
}

var __20200319132237_create_tags_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x9c\x53\xc1\x8e\x9b\x30\x14\xbc\xf3\x15\x73\xc4\xd2\x9e\x2a\xed\xa9\xed\xc1\x0b\x2f\x5b\x6b\xa9\x69\x8d\x91\x9a\x13\xa2\xc1\x9b\x58\x9b\x90\x34\x78\xb5\x6d\xbf\xbe\x02\x2f\x02\x42\x9a\xa6\xeb\x1b\x9a\x79\x33\x8f\x19\x3b\x52\xc4\x35\x41\xf3\xbb\x84\x20\x16\x90\xa9\x06\x7d\x13\x99\xce\xe0\xca\x75\x13\x84\x01\x00\xd8\x0a\xd3\xf3\xdd\xae\x6d\xed\x30\x3f\xed\xbc\xcc\x93\x04\xf7\x24\x49\x71\x4d\x31\xee\x96\x88\x69\xc1\xf3\x44\x83\x67\x10\x31\x49\x2d\xf4\xf2\xa6\x13\xae\xcb\x9d\x99\xcc\x3b\xf3\xf3\x9c\xec\x20\xec\xe7\x8e\xe6\xb0\x6f\xac\xdb\x1f\x7f\x15\x7e\xb7\x7f\x2e\xe4\xe7\x76\x65\x6d\x1f\x4d\xe3\x8a\xfe\x8f\xfc\xdc\x09\xb8\xb5\x9e\x31\x06\x57\x47\x53\x3a\x53\x15\x65\x6f\xe2\xec\xce\x34\xae\xdc\x1d\xf0\x62\xdd\xa6\xfb\xc4\xef\x7d\x6d\x86\x08\xfa\xbf\xae\xf7\x2f\x21\xf3\x2a\xcf\x87\xea\x3a\x15\x4f\xaf\xcc\xd6\xfc\x07\x3d\x4a\x65\xa6\x15\x17\x52\xe3\xf0\x54\xb4\xfd\xe1\x8b\x12\x9f\xb9\x5a\xe2\x81\x96\x08\x6d\xc5\x66\xc4\x47\x4f\x2c\xa6\x89\x2e\x52\x45\xe2\x5e\xfa\xb1\x09\xc4\x82\x3e\x59\x45\x0b\x52\x24\x23\xca\x86\x3a\xac\x69\x3a\x1b\xa4\x12\x31\x25\xa4\x09\x11\xcf\x22\x1e\xd3\x5f\x8d\xc7\x95\x4c\x6c\x47\xc0\x59\xd3\x1e\x7f\xbb\x63\xdf\xf3\x79\xdb\x57\xf4\xa2\x77\x47\xba\x7a\x81\xe7\x1f\x7e\x81\xf6\xd6\x9f\x04\x9e\x4b\xf1\x35\x27\x84\x2d\x74\x33\xbd\xde\xf3\xce\x56\x9b\xa7\x41\x08\xd1\x27\x8a\x1e\x10\x86\xab\x4d\x79\x2c\xb6\xa6\x5e\xbb\x4d\x27\xc3\xf0\xe1\x23\xde\xdd\xde\xb2\x0b\x02\xa3\x8c\xe7\xb1\xf4\xc2\xe3\x26\x20\xb2\xe1\x82\x73\x19\xcf\x1f\x4d\x4b\xc8\x93\x84\x21\x55\xc1\xd9\x97\x7c\xed\x99\xd9\x5e\xb4\x7c\xdd\x89\x31\x16\xb0\xf7\x7f\x02\x00\x00\xff\xff\x7c\x14\xd0\x62\xd6\x04\x00\x00")

func _20200319132237_create_tags_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319132237_create_tags_table_up_sql,
		"20200319132237_create_tags_table.up.sql",
	)
}

var __20200408192311_create_repository_manifests_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\x28\x4a\x2d\xc8\x2f\xce\x2c\xc9\x2f\xaa\x8c\xcf\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x29\xb6\x06\x04\x00\x00\xff\xff\xf7\xde\xbc\xab\x2a\x00\x00\x00")

func _20200408192311_create_repository_manifests_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200408192311_create_repository_manifests_table_down_sql,
		"20200408192311_create_repository_manifests_table.down.sql",
	)
}

var __20200408192311_create_repository_manifests_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x8c\x91\x41\x4f\x83\x40\x10\x85\xef\xfd\x15\xef\x58\x92\xfe\x89\x2d\x0c\xcd\x46\x5c\x74\x19\x12\x39\x11\x94\xad\x6e\xb4\x80\xb0\xa6\xd1\x5f\x6f\x84\x90\x82\x25\x29\x73\x9b\xcc\xbc\x37\x5f\xde\xf8\x9a\x04\x13\x58\xec\x23\x82\x0c\xa1\x62\x06\x3d\xc9\x84\x13\xb4\xa6\xa9\x3b\xeb\xea\xf6\x3b\x3f\x15\x95\x3d\x9a\xce\x75\x9b\xed\x06\x00\x6c\x89\x49\x3d\xdb\x57\x5b\x39\x5c\xd7\x9f\x99\x4a\xa3\x08\x07\x52\xa4\x05\x53\x80\x7d\x86\x80\x42\x91\x46\x0c\x91\x40\x06\xa4\x58\x72\xb6\xeb\x5d\x27\x07\x6d\x79\xdb\x75\x10\x8d\x68\x79\xcf\xb4\x52\xf4\xd2\x9a\xc2\x99\x32\x2f\xfa\x55\x67\x4f\xa6\x73\xc5\xa9\xc1\xd9\xba\xb7\xbe\xc5\x4f\x5d\x99\x0b\xff\x88\x5c\xd5\xe7\xad\x37\x58\x94\xe6\xc3\xdc\xb6\x18\x76\xfd\x58\x25\xac\x85\x54\x8c\xe6\x3d\x5f\xca\x15\x0f\x5a\xde\x0b\x9d\xe1\x8e\x32\x6c\x6d\xe9\x5d\x09\x8f\xcb\xc2\x7c\x1e\x5a\x18\x6b\x92\x07\x35\xd8\xcc\x46\xde\x66\x8c\x42\x53\x48\x9a\x94\x4f\x93\x17\x5b\xd3\xf5\x67\x11\x2b\x04\x14\x11\x13\x7c\x91\xf8\x22\xa0\xd5\x20\xd3\x47\xcc\x30\x26\x83\x45\x88\x4b\x08\x2b\x09\xbe\x3e\x57\x44\x31\xe3\x49\x95\x7c\x4c\xe9\x5f\x22\x3b\xcc\xc8\xbc\xdf\x00\x00\x00\xff\xff\xed\x7f\x49\x76\x0b\x03\x00\x00")

func _20200408192311_create_repository_manifests_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200408192311_create_repository_manifests_table_up_sql,
		"20200408192311_create_repository_manifests_table.up.sql",
	)
}

var __20200408193126_create_repository_manifest_lists_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\x28\x4a\x2d\xc8\x2f\xce\x2c\xc9\x2f\xaa\x8c\xcf\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x89\xcf\xc9\x2c\x2e\x29\xb6\x06\x04\x00\x00\xff\xff\xd8\xa5\xd5\x69\x2f\x00\x00\x00")

func _20200408193126_create_repository_manifest_lists_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200408193126_create_repository_manifest_lists_table_down_sql,
		"20200408193126_create_repository_manifest_lists_table.down.sql",
	)
}

var __20200408193126_create_repository_manifest_lists_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x94\x92\xd1\x4e\xb3\x40\x10\x85\xef\x79\x8a\x73\x59\x92\xbe\xc1\x7f\xb5\x85\xa1\xd9\xfc\xb8\xe8\xb2\x44\xb9\x22\x28\x5b\xdd\xd8\x16\x64\xd7\x18\x7d\x7a\x53\x08\xb1\x40\x55\x9c\x3b\x32\x73\xce\x7c\x9c\x9d\x40\x12\x53\x04\xc5\x36\x31\x81\x47\x10\x89\x02\xdd\xf1\x54\xa5\x68\x75\x53\x5b\xe3\xea\xf6\xbd\x38\x94\x47\xb3\xd3\xd6\x15\x7b\x63\x9d\xf5\x56\x1e\x00\x98\x0a\xe3\xba\x37\x8f\xe6\xe8\x30\xaf\x93\xa9\xc8\xe2\x18\x5b\x12\x24\x99\xa2\x10\x9b\x1c\x21\x45\x2c\x8b\x15\x58\x0a\x1e\x92\x50\x5c\xe5\xeb\xce\xf8\x6c\x71\xbf\xe3\x57\xe3\x5e\x37\xa2\x3c\x49\x17\xea\x1e\x5a\x5d\x3a\x5d\x15\xe5\x30\xea\xcc\x41\x5b\x57\x1e\x1a\xbc\x19\xf7\xd4\x7d\xe2\xa3\x3e\xea\xaf\x1f\x19\xd8\x45\x72\xbb\xf2\x7b\x97\x4a\xef\xf5\x22\x97\x7e\x3c\x48\x44\xaa\x24\xe3\x42\xa1\x79\x2e\xbe\xcd\x1a\xd7\x92\x5f\x31\x99\xe3\x3f\xe5\x58\x99\xca\x9f\xa9\x77\x3f\xa8\x8b\x71\x94\x51\x22\x89\x6f\x45\xef\x35\x6a\xf9\xde\x10\x8d\xa4\x88\x24\x89\x80\xce\x0e\xc0\x68\xdb\xed\x46\x22\x10\x52\x4c\x8a\x10\xb0\x34\x60\x21\xfd\x8d\x66\xf6\x40\x23\xa0\x69\xf7\x22\xd3\x24\x9d\x85\x54\xaf\x2f\x97\xa8\x26\xf1\xcc\xf1\x32\xc1\x6f\x32\x9a\x44\xb5\x9e\xdd\x99\xef\xf9\xff\x3e\x03\x00\x00\xff\xff\xb0\x63\xc3\x78\x48\x03\x00\x00")

func _20200408193126_create_repository_manifest_lists_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200408193126_create_repository_manifest_lists_table_up_sql,
		"20200408193126_create_repository_manifest_lists_table.up.sql",
	)
}

var __20200428184744_create_foreign_key_indexes_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xa4\x91\xb1\x0e\x82\x30\x10\x86\x77\x9f\xa2\xef\xc1\xa6\xa0\xe9\x00\x18\x61\x60\xbb\x34\x7a\x90\x8b\xd2\x92\xde\x39\xf0\xf6\x46\x34\xc8\x00\xa6\xe8\xd4\xb4\xe9\xf7\xdf\xd7\xbf\xdb\xe4\xa0\xb3\x68\x13\x9f\xf2\xa3\xd2\x59\x9c\x54\x4a\xef\x55\x52\xe9\xa2\x2c\x94\xc7\xce\x31\x89\xf3\x84\x0c\x9d\xf1\x68\x05\xe8\x02\xf5\x15\xfb\x05\xa2\x35\x96\x6a\x64\x61\x38\x3b\x5b\x53\x73\xf7\x46\xc8\xd9\x40\x0a\x6e\xa6\x47\xcf\x30\xee\xd7\x61\xc3\x12\xcc\xd0\x33\x5f\xb0\x9d\x8c\x7b\x9d\xfd\xce\x7f\x47\xc5\x34\x0c\x63\xa5\x7d\xc8\xed\x35\xc9\x6b\x5e\x31\xb1\xf8\xfc\x58\xb0\xda\x2c\x1d\xa8\x3a\xc3\x0e\xc2\xff\x8d\x7f\x47\x2c\x54\xb0\xcb\xd3\x54\x97\xd1\x23\x00\x00\xff\xff\x52\x77\x2d\xb2\xea\x02\x00\x00")

func _20200428184744_create_foreign_key_indexes_down_sql() ([]byte, error) {
	return bindata_read(
		__20200428184744_create_foreign_key_indexes_down_sql,
		"20200428184744_create_foreign_key_indexes.down.sql",
	)
}

var __20200428184744_create_foreign_key_indexes_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xa4\x91\xcb\x6e\x86\x20\x10\x85\xf7\x7d\x8a\x59\xda\x67\x70\xd5\x0b\x6d\x58\x88\x49\x65\xe1\x8e\x90\x14\x1b\xd2\x7a\x09\xb0\xf1\xed\x1b\x6f\x04\xcd\xe8\x0f\x71\xf5\xe7\x77\x0e\xdf\x99\x73\xe6\x95\x7c\x52\x96\x3f\xbd\x7d\x91\x17\x4e\x80\xb2\x77\x52\x03\xfd\x00\x56\x72\x20\x35\xad\x78\x05\x46\x0d\xbd\xd5\xae\x37\x5a\x59\x31\x48\xa3\x3a\x27\xf4\xb7\x68\x7e\xd5\x08\x25\xdb\x8d\x21\xf3\xf3\xe7\x4b\x66\x2b\x3b\xdd\x28\xeb\xc4\x9f\x1c\x95\xb1\xc2\xff\x0f\xc0\x07\x0d\x64\x81\x28\x8d\x3e\xff\x5c\xa2\x37\x45\x2c\x57\x4f\x5b\x38\xd5\x06\x9b\x2f\xdf\x30\x0f\x2f\x0e\x22\xac\xea\x1b\x7e\xd1\x56\x8f\x5c\x9c\xfc\xb1\xc2\x5f\x71\x0c\xc1\xd3\x08\xb2\xdd\x2c\x82\x85\xad\xb8\x90\xd2\x76\x3a\xad\xf6\x00\x8b\xea\x32\x08\xb1\x3d\x3c\x4b\x8d\x49\x93\x5a\x40\xbd\xb0\x56\x70\xa7\xe8\x96\x90\xe7\x73\x19\x09\xc1\x16\xfd\xdd\x74\xab\xeb\xe9\xc1\x2e\x8c\xb1\x2b\x96\x45\x41\x79\xfe\x1f\x00\x00\xff\xff\xac\x6d\x93\x8b\x9a\x04\x00\x00")

func _20200428184744_create_foreign_key_indexes_up_sql() ([]byte, error) {
	return bindata_read(
		__20200428184744_create_foreign_key_indexes_up_sql,
		"20200428184744_create_foreign_key_indexes.up.sql",
	)
}

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func Asset(name string) ([]byte, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		return f()
	}
	return nil, fmt.Errorf("Asset %s not found", name)
}

// AssetNames returns the names of the assets.
func AssetNames() []string {
	names := make([]string, 0, len(_bindata))
	for name := range _bindata {
		names = append(names, name)
	}
	return names
}

// _bindata is a table, holding each asset generator, mapped to its name.
var _bindata = map[string]func() ([]byte, error){
	"20200319122755_create_repositories_table.down.sql":              _20200319122755_create_repositories_table_down_sql,
	"20200319122755_create_repositories_table.up.sql":                _20200319122755_create_repositories_table_up_sql,
	"20200319130108_create_manifests_table.down.sql":                 _20200319130108_create_manifests_table_down_sql,
	"20200319130108_create_manifests_table.up.sql":                   _20200319130108_create_manifests_table_up_sql,
	"20200319131222_create_manifest_configurations_table.down.sql":   _20200319131222_create_manifest_configurations_table_down_sql,
	"20200319131222_create_manifest_configurations_table.up.sql":     _20200319131222_create_manifest_configurations_table_up_sql,
	"20200319131542_create_layers_table.down.sql":                    _20200319131542_create_layers_table_down_sql,
	"20200319131542_create_layers_table.up.sql":                      _20200319131542_create_layers_table_up_sql,
	"20200319131632_create_manifest_layers_table.down.sql":           _20200319131632_create_manifest_layers_table_down_sql,
	"20200319131632_create_manifest_layers_table.up.sql":             _20200319131632_create_manifest_layers_table_up_sql,
	"20200319131907_create_manifest_lists_table.down.sql":            _20200319131907_create_manifest_lists_table_down_sql,
	"20200319131907_create_manifest_lists_table.up.sql":              _20200319131907_create_manifest_lists_table_up_sql,
	"20200319132010_create_manifest_list_items_table.down.sql":       _20200319132010_create_manifest_list_items_table_down_sql,
	"20200319132010_create_manifest_list_items_table.up.sql":         _20200319132010_create_manifest_list_items_table_up_sql,
	"20200319132237_create_tags_table.down.sql":                      _20200319132237_create_tags_table_down_sql,
	"20200319132237_create_tags_table.up.sql":                        _20200319132237_create_tags_table_up_sql,
	"20200408192311_create_repository_manifests_table.down.sql":      _20200408192311_create_repository_manifests_table_down_sql,
	"20200408192311_create_repository_manifests_table.up.sql":        _20200408192311_create_repository_manifests_table_up_sql,
	"20200408193126_create_repository_manifest_lists_table.down.sql": _20200408193126_create_repository_manifest_lists_table_down_sql,
	"20200408193126_create_repository_manifest_lists_table.up.sql":   _20200408193126_create_repository_manifest_lists_table_up_sql,
	"20200428184744_create_foreign_key_indexes.down.sql":             _20200428184744_create_foreign_key_indexes_down_sql,
	"20200428184744_create_foreign_key_indexes.up.sql":               _20200428184744_create_foreign_key_indexes_up_sql,
}

// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"}
// AssetDir("data/img") would return []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") would return an error
// AssetDir("") will return []string{"data"}.
func AssetDir(name string) ([]string, error) {
	node := _bintree
	if len(name) != 0 {
		cannonicalName := strings.Replace(name, "\\", "/", -1)
		pathList := strings.Split(cannonicalName, "/")
		for _, p := range pathList {
			node = node.Children[p]
			if node == nil {
				return nil, fmt.Errorf("Asset %s not found", name)
			}
		}
	}
	if node.Func != nil {
		return nil, fmt.Errorf("Asset %s not found", name)
	}
	rv := make([]string, 0, len(node.Children))
	for name := range node.Children {
		rv = append(rv, name)
	}
	return rv, nil
}

type _bintree_t struct {
	Func     func() ([]byte, error)
	Children map[string]*_bintree_t
}

var _bintree = &_bintree_t{nil, map[string]*_bintree_t{
	"20200319122755_create_repositories_table.down.sql":              {_20200319122755_create_repositories_table_down_sql, map[string]*_bintree_t{}},
	"20200319122755_create_repositories_table.up.sql":                {_20200319122755_create_repositories_table_up_sql, map[string]*_bintree_t{}},
	"20200319130108_create_manifests_table.down.sql":                 {_20200319130108_create_manifests_table_down_sql, map[string]*_bintree_t{}},
	"20200319130108_create_manifests_table.up.sql":                   {_20200319130108_create_manifests_table_up_sql, map[string]*_bintree_t{}},
	"20200319131222_create_manifest_configurations_table.down.sql":   {_20200319131222_create_manifest_configurations_table_down_sql, map[string]*_bintree_t{}},
	"20200319131222_create_manifest_configurations_table.up.sql":     {_20200319131222_create_manifest_configurations_table_up_sql, map[string]*_bintree_t{}},
	"20200319131542_create_layers_table.down.sql":                    {_20200319131542_create_layers_table_down_sql, map[string]*_bintree_t{}},
	"20200319131542_create_layers_table.up.sql":                      {_20200319131542_create_layers_table_up_sql, map[string]*_bintree_t{}},
	"20200319131632_create_manifest_layers_table.down.sql":           {_20200319131632_create_manifest_layers_table_down_sql, map[string]*_bintree_t{}},
	"20200319131632_create_manifest_layers_table.up.sql":             {_20200319131632_create_manifest_layers_table_up_sql, map[string]*_bintree_t{}},
	"20200319131907_create_manifest_lists_table.down.sql":            {_20200319131907_create_manifest_lists_table_down_sql, map[string]*_bintree_t{}},
	"20200319131907_create_manifest_lists_table.up.sql":              {_20200319131907_create_manifest_lists_table_up_sql, map[string]*_bintree_t{}},
	"20200319132010_create_manifest_list_items_table.down.sql":       {_20200319132010_create_manifest_list_items_table_down_sql, map[string]*_bintree_t{}},
	"20200319132010_create_manifest_list_items_table.up.sql":         {_20200319132010_create_manifest_list_items_table_up_sql, map[string]*_bintree_t{}},
	"20200319132237_create_tags_table.down.sql":                      {_20200319132237_create_tags_table_down_sql, map[string]*_bintree_t{}},
	"20200319132237_create_tags_table.up.sql":                        {_20200319132237_create_tags_table_up_sql, map[string]*_bintree_t{}},
	"20200408192311_create_repository_manifests_table.down.sql":      {_20200408192311_create_repository_manifests_table_down_sql, map[string]*_bintree_t{}},
	"20200408192311_create_repository_manifests_table.up.sql":        {_20200408192311_create_repository_manifests_table_up_sql, map[string]*_bintree_t{}},
	"20200408193126_create_repository_manifest_lists_table.down.sql": {_20200408193126_create_repository_manifest_lists_table_down_sql, map[string]*_bintree_t{}},
	"20200408193126_create_repository_manifest_lists_table.up.sql":   {_20200408193126_create_repository_manifest_lists_table_up_sql, map[string]*_bintree_t{}},
	"20200428184744_create_foreign_key_indexes.down.sql":             {_20200428184744_create_foreign_key_indexes_down_sql, map[string]*_bintree_t{}},
	"20200428184744_create_foreign_key_indexes.up.sql":               {_20200428184744_create_foreign_key_indexes_up_sql, map[string]*_bintree_t{}},
}}
