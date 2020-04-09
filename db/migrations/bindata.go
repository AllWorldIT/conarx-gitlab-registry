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

var __20200319122755_create_repositories_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x84\x90\xc1\x4e\x84\x30\x10\x86\xef\x3c\xc5\x1c\x21\xf1\x0d\x3c\x55\x18\x4c\x23\xb6\x5a\x4a\x74\x4f\xa4\x91\x51\x1a\x17\x16\xcb\x98\xf8\xf8\x86\xad\x8b\xb2\x7b\x90\xd3\x64\xf8\xbe\xfc\x7f\x27\x37\x28\x2c\x82\x15\x37\x15\x82\x2c\x41\x69\x0b\xf8\x2c\x6b\x5b\x43\xa0\xe9\x30\x7b\x3e\x04\x4f\x73\x92\x26\x00\x00\xbe\x83\xd3\x37\x53\xf0\x6e\xbf\x4c\x8b\xa2\x9a\xaa\xba\x3a\x22\xa3\x1b\xe8\x07\x61\xfa\xe2\x38\x6d\x91\xc9\x71\xff\x2f\x12\x68\xe4\x76\xc9\xf3\x23\xd3\x1b\x85\xb8\x7f\x09\xe4\x98\xba\xd6\x31\xb0\x1f\x68\x66\x37\x4c\xab\x0a\x05\x96\xa2\xa9\x2c\x28\xfd\x94\x66\x51\xe8\x68\x4f\xe7\x42\xfc\x93\x6b\x55\x5b\x23\xa4\xb2\x30\xbd\xb7\x7f\x1f\x0b\x0f\x46\xde\x0b\xb3\x83\x3b\xdc\x41\xea\xbb\xec\x42\x78\xdd\x0a\xed\x6f\xdd\x52\x1b\x94\xb7\x2a\xaa\xeb\x3a\x4b\x4e\x67\x33\x58\xa2\x41\x95\xe3\xf6\xbe\xc7\x98\x15\xd2\x0a\x0a\xac\xd0\x22\xe4\xa2\xce\x45\x81\x17\x05\x3e\x3f\xce\x0b\x70\x0f\x8d\x92\x8f\x0d\x2e\xb1\xdc\x67\x49\x76\xfd\x1d\x00\x00\xff\xff\x8d\xf8\xde\x15\xdc\x01\x00\x00")

func _20200319122755_create_repositories_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319122755_create_repositories_table_up_sql,
		"20200319122755_create_repositories_table.up.sql",
	)
}

var __20200319130108_create_manifest_configurations_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x89\x4f\xce\xcf\x4b\xcb\x4c\x2f\x2d\x4a\x2c\xc9\xcc\xcf\x2b\xb6\x06\x04\x00\x00\xff\xff\xd0\x67\xff\x10\x2d\x00\x00\x00")

func _20200319130108_create_manifest_configurations_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319130108_create_manifest_configurations_table_down_sql,
		"20200319130108_create_manifest_configurations_table.down.sql",
	)
}

var __20200319130108_create_manifest_configurations_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x74\x90\x41\x4e\xc3\x30\x10\x45\xf7\x39\xc5\x2c\x13\x89\x1b\xb0\x32\xc5\x95\x2c\x82\x03\x89\x23\xe8\xca\x32\xf5\x34\x1a\x48\x9c\x10\x4f\x25\xca\xe9\x51\x9b\x14\x89\x94\x7a\x65\x6b\xde\xf3\x7c\xfd\x55\x29\x85\x91\x60\xc4\x5d\x2e\x41\xad\x41\x17\x06\xe4\xab\xaa\x4c\x05\x9d\x0b\xb4\xc3\xc8\x76\xdb\x87\x1d\x35\xfb\xd1\x31\xf5\x21\x26\x69\x02\x00\x40\x1e\xce\x27\xe2\x48\xae\x3d\xde\x8e\xb6\xae\xf3\xfc\xe6\x84\x74\xe8\xc9\x59\x3e\x0c\x08\x8c\x5f\x3c\xc1\x7f\x11\x4f\x0d\xc6\x69\x72\x0d\x89\xf4\x8d\xf3\xa2\x37\x6a\x28\xf0\x25\x32\xb8\x43\xdb\xbb\x53\xa0\xf7\xd8\x87\xff\x7e\xd9\x8e\xe8\x18\xbd\x75\x0c\x4c\x1d\x46\x76\xdd\xf0\x8b\xc0\xbd\x5c\x8b\x3a\x37\xa0\x8b\x97\x34\x9b\x93\x61\x8b\x4b\x61\x9a\xac\x0a\x5d\x99\x52\x28\x6d\x60\xf8\xb0\x8b\x96\x22\x3c\x95\xea\x51\x94\x1b\x78\x90\x1b\x48\xc9\x67\x17\xd2\xfe\x73\x29\x9d\xab\xb5\x73\x1d\xb5\x56\xcf\xb5\x84\x74\x7a\x66\x49\x76\xfb\x13\x00\x00\xff\xff\x12\xc9\x0e\x0c\xa7\x01\x00\x00")

func _20200319130108_create_manifest_configurations_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319130108_create_manifest_configurations_table_up_sql,
		"20200319130108_create_manifest_configurations_table.up.sql",
	)
}

var __20200319131222_create_manifests_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x4d\xcc\xcb\x4c\x4b\x2d\x2e\x29\xb6\x06\x04\x00\x00\xff\xff\x22\x39\x5a\x0e\x1f\x00\x00\x00")

func _20200319131222_create_manifests_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319131222_create_manifests_table_down_sql,
		"20200319131222_create_manifests_table.down.sql",
	)
}

var __20200319131222_create_manifests_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x94\x92\x4d\x6e\xc2\x30\x10\x85\xf7\x39\xc5\x2c\x89\xc4\x0d\xba\x4a\xc3\xa4\x8a\x9a\x3a\xad\xe3\xa8\x65\x65\x59\x64\xa0\x2e\xe4\xa7\xb6\xa9\xca\xed\x2b\x48\x80\x84\x00\x52\xbd\xb2\x34\xdf\x9b\x79\xe3\xe7\x90\x63\x20\x10\x44\xf0\x98\x20\xc4\x11\xb0\x54\x00\x7e\xc4\x99\xc8\xa0\x54\x95\x5e\x92\x75\xd6\x9b\x78\x00\x00\xba\x80\xe1\xb1\x64\xb4\xda\xec\x6f\x7b\x15\xcb\x93\x64\x7a\x00\x0d\x35\xb5\xd5\xae\x36\x3b\xd9\x6a\x74\xe5\x68\x45\x66\x04\xda\xc5\x27\x95\x4a\xfe\x90\xb1\xba\xae\xee\x80\x25\x15\x5a\x49\xb7\x6b\xa8\x1b\xed\xe8\xd7\xb5\xb7\x21\x58\xe8\x15\x59\xd7\xf3\x78\x0b\x5c\xd4\xd5\x52\xaf\xb6\x46\x39\x5d\x57\x7b\x9b\xb7\x46\x37\x6a\xb7\xa9\x55\x6f\xf5\x2f\x7b\xb0\x3a\xee\x68\x48\x39\x2a\xa4\x3a\x8e\x77\xba\x24\xeb\x54\xd9\x9c\x40\x98\x61\x14\xe4\x89\x00\x96\xbe\x4f\xfc\x6e\x35\x65\xd6\x7d\xd5\x59\xd6\x6d\x44\x1b\xba\xde\xb6\xad\x87\x29\xcb\x04\x0f\x62\x26\xa0\x59\xcb\x53\x66\xf0\xca\xe3\x97\x80\xcf\xe1\x19\xe7\x30\xd1\x85\x3f\xa2\x97\x3d\x5a\x0e\x23\x8b\x52\x8e\xf1\x13\x6b\xb5\x83\x92\xef\x1d\x6d\x72\x8c\x90\x23\x0b\x31\x3b\xe7\xad\xc9\x1e\x66\x9d\xa0\x94\xc1\x0c\x13\x14\x08\x61\x90\x85\xc1\x0c\xef\xbb\x18\x85\x32\x30\x72\x59\xbd\xea\xe5\xd8\x6c\xd8\xeb\xdf\xb6\xb6\xdf\xb7\x1e\x47\x76\x5f\x2c\x67\xf1\x5b\x8e\x17\xcf\x33\xed\x3e\xa0\xef\xf9\x0f\x7f\x01\x00\x00\xff\xff\x9c\xd7\xb2\xd1\x5a\x03\x00\x00")

func _20200319131222_create_manifests_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319131222_create_manifests_table_up_sql,
		"20200319131222_create_manifests_table.up.sql",
	)
}

var __20200319131542_create_layers_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\xc8\x49\xac\x4c\x2d\x2a\xb6\x06\x04\x00\x00\xff\xff\x04\xc2\x07\x1b\x1c\x00\x00\x00")

func _20200319131542_create_layers_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200319131542_create_layers_table_down_sql,
		"20200319131542_create_layers_table.down.sql",
	)
}

var __20200319131542_create_layers_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x74\xd0\xc1\x4e\xc4\x20\x14\x05\xd0\x7d\xbf\xe2\x2e\xdb\xc4\x3f\x70\x85\x23\x93\x10\x2b\xd5\x96\x46\x67\xd5\xa0\xbc\x4c\x5e\xa6\x35\xb5\x3c\x13\xc7\xaf\x37\x23\xa8\xd1\x66\x58\x41\xee\x01\x2e\x6c\x5a\xad\x9c\x86\x53\x57\xb5\x86\xd9\xc2\x36\x0e\xfa\xd1\x74\xae\xc3\xe8\x8f\xb4\xc4\xa2\x2c\x00\x80\x03\xbe\x47\xa4\x85\xfd\x78\x9a\x9d\xb0\xed\xeb\xfa\xe2\x8b\x4c\x14\xd8\x0f\x72\x9c\x09\x42\xef\x92\xf0\x5f\x12\x78\x4f\x31\x25\xe7\x48\xe4\x0f\xca\x17\x3d\xf1\x9e\x5f\x64\x4d\x9e\x17\xf2\x42\x61\xf0\x02\xe1\x89\xa2\xf8\x69\xfe\x21\xb8\xd6\x5b\xd5\xd7\x0e\xb6\x79\x28\xab\xdc\xcc\x2f\x87\xe4\x7f\x37\xe4\x42\x34\xd2\xff\xa3\x52\xb2\x69\x6c\xe7\x5a\x65\xac\xc3\x7c\x18\xd2\x5f\xe0\xae\x35\xb7\xaa\xdd\xe1\x46\xef\x50\x72\xa8\x56\xf4\xed\x35\xd3\x21\x3f\xb5\xb7\xe6\xbe\xd7\x28\xd3\xb2\x2a\xaa\xcb\xcf\x00\x00\x00\xff\xff\x26\x51\x54\x56\x72\x01\x00\x00")

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

var __20200319131632_create_manifest_layers_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x8c\x90\xc1\x6e\xb3\x30\x10\x84\xef\x3c\xc5\x1c\x41\xca\x1b\xfc\x27\xff\xb0\x54\x56\xa9\x69\x8d\x51\x9b\x13\xb2\xca\xa6\xb2\x02\x51\x0a\xee\xa1\x6f\x5f\x05\x02\x24\x25\xad\xca\x09\x79\xe7\xdb\xd9\x99\x58\x93\x30\x04\x23\xfe\x67\x04\x99\x42\xe5\x06\xf4\x22\x0b\x53\xa0\xb5\x07\xb7\xe3\xde\x57\x8d\xfd\xe4\xae\x0f\xc2\x00\x00\x5c\x8d\xf9\xeb\xb9\x73\xb6\x39\xfd\x9d\x30\x55\x66\xd9\x66\xd0\xcc\xa4\xab\xe1\x0e\x9e\xdf\xb8\x5b\x69\x86\xa5\xd5\xb8\xed\x27\xcd\x6b\xc7\xd6\x73\x5d\x59\x0f\x78\xd7\x72\xef\x6d\x7b\x9c\x35\x48\x28\x15\x65\x66\xa0\xf2\xe7\x30\x9a\x9c\xbb\xfd\x19\x58\x88\x71\x54\x73\xc3\xab\x65\xe3\x28\xce\x55\x61\xb4\x90\xca\xe0\xb8\xaf\xbe\xe5\xc6\xa3\x96\x0f\x42\x6f\x71\x4f\x5b\x84\xae\x8e\x56\xcc\x6e\xc5\x54\x97\x0d\xa4\xb9\x26\x79\xa7\x46\xfe\x62\x10\x05\x53\x8f\x9a\x52\xd2\xa4\x62\x5a\x4a\xef\x07\xab\x59\x91\x2b\x24\x94\x91\x21\xc4\xa2\x88\x45\x42\x7f\x39\x62\xae\xf8\xea\x82\xe9\xf5\xa6\xfd\x39\xf2\xcd\x98\x1f\xef\xbf\xc5\x5c\xdc\x4a\x25\x9f\x4a\xba\x8a\xba\xc1\xe2\x1a\xfd\xfb\x0a\x00\x00\xff\xff\x9d\xa8\x93\x26\x74\x02\x00\x00")

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

var __20200319131907_create_manifest_lists_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x74\x91\xcf\x4e\x84\x30\x10\xc6\xef\x3c\xc5\x1c\x21\xf1\x0d\x3c\x55\x18\x0c\x11\x8b\x29\xdd\xe8\x9e\x9a\x66\x99\xd5\xba\xfc\x4b\xdb\x18\x79\x7b\x03\x55\x90\xac\xf6\x34\xc9\xf7\xfb\x75\x9a\xaf\xa9\x40\x26\x11\x24\xbb\x2b\x11\x8a\x1c\x78\x25\x01\x5f\x8a\x5a\xd6\xd0\xe9\xde\x9c\xc9\x79\xd5\x1a\xe7\x5d\x14\x47\x00\x00\xa6\x81\xdf\xc7\x91\x35\xba\x9d\xa7\x59\xe4\x87\xb2\xbc\x59\x30\x4b\xe3\xe0\x8c\x1f\xec\xa4\x66\xc3\xf4\x9e\x5e\xc9\x5e\x61\xee\xf4\x46\x9d\x56\x1f\x64\x9d\x19\xfa\x7f\xb1\x8e\x1a\xa3\x95\x9f\x46\x5a\x96\x7a\xfa\xf4\x21\x18\xf5\xd4\x0e\x7a\x7d\xd2\xbb\x1b\xfa\x30\xed\xfd\x93\x25\xed\xa9\x51\xda\x07\xdf\x74\xe4\xbc\xee\xc6\x15\x83\x0c\x73\x76\x28\x25\xf0\xea\x39\x4e\xbe\x97\x6a\x7b\xd9\x9c\x4d\x0a\x69\x43\x2d\xfd\x75\x65\x48\xd3\x8a\xd7\x52\xb0\x82\x4b\x18\x2f\x6a\x5f\x24\x3c\x89\xe2\x91\x89\x23\x3c\xe0\x11\x62\xd3\x24\x57\xca\x79\x53\x9c\xda\x37\x99\x57\x02\x8b\x7b\x1e\xdc\x5d\x94\x44\x3f\x2d\x08\xcc\x51\x20\x4f\xb1\xde\xbe\xc1\x90\x5b\x76\xad\x50\xc5\x21\xc3\x12\x25\x42\xca\xea\x94\x65\x18\x25\xb7\x5f\x01\x00\x00\xff\xff\x31\x2d\xe9\x7b\x0c\x02\x00\x00")

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

var __20200319132010_create_manifest_list_items_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x94\x51\xdd\x4e\x83\x30\x14\xbe\xe7\x29\xbe\x4b\x48\xf6\x06\x5e\x55\x38\x98\x46\x2c\x5a\x4a\x74\x57\x84\xd8\x33\xd3\x08\xcb\xa4\xf5\xfd\xcd\x86\x5b\x02\x83\xe8\x7a\xd5\xf4\x7c\x7f\xfd\x4e\xaa\x49\x18\x82\x11\xf7\x05\x41\xe6\x50\xa5\x01\xbd\xc9\xca\x54\xe8\xdb\xbd\xdb\xb1\x0f\x4d\xe7\x7c\x68\x5c\xe0\xde\x47\x71\x04\x00\xce\x62\x7a\x3c\x0f\xae\xed\x8e\xb7\x23\x5f\xd5\x45\xb1\x39\x01\x67\x12\x16\x6e\x1f\xf8\x83\x87\x75\xe0\x59\x7a\x0d\xf8\x3e\x70\x1b\xd8\x36\x6d\xf8\xb5\x0e\xae\x67\x1f\xda\xfe\x70\x01\x22\xa3\x5c\xd4\x85\x81\x2a\x5f\xe3\x64\xa4\x59\xee\x78\x99\x36\xce\xd3\x52\x55\x46\x0b\xa9\x0c\x0e\x9f\xcd\xc2\xcf\xf1\xac\xe5\x93\xd0\x5b\x3c\xd2\x16\xb1\xb3\xc9\x15\x6f\xb7\xc8\x9b\xbf\x59\xe4\xa5\x26\xf9\xa0\x46\xa1\xf9\x34\x89\xce\x95\x6a\xca\x49\x93\x4a\x69\xb6\x08\x7f\x72\xbf\xc0\x4a\x85\x8c\x0a\x32\x84\x54\x54\xa9\xc8\xe8\xe6\x5c\xab\x91\xfe\x48\xe3\x97\x6b\xf8\xfe\xfa\x57\x0d\x13\xff\x5a\xc9\x97\x9a\xae\xdb\xd8\x60\x12\x26\xb9\xfb\x09\x00\x00\xff\xff\x34\x74\x86\xdf\xae\x02\x00\x00")

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

var __20200319132237_create_tags_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x74\x90\xc1\x6a\xb4\x30\x14\x85\xf7\x3e\xc5\x59\x2a\xcc\x1b\xfc\xab\xfc\x7a\x2d\xa1\x36\xb6\x31\xd2\xce\x4a\x42\x73\x67\x08\x1d\xc5\x6a\x06\xfa\xf8\x45\x65\x64\x86\xd0\xac\x6e\x38\xdf\xb9\x87\x7b\x72\x4d\xc2\x10\x8c\xf8\x5f\x11\x64\x09\x55\x1b\xd0\x87\x6c\x4c\x83\x60\xcf\x73\x92\x26\x00\xe0\x1d\xf6\x37\xf3\xe4\xed\x65\x99\x16\x56\xb5\x55\x75\x58\x99\xc1\xf6\x7c\x63\x02\xff\x84\x6d\x7a\x64\x7a\x3b\xf8\x13\xcf\xa1\xf3\x0e\x7e\x08\x7c\xe6\x29\x62\x3e\x27\xb6\x81\x5d\x67\x03\x10\x7c\xcf\x73\xb0\xfd\xb8\x33\x28\xa8\x14\x6d\x65\xa0\xea\xf7\x34\xdb\x1c\xd7\xd1\xc5\x8e\x4d\x72\x7c\xe1\x3f\xa4\xbc\x56\x8d\xd1\x42\x2a\x83\xf1\xab\x5b\x8e\xc5\xab\x96\x2f\x42\x1f\xf1\x4c\x47\xa4\xde\x65\x11\x78\xda\xc0\xee\xfe\x8e\xb2\xd6\x24\x9f\xd4\x66\xba\x13\xb2\xe4\xd6\x86\xa6\x92\x34\xa9\x9c\x9a\xbd\x80\x79\xdd\xbf\x13\xb5\x42\x41\x15\x19\x42\x2e\x9a\x5c\x14\x14\x25\x5f\xbf\xa3\xe4\x6e\x6d\xbc\x55\xf2\xad\x25\xa4\xcb\xe7\x80\x87\xfc\xec\xdf\x6f\x00\x00\x00\xff\xff\x21\x97\x7b\xbf\xde\x01\x00\x00")

func _20200319132237_create_tags_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200319132237_create_tags_table_up_sql,
		"20200319132237_create_tags_table.up.sql",
	)
}

var __20200408191941_update_tags_table_down_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x84\x8e\xb1\x0a\xc2\x30\x10\x86\xf7\x3e\xc5\x3f\x2a\xf4\x0d\x9c\xce\x26\x42\xa0\xa6\xda\x24\xe0\x16\x02\x4d\x25\x48\x5b\x6d\xe2\xe0\xdb\x4b\x15\x21\x93\x1d\x8f\xbb\xef\xbb\x8f\x6a\xcd\x5b\x68\xda\xd7\x1c\xc9\x5d\x63\x01\x00\xac\x6d\x4e\xa8\x1a\xa9\x74\x4b\x42\x6a\x88\x03\xf8\x45\x28\xad\xf0\x7c\xd8\xe5\xca\x8e\x6e\xf0\x76\xf6\xf7\x29\x86\x34\xcd\x2f\x1b\x3a\x54\xa4\x2a\x62\xbc\x5c\x31\xf4\xb7\xaf\x61\x1d\xae\xcd\x51\x66\xe0\x1f\x80\x18\xcb\x9f\xfd\x22\x07\x37\x86\xde\xc7\x64\x43\xf7\x09\x86\x91\xe2\x6c\x38\x36\xcb\x50\x22\x5b\x6f\x77\xc5\x3b\x00\x00\xff\xff\x63\x62\x56\x01\x09\x01\x00\x00")

func _20200408191941_update_tags_table_down_sql() ([]byte, error) {
	return bindata_read(
		__20200408191941_update_tags_table_down_sql,
		"20200408191941_update_tags_table.down.sql",
	)
}

var __20200408191941_update_tags_table_up_sql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x6c\x90\xc1\x4e\xc3\x30\x10\x44\xef\x7c\xc5\x1c\x53\xa9\x7f\xc0\xc9\xd8\x1b\xb0\x70\xd6\xc5\xde\x48\x70\xb2\x22\xc5\xad\x2c\xd4\x14\x92\x70\xe0\xef\x51\x8a\x5a\x88\x54\x5f\x67\xde\x1b\x79\x95\x13\x0a\x10\xf5\xe0\x08\x73\x77\x98\xee\x00\xc0\x04\xbf\x83\xf6\x1c\x25\x28\xcb\x02\x5b\x83\x5e\x6d\x94\x88\xaf\xcf\xb4\xb4\xd2\xb1\x1b\xca\x3e\x4f\x73\x2a\x7d\x1a\xba\x63\x86\x56\x51\x2b\x43\xdb\x33\xaf\x8c\x81\xf6\xae\x6d\x18\x63\xfe\x38\x4d\x65\x3e\x8d\xdf\xa9\xf4\x28\xc3\x9c\x0f\x79\x04\x7b\x01\xb7\xce\xfd\xaf\x5f\xd7\x2e\x1b\x8b\x37\xad\xf9\x96\xed\x4b\x4b\xa8\x96\x68\xbb\x8a\x36\x37\x55\xfb\xf7\x5f\xd5\xda\x52\xfb\x40\xf6\x91\xf1\x4c\x6f\xa8\xd6\x96\xb3\x64\x79\x81\x6a\x0a\xc4\x9a\xe2\xdf\x17\x4a\x9e\x50\x95\x7e\x83\x46\x89\x7e\x42\xb4\xcd\xce\xd1\x95\xf0\x0c\x43\x8e\x84\x2e\xb7\xb8\xff\x09\x00\x00\xff\xff\x34\xf3\x70\x06\x5c\x01\x00\x00")

func _20200408191941_update_tags_table_up_sql() ([]byte, error) {
	return bindata_read(
		__20200408191941_update_tags_table_up_sql,
		"20200408191941_update_tags_table.up.sql",
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
	"20200319122755_create_repositories_table.down.sql":            _20200319122755_create_repositories_table_down_sql,
	"20200319122755_create_repositories_table.up.sql":              _20200319122755_create_repositories_table_up_sql,
	"20200319130108_create_manifest_configurations_table.down.sql": _20200319130108_create_manifest_configurations_table_down_sql,
	"20200319130108_create_manifest_configurations_table.up.sql":   _20200319130108_create_manifest_configurations_table_up_sql,
	"20200319131222_create_manifests_table.down.sql":               _20200319131222_create_manifests_table_down_sql,
	"20200319131222_create_manifests_table.up.sql":                 _20200319131222_create_manifests_table_up_sql,
	"20200319131542_create_layers_table.down.sql":                  _20200319131542_create_layers_table_down_sql,
	"20200319131542_create_layers_table.up.sql":                    _20200319131542_create_layers_table_up_sql,
	"20200319131632_create_manifest_layers_table.down.sql":         _20200319131632_create_manifest_layers_table_down_sql,
	"20200319131632_create_manifest_layers_table.up.sql":           _20200319131632_create_manifest_layers_table_up_sql,
	"20200319131907_create_manifest_lists_table.down.sql":          _20200319131907_create_manifest_lists_table_down_sql,
	"20200319131907_create_manifest_lists_table.up.sql":            _20200319131907_create_manifest_lists_table_up_sql,
	"20200319132010_create_manifest_list_items_table.down.sql":     _20200319132010_create_manifest_list_items_table_down_sql,
	"20200319132010_create_manifest_list_items_table.up.sql":       _20200319132010_create_manifest_list_items_table_up_sql,
	"20200319132237_create_tags_table.down.sql":                    _20200319132237_create_tags_table_down_sql,
	"20200319132237_create_tags_table.up.sql":                      _20200319132237_create_tags_table_up_sql,
	"20200408191941_update_tags_table.down.sql":                    _20200408191941_update_tags_table_down_sql,
	"20200408191941_update_tags_table.up.sql":                      _20200408191941_update_tags_table_up_sql,
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
	"20200319122755_create_repositories_table.down.sql":            &_bintree_t{_20200319122755_create_repositories_table_down_sql, map[string]*_bintree_t{}},
	"20200319122755_create_repositories_table.up.sql":              &_bintree_t{_20200319122755_create_repositories_table_up_sql, map[string]*_bintree_t{}},
	"20200319130108_create_manifest_configurations_table.down.sql": &_bintree_t{_20200319130108_create_manifest_configurations_table_down_sql, map[string]*_bintree_t{}},
	"20200319130108_create_manifest_configurations_table.up.sql":   &_bintree_t{_20200319130108_create_manifest_configurations_table_up_sql, map[string]*_bintree_t{}},
	"20200319131222_create_manifests_table.down.sql":               &_bintree_t{_20200319131222_create_manifests_table_down_sql, map[string]*_bintree_t{}},
	"20200319131222_create_manifests_table.up.sql":                 &_bintree_t{_20200319131222_create_manifests_table_up_sql, map[string]*_bintree_t{}},
	"20200319131542_create_layers_table.down.sql":                  &_bintree_t{_20200319131542_create_layers_table_down_sql, map[string]*_bintree_t{}},
	"20200319131542_create_layers_table.up.sql":                    &_bintree_t{_20200319131542_create_layers_table_up_sql, map[string]*_bintree_t{}},
	"20200319131632_create_manifest_layers_table.down.sql":         &_bintree_t{_20200319131632_create_manifest_layers_table_down_sql, map[string]*_bintree_t{}},
	"20200319131632_create_manifest_layers_table.up.sql":           &_bintree_t{_20200319131632_create_manifest_layers_table_up_sql, map[string]*_bintree_t{}},
	"20200319131907_create_manifest_lists_table.down.sql":          &_bintree_t{_20200319131907_create_manifest_lists_table_down_sql, map[string]*_bintree_t{}},
	"20200319131907_create_manifest_lists_table.up.sql":            &_bintree_t{_20200319131907_create_manifest_lists_table_up_sql, map[string]*_bintree_t{}},
	"20200319132010_create_manifest_list_items_table.down.sql":     &_bintree_t{_20200319132010_create_manifest_list_items_table_down_sql, map[string]*_bintree_t{}},
	"20200319132010_create_manifest_list_items_table.up.sql":       &_bintree_t{_20200319132010_create_manifest_list_items_table_up_sql, map[string]*_bintree_t{}},
	"20200319132237_create_tags_table.down.sql":                    &_bintree_t{_20200319132237_create_tags_table_down_sql, map[string]*_bintree_t{}},
	"20200319132237_create_tags_table.up.sql":                      &_bintree_t{_20200319132237_create_tags_table_up_sql, map[string]*_bintree_t{}},
	"20200408191941_update_tags_table.down.sql":                    &_bintree_t{_20200408191941_update_tags_table_down_sql, map[string]*_bintree_t{}},
	"20200408191941_update_tags_table.up.sql":                      &_bintree_t{_20200408191941_update_tags_table_up_sql, map[string]*_bintree_t{}},
}}
