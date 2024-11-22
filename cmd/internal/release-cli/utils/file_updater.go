package utils

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

func UpdateFileInK8s(fileName, stage, version string) ([]byte, error) {
	if stage == "gstg-pre" {
		return updateFileWithRegex(fileName, " "+version, `registry_version: v[0-9\.]+-gitlab`)
	} else {
		return updateFileWithScanner(fileName, stage, version)
	}
}

func UpdateFileInGDK(fileName, version string) ([]byte, error) {
	return updateFileWithRegex(fileName, version, `(?s)gitlab-container-registry:.*v[0-9\.]+-gitlab`)
}

func updateFileWithScanner(fileName, stage, newVersion string) ([]byte, error) {
	//nolint: gosec
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var outputLines []string
	isTargetStage := false
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, stage+":") {
			isTargetStage = true
		}

		if isTargetStage && strings.Contains(line, "registry_version:") {
			line = "        registry_version: " + newVersion
			isTargetStage = false
		}
		outputLines = append(outputLines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	output := strings.Join(outputLines, "\n") + "\n"
	//nolint: gosec
	err = os.WriteFile(fileName, []byte(output), 0o644)
	if err != nil {
		return nil, err
	}

	// nolint: gosec
	updatedFile, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	err = os.Remove(fileName)
	if err != nil {
		return nil, err
	}

	return updatedFile, err
}

func updateFileWithRegex(fileName, version, pattern string) ([]byte, error) {
	//nolint:gosec
	f, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	m := regexp.MustCompile(pattern)
	output := m.ReplaceAllStringFunc(string(f), func(s string) string {
		prefix := strings.Split(s, ":")[0]
		return fmt.Sprintf("%s:%s", prefix, version)
	})

	//nolint: gosec
	err = os.WriteFile(fileName, []byte(output), 0o644)
	if err != nil {
		return nil, err
	}

	//nolint: gosec
	updatedFile, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	err = os.Remove(fileName)
	if err != nil {
		return nil, err
	}

	return updatedFile, err
}
