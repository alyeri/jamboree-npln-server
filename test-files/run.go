//go:build ignore

// Run the server tests from the repository root with:
//
//	go run ./test-files/run.go
//
// Additional go test arguments may follow the command, for example -v or
// -run TestTagMatch. An overlay gives the tests access to the server's internal
// package without copying files into the repository root.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

type testOverlay struct {
	Replace map[string]string
}

func runTests() error {
	root, err := os.Getwd()

	if err != nil {
		return err
	}

	testDirectory := filepath.Join(root, "test-files")
	sources, err := filepath.Glob(filepath.Join(testDirectory, "*_test.go"))

	if err != nil {
		return err
	}

	if len(sources) == 0 {
		return errors.New("run this command from the server repository root")
	}

	overlay := testOverlay{
		Replace: make(map[string]string, len(sources)),
	}

	for _, source := range sources {
		overlay.Replace[filepath.Join(root, filepath.Base(source))] = source
	}

	temporaryDirectory, err := os.MkdirTemp("", "jamboree-server-tests-")

	if err != nil {
		return err
	}

	defer os.RemoveAll(temporaryDirectory)

	data, err := json.MarshalIndent(overlay, "", "  ")

	if err != nil {
		return err
	}

	overlayPath := filepath.Join(temporaryDirectory, "overlay.json")

	if err := os.WriteFile(overlayPath, data, 0600); err != nil {
		return err
	}

	arguments := []string{
		"test",
		"-overlay", overlayPath,
		"-p", "1",
		"-timeout", "90s",
	}
	arguments = append(arguments, os.Args[1:]...)
	arguments = append(arguments, "./...")

	command := exec.Command("go", arguments...)
	command.Dir = root
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr

	return command.Run()
}

func main() {
	if err := runTests(); err != nil {
		fmt.Fprintln(os.Stderr, err)

		var exitError *exec.ExitError

		if errors.As(err, &exitError) {
			os.Exit(exitError.ExitCode())
		}

		os.Exit(1)
	}
}
