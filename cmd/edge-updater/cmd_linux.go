//go:build !windows
// +build !windows

package main

import "os/exec"

func killApp(appName string) error {
	cmd := exec.Command("pkill", appName)
	_, err := cmd.Output()
	if err != nil {
		return err
	}

	return nil

}
