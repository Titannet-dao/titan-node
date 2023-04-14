package fsutil

import (
	"fmt"
	"os/exec"
	"strings"
)

func GetFilesystemType(absPath string) string {
	cmdStr := fmt.Sprintf("df -T %s |tail -n +2 |awk '{print $2}'", absPath)
	output, err := exec.Command("cmd", "/C", cmdStr).Output()
	if err != nil {
		log.Errorf("exec df cmd error:%s", err.Error())
		return ""
	}
	return strings.TrimSpace(string(output))
}
