package workerd

import (
	"archive/zip"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
)

func unzip(source, destination string) error {
	// Open the ZIP file for reading
	reader, err := zip.OpenReader(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Create the destination directory if it doesn't exist
	err = os.MkdirAll(destination, 0755)
	if err != nil {
		return err
	}

	// Extract each file from the ZIP archive
	for _, file := range reader.File {
		filePath := fmt.Sprintf("%s/%s", destination, file.Name)

		// Create the file at the destination
		if file.FileInfo().IsDir() {
			// Create directory if it's a directory
			os.MkdirAll(filePath, file.Mode())
			continue
		}

		// Create the file
		writer, err := os.Create(filePath)
		if err != nil {
			return err
		}
		defer writer.Close()

		// Open file from ZIP archive
		zipFile, err := file.Open()
		if err != nil {
			return err
		}
		defer zipFile.Close()

		// Copy the contents of the file to the destination file
		_, err = io.Copy(writer, zipFile)
		if err != nil {
			return err
		}
	}

	return nil
}

func copySubDirectory(parentDir, dest string) error {
	return filepath.WalkDir(parentDir, func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() || path == parentDir {
			return nil
		}

		return CopyDirectory(path, parentDir)
	})
}

// CopyDirectory copies a whole directory recursively
func CopyDirectory(src string, dst string) error {
	var err error
	var fds []os.FileInfo
	var srcinfo os.FileInfo

	if srcinfo, err = os.Stat(src); err != nil {
		return err
	}

	if err = os.MkdirAll(dst, srcinfo.Mode()); err != nil {
		return err
	}

	if fds, err = ioutil.ReadDir(src); err != nil {
		return err
	}
	for _, fd := range fds {
		srcfp := filepath.Join(src, fd.Name())
		dstfp := filepath.Join(dst, fd.Name())

		if fd.IsDir() {
			if err = CopyDirectory(srcfp, dstfp); err != nil {
				fmt.Println(err)
			}
		} else {
			if err = File(srcfp, dstfp); err != nil {
				fmt.Println(err)
			}
		}
	}
	return nil
}

// File copies a single file from src to dst
func File(src, dst string) error {
	var err error
	var srcfd *os.File
	var dstfd *os.File

	if srcfd, err = os.Open(src); err != nil {
		return err
	}
	defer srcfd.Close()

	if dstfd, err = os.Create(dst); err != nil {
		return err
	}
	defer dstfd.Close()

	if _, err = io.Copy(dstfd, srcfd); err != nil {
		return err
	}

	return nil
}

func Exists(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}

	return true
}
