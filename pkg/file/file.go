package file

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/deepfence/PacketStreamer/pkg/config"
)

type FileOutput struct {
	sync.RWMutex
	pathTemplate    string
	currentName     string
	currentFileSize int
	file            *os.File
	writer          io.Writer
	config          *config.FileOutputConfig
	closed          bool
}

func NewFileOutput(config *config.FileOutputConfig) *FileOutput {
	return &FileOutput{
		pathTemplate: config.Path,
		config:       config,
	}
}

func withoutIndex(s string) string {
	if i := strings.LastIndex(s, "_"); i != -1 {
		return s[:i]
	}

	return s
}

func setFileIndex(name string, idx int) string {
	idxS := strconv.Itoa(idx)
	ext := filepath.Ext(name)
	withoutExt := strings.TrimSuffix(name, ext)

	if i := strings.LastIndex(withoutExt, "_"); i != -1 {
		if _, err := strconv.Atoi(withoutExt[i+1:]); err == nil {
			withoutExt = withoutExt[:i]
		}
	}

	return withoutExt + "_" + idxS + ext
}

func getFileIndex(name string) int {
	ext := filepath.Ext(name)
	withoutExt := strings.TrimSuffix(name, ext)

	if idx := strings.LastIndex(withoutExt, "_"); idx != -1 {
		if i, err := strconv.Atoi(withoutExt[idx+1:]); err == nil {
			return i
		}
	}

	return -1
}

type sortByFileIndex []string

func (s sortByFileIndex) Len() int {
	return len(s)
}

func (s sortByFileIndex) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortByFileIndex) Less(i, j int) bool {
	if withoutIndex(s[i]) == withoutIndex(s[j]) {
		return getFileIndex(s[i]) < getFileIndex(s[j])
	}

	return s[i] < s[j]
}

func (o *FileOutput) filename() string {
	o.RLock()
	defer o.RUnlock()

	path := o.pathTemplate

	nextChunk := false

	if o.currentName == "" ||
		(o.config.SizeLimit > 0 && o.currentFileSize >= int(o.config.SizeLimit)) {
		nextChunk = true
	}

	ext := filepath.Ext(path)
	withoutExt := strings.TrimSuffix(path, ext)

	if matches, err := filepath.Glob(withoutExt + "*" + ext); err == nil {
		if len(matches) == 0 {
			return setFileIndex(path, 0)
		}
		sort.Sort(sortByFileIndex(matches))

		last := matches[len(matches)-1]

		fileIndex := 0
		if idx := getFileIndex(last); idx != -1 {
			fileIndex = idx

			if nextChunk {
				fileIndex++
			}
		}

		return setFileIndex(last, fileIndex)
	}

	return path
}

func (o *FileOutput) updateName() {
	name := filepath.Clean(o.filename())
	o.Lock()
	o.currentName = name
	o.Unlock()
}

func (o *FileOutput) Write(p []byte) (n int, err error) {
	o.updateName()
	o.Lock()
	defer o.Unlock()

	if o.file == nil || o.currentName != o.file.Name() {
		o.closeLocked()

		o.file, err = os.OpenFile(o.currentName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
		o.writer = o.file
		if err != nil {
			log.Fatal(o, "Cannot open file %q. Error: %s", o.currentName, err)
		}
	}

	n, err = o.writer.Write(p)
	o.currentFileSize += n

	return n, err
}

func (o *FileOutput) closeLocked() error {
	if o.file != nil {
		o.file.Close()
	}

	o.closed = true
	o.currentFileSize = 0

	return nil
}
