package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type CgroupManager struct {
	mountpoint string
	root       string
	bender     string
	jobs       string
}

// Cgroup represents a cgroup and provides methods to interact with it
type Cgroup struct {
	Path string // Absolute path to the cgroup directory
}

// NewCGroup creates a new CGroup instance
func NewCGroup(path string) *Cgroup {
	return &Cgroup{
		Path: path,
	}
}

// Create creates the cgroup directory structure
func (cg *Cgroup) Create() error {
	return os.MkdirAll(cg.Path, 0755)
}

// SetValue sets a value in a cgroup control file
func (cg *Cgroup) SetValue(controller, value string) error {
	controlPath := filepath.Join(cg.Path, controller)
	return os.WriteFile(controlPath, []byte(value), 0644)
}

// GetValue gets a value from a cgroup control file
func (cg *Cgroup) GetValue(controller string) (string, error) {
	controlPath := filepath.Join(cg.Path, controller)
	data, err := os.ReadFile(controlPath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// Exists checks if the cgroup directory exists
func (cg *Cgroup) Exists() bool {
	_, err := os.Stat(cg.Path)
	return err == nil
}

// Remove removes the cgroup directory
func (cg *Cgroup) Remove() error {
	return os.Remove(cg.Path)
}

// String returns the cgroup path
func (cg *Cgroup) String() string {
	return cg.Path
}

func initCgroup() CgroupManager {
	mountpoint := "/sys/fs/cgroup"
	rootBytes, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		panic(err)
	}
	root := string(rootBytes)
	root = strings.TrimSpace(strings.TrimPrefix(root, "0::"))

	cg := CgroupManager{
		mountpoint: mountpoint,
		root:       root,
		bender:     filepath.Join(root, "bender"),
		jobs:       filepath.Join(root, "jobs"),
	}

	// create sub-cgroups
	err = os.Mkdir(filepath.Join(cg.mountpoint, cg.bender), 0777)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}
	err = os.Mkdir(filepath.Join(cg.mountpoint, cg.jobs), 0777)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}

	// move ourselves to the bender cgroup.
	err = os.WriteFile(filepath.Join(cg.mountpoint, cg.bender, "cgroup.procs"), []byte(fmt.Sprint(os.Getpid())), 0777)
	if err != nil {
		panic(err)
	}

	return cg
}

// CreateJobCgroup creates a new cgroup for a job and returns a Cgroup instance
func (cgm *CgroupManager) CreateJobCgroup(jobID string) (*Cgroup, error) {
	jobCgroupPath := filepath.Join(cgm.mountpoint, cgm.jobs, fmt.Sprintf("job-%s", jobID))
	jobCgroup := NewCGroup(jobCgroupPath)

	err := jobCgroup.Create()
	if err != nil {
		return nil, err
	}

	return jobCgroup, nil
}
