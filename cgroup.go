package main

import (
	"fmt"
	"log"
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

	// enableControllers enables the memory/cpu/pids controllers in a cgroup's
	// subtree_control so they're available to its children. Writing a controller
	// file (e.g. memory.oom.group) in a job cgroup requires the controller to be
	// enabled in cgroup.subtree_control of every ancestor. systemd delegates the
	// controllers to us (Delegate=memory cpu pids in the unit), but we must
	// propagate them down ourselves.
	//
	// Note: the kernel's "no internal process" rule forbids enabling
	// subtree_control on a cgroup that has processes directly in it, so the
	// caller must move our own process into a leaf cgroup before enabling
	// controllers on an ancestor.
	enableControllers := func(cgPath string) {
		err := os.WriteFile(filepath.Join(cgPath, "cgroup.subtree_control"), []byte("+memory +cpu +pids"), 0644)
		if err != nil {
			log.Printf("Warning: failed to enable controllers in %s: %v", cgPath, err)
		}
	}

	// create the bender sub-cgroup and move ourselves into it first, so that
	// the delegation root has no internal processes and we can enable
	// controllers on it below.
	err = os.Mkdir(filepath.Join(cg.mountpoint, cg.bender), 0777)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}
	err = os.WriteFile(filepath.Join(cg.mountpoint, cg.bender, "cgroup.procs"), []byte(fmt.Sprint(os.Getpid())), 0777)
	if err != nil {
		panic(err)
	}

	// Now that the delegation root is empty of processes, enable controllers in
	// its subtree_control. This makes the controllers available to the jobs
	// sub-cgroup created below, and creates the memory.* interface files in the
	// bender sub-cgroup (needed for memory.low below).
	enableControllers(filepath.Join(cg.mountpoint, cg.root))

	// Protect bender's own memory so that when the bender.service MemoryMax is
	// hit, the OOM killer prefers killing job processes over bender itself.
	// Under memory pressure the kernel reclaims/kills from cgroups whose usage
	// exceeds memory.low first; by reserving memory.low for the bender cgroup
	// and leaving the jobs subtree unprotected (memory.low=0, the default), jobs
	// become the preferred OOM victims. bender is a lightweight daemon, so a
	// small reservation is plenty. This must come after enabling the memory
	// controller in the root's subtree_control, otherwise memory.low won't exist.
	err = os.WriteFile(filepath.Join(cg.mountpoint, cg.bender, "memory.low"), []byte("512M"), 0644)
	if err != nil {
		log.Printf("Warning: failed to set memory.low for bender cgroup: %v", err)
	}

	// create the jobs sub-cgroup and enable controllers in it so per-job cgroups
	// get the controller interface files (memory.*, cpu.*, pids.*).
	err = os.Mkdir(filepath.Join(cg.mountpoint, cg.jobs), 0777)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}
	enableControllers(filepath.Join(cg.mountpoint, cg.jobs))

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
