package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"gopkg.in/yaml.v3"
)

type Config struct {
	DataDir        string            `yaml:"data_dir"`
	ExternalURL    string            `yaml:"external_url"`
	ListenPort     int               `yaml:"listen_port"`
	MaxConcurrency int               `yaml:"max_concurrency"`
	NetSandbox     *NetSandboxConfig `yaml:"net_sandbox"`
	Image          string            `yaml:"image"`
	Github         GithubConfig      `yaml:"github"`
	Cache          CacheConfig       `yaml:"cache"`
	SeccompLog     bool              `yaml:"seccomp_log"`
	// Per-job memory limits, in cgroup v2 syntax (e.g. "12G", or "max" for
	// unlimited). When a job exceeds memory_max RAM + memory_swap_max swap, it
	// gets OOM-killed as a group (memory.oom.group=1).
	MemoryMax     string `yaml:"memory_max"`
	MemorySwapMax string `yaml:"memory_swap_max"`
}

type CacheConfig struct {
	MinFreeSpaceMB int `yaml:"min_free_space_mb"`
	MaxSizeMB      int `yaml:"max_size_mb"`
}

type NetSandboxConfig struct {
	AllowedDomains []string `yaml:"allowed_domains"`
}

type GithubConfig struct {
	WebhookSecret string `yaml:"webhook_secret"`
	AppID         int64  `yaml:"app_id"`
	PrivateKey    string `yaml:"private_key"`
}

type Service struct {
	config     Config
	containerd *containerd.Client
	queue      *Queue
	cgroup     CgroupManager
}

func main() {
	var configFlag = flag.String("c", "config.yaml", "path to config.yaml")
	flag.Parse()

	log.Printf("loading config from %s", *configFlag)
	configData, err := os.ReadFile(*configFlag)
	if err != nil {
		log.Fatal(err)
	}
	config := Config{
		ListenPort:     8000,
		MaxConcurrency: 4, // Default to 4 concurrent jobs
		Cache: CacheConfig{
			MinFreeSpaceMB: 20 * 1024, // 20gb
			MaxSizeMB:      40 * 1024, // 40gb
		},
		MemoryMax:     "12G",
		MemorySwapMax: "2G",
	}
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		log.Fatal(err)
	}

	// Validate max concurrency
	if config.MaxConcurrency <= 0 {
		log.Printf("Invalid max_concurrency %d, using default of 4", config.MaxConcurrency)
		config.MaxConcurrency = 4
	}
	if config.MaxConcurrency > 100 {
		log.Printf("Max concurrency %d seems too high, consider reducing it", config.MaxConcurrency)
	}

	config.DataDir, err = filepath.Abs(config.DataDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, subdir := range []string{"logs", "fifo", "cache", "jobs"} {
		err = os.MkdirAll(filepath.Join(config.DataDir, subdir), 0700)
		if err != nil {
			log.Fatal(err)
		}
	}

	cntd, err := containerd.New("/run/containerd/containerd.sock")
	if err != nil {
		log.Fatal(err)
	}

	cgroup := initCgroup()

	queue := newQueue(config.MaxConcurrency)

	s := Service{
		config:     config,
		containerd: cntd,
		queue:      queue,
		cgroup:     cgroup,
	}

	s.cleanupStale()

	// Start the scheduler
	log.Printf("Starting job scheduler with max concurrency: %d", config.MaxConcurrency)
	go s.schedulerRun()

	if s.config.NetSandbox != nil {
		go s.netRun()
	}

	go s.cacheGCRun()

	s.serverRun()
}

func (s *Service) cleanupStale() {
	// List stale job dirs
	jobsDir := filepath.Join(s.config.DataDir, "jobs")
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		log.Printf("cleanupStale: failed to read jobs dir: %v", err)
		entries = nil
	}
	var staleJobDirs []string
	for _, e := range entries {
		staleJobDirs = append(staleJobDirs, filepath.Join(jobsDir, e.Name()))
	}

	// List stale containers
	ctx := namespaces.WithNamespace(context.Background(), "bender")
	containers, err := s.containerd.Containers(ctx)
	if err != nil {
		log.Printf("cleanupStale: failed to list containers: %v", err)
		containers = nil
	}

	if len(staleJobDirs) == 0 && len(containers) == 0 {
		return
	}
	log.Printf("cleanupStale: %d stale job dirs, %d stale containers", len(staleJobDirs), len(containers))

	go func() {
		for _, c := range containers {
			log.Printf("cleanupStale: killing stale container: %s", c.ID())
			if task, err := c.Task(ctx, nil); err == nil {
				if err := task.Kill(ctx, syscall.SIGKILL); err != nil && !errdefs.IsNotFound(err) {
					log.Printf("cleanupStale: failed to kill task for %s: %v", c.ID(), err)
				}
				if statusC, err := task.Wait(ctx); err == nil {
					<-statusC
				}
				if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
					log.Printf("cleanupStale: failed to delete task for %s: %v", c.ID(), err)
				}
			} else if !errdefs.IsNotFound(err) {
				log.Printf("cleanupStale: failed to load task for %s: %v", c.ID(), err)
			}
			if err := c.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
				log.Printf("cleanupStale: failed to delete container %s: %v", c.ID(), err)
			}
		}
		for _, dir := range staleJobDirs {
			log.Printf("cleanupStale: deleting stale job dir: %s", dir)
			cmd := exec.Command("btrfs", "subvolume", "delete", "-R", dir)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				log.Printf("cleanupStale: failed to delete job dir %s: %v", dir, err)
			}
		}
	}()
}

func (s Service) schedulerRun() {
	for {
		job := s.queue.nextJob()
		go s.runJob(context.Background(), job)
	}
}
