package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/containerd/containerd"
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
	for _, subdir := range []string{"logs", "fifo", "cache"} {
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

	// Start the scheduler
	log.Printf("Starting job scheduler with max concurrency: %d", config.MaxConcurrency)
	go s.schedulerRun()

	if s.config.NetSandbox != nil {
		go s.netRun()
	}

	go s.cacheGCRun()

	s.serverRun()
}

func (s Service) schedulerRun() {
	for {
		job := s.queue.nextJob()
		go s.runJob(context.Background(), job)
	}
}
