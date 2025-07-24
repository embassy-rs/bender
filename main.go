package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/containerd"
	"github.com/google/go-github/v72/github"
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

	runningJobsMutex   sync.Mutex
	runningJobs        map[string]*Job   // jobID -> Job
	runningJobsByDedup map[string]*Job   // dedupKey -> Job
	waitingJobs        map[string][]*Job // dedupKey -> slice of waiting jobs

	jobQueue *JobQueue

	cgroup Cgroup
}

type Event struct {
	Event      string            `json:"event"`
	Attributes map[string]string `json:"-"`

	Repo           *github.Repository  `json:"repository"`
	PullRequest    *github.PullRequest `json:"pull_request"`
	CloneURL       string              `json:"-"`
	SHA            string              `json:"-"`
	InstallationID int64               `json:"-"`

	// Cache[0] is the primary cache, Cache[1:] are secondary caches
	// that will be cloned into the primary cache if the primary cache
	// does not exist.
	// Example for PR 1234, which targets the foo branch:
	//    "pr-1234", "branch-foo", "branch-main"
	Cache []string `json:"-"`

	// If true, secrets will be mounted.
	Trusted bool `json:"-"`
}

type Job struct {
	*Event
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Priority        int               `json:"priority"`
	Dedup           DedupMode         `json:"-"` // Deduplication mode
	Script          string            `json:"-"`
	Permissions     map[string]string `json:"-"`
	PermissionRepos []string          `json:"-"`

	// Internal fields for job management
	cancelFunc context.CancelFunc `json:"-"` // Function to cancel this job
}

// Cancel cancels the job if it's running
func (j *Job) Cancel() {
	if j.cancelFunc != nil {
		j.cancelFunc()
	}
}

// DedupKey generates a unique deduplication key for this job
func (j *Job) DedupKey() string {
	if j.Dedup == DedupNone {
		return "" // No deduplication
	}

	// Handle nil fields for tests
	if j.Repo == nil || j.Repo.Owner == nil || j.Repo.Owner.Login == nil || j.Repo.Name == nil {
		return fmt.Sprintf("test/%s", j.Name) // Fallback for tests
	}

	// Base key: owner/repo/jobname
	key := fmt.Sprintf("%s/%s/%s", *j.Repo.Owner.Login, *j.Repo.Name, j.Name)

	// Add branch or PR number
	if j.PullRequest != nil && j.PullRequest.Number != nil {
		key += fmt.Sprintf("/pr-%d", *j.PullRequest.Number)
	} else if branch, ok := j.Attributes["branch"]; ok {
		key += fmt.Sprintf("/branch-%s", branch)
	}

	return key
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

	s := Service{
		config:             config,
		containerd:         cntd,
		runningJobs:        make(map[string]*Job),
		runningJobsByDedup: make(map[string]*Job),
		waitingJobs:        make(map[string][]*Job),
		jobQueue:           NewJobQueue(),
		cgroup:             cgroup,
	}

	// Start job queue workers
	log.Printf("Starting job queue with max concurrency: %d", config.MaxConcurrency)
	for i := 0; i < config.MaxConcurrency; i++ {
		go s.jobWorker()
	}

	if s.config.NetSandbox != nil {
		go s.netRun()
	}

	go s.cacheGCRun()

	s.serverRun()
}
