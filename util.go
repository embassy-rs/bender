package main

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v72/github"
	"github.com/sqlbunny/errors"
)

func tryExec(cmd string, args ...string) {
	log.Printf("Executing command: %s %s", cmd, strings.Join(args, " "))
	c := exec.Command(cmd, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	err := c.Run()
	if err != nil {
		log.Printf("Failed to execute command: %v", err)
	}
}

func doExec(cmd string, args ...string) error {
	log.Printf("Executing command: %s %s", cmd, strings.Join(args, " "))
	c := exec.Command(cmd, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	err := c.Run()
	if err != nil {
		return errors.Errorf("Failed to execute command: %w", err)
	}
	return nil
}

func nopanic(fn func() error) (err error) {
	// This very convoluted code is because there's no way to distinguish
	// between `panic(nil)` and no panic with just `recover()` (both return nil)
	// https://github.com/golang/go/issues/25448
	panicked := true
	err = nil
	defer func() {
		if panicked {
			rvr := recover()
			if rvre, ok := rvr.(error); ok {
				err = errors.Errorf("panic: %w", rvre)
			} else {
				err = errors.Errorf("panic: %+v", rvr)
			}
		}
	}()

	err = fn()

	panicked = false
	return
}

func removeExtension(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n == -1 {
		return s
	}
	return s[:n]
}

func makeJobID() string {
	b := make([]byte, 6)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func getRepoFromPushEvent(e *github.PushEvent) *github.Repository {
	return &github.Repository{
		ID:              e.Repo.ID,
		NodeID:          e.Repo.NodeID,
		Name:            e.Repo.Name,
		FullName:        e.Repo.FullName,
		Owner:           e.Repo.Owner,
		Private:         e.Repo.Private,
		Description:     e.Repo.Description,
		Fork:            e.Repo.Fork,
		CreatedAt:       e.Repo.CreatedAt,
		PushedAt:        e.Repo.PushedAt,
		UpdatedAt:       e.Repo.UpdatedAt,
		Homepage:        e.Repo.Homepage,
		PullsURL:        e.Repo.PullsURL,
		Size:            e.Repo.Size,
		StargazersCount: e.Repo.StargazersCount,
		WatchersCount:   e.Repo.WatchersCount,
		Language:        e.Repo.Language,
		HasIssues:       e.Repo.HasIssues,
		HasDownloads:    e.Repo.HasDownloads,
		HasWiki:         e.Repo.HasWiki,
		HasPages:        e.Repo.HasPages,
		ForksCount:      e.Repo.ForksCount,
		Archived:        e.Repo.Archived,
		Disabled:        e.Repo.Disabled,
		OpenIssuesCount: e.Repo.OpenIssuesCount,
		DefaultBranch:   e.Repo.DefaultBranch,
		MasterBranch:    e.Repo.MasterBranch,
		Organization:    e.Organization,
		URL:             e.Repo.URL,
		ArchiveURL:      e.Repo.ArchiveURL,
		HTMLURL:         e.Repo.HTMLURL,
		StatusesURL:     e.Repo.StatusesURL,
		GitURL:          e.Repo.GitURL,
		SSHURL:          e.Repo.SSHURL,
		CloneURL:        e.Repo.CloneURL,
		SVNURL:          e.Repo.SVNURL,
		Topics:          e.Repo.Topics,
	}
}

func is404(err error) bool {
	var ghErr *github.ErrorResponse
	return errors.As(err, &ghErr) && ghErr.Response.StatusCode == 404
}

func (s *Service) githubClient(installationID int64) (*github.Client, error) {
	itr, err := ghinstallation.New(http.DefaultTransport, s.config.Github.AppID, installationID, []byte(s.config.Github.PrivateKey))
	if err != nil {
		return nil, err
	}

	gh := github.NewClient(&http.Client{Transport: itr})
	return gh, nil
}
