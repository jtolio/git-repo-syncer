package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"

	"github.com/BurntSushi/toml"
	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	errs "github.com/zeebo/errs/v2"
)

var (
	configPath = flag.String("config", "", "path to config file")
)

func main() {
	flag.Parse()
	err := Main()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

type Repo struct {
	Webhook       string
	Path          string
	Remotes       []string
	Branches      []string
	BranchRenames []string
	Tags          bool
}

type CommitList []*object.Commit

func (l CommitList) Len() int      { return len(l) }
func (l CommitList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l CommitList) Less(i, j int) bool {
	return l[i].Committer.When.Before(l[j].Committer.When)
}

func cmdRun(cmd *exec.Cmd) error {
	out, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	return errs.Errorf("execution: %v:\n%v", err, string(out))
}

func gitFetch(repoPath string, repo *git.Repository, remote string) error {
	cmd := exec.Command("git", "fetch", "-p", remote, "--tags")
	cmd.Dir = repoPath
	return cmdRun(cmd)
}

func gitPush(repoPath string, repo *git.Repository, remote, localref, remoteref string) error {
	cmd := exec.Command("git", "push", remote, fmt.Sprintf("%s:%s", localref, remoteref))
	cmd.Dir = repoPath
	return cmdRun(cmd)
}

func IdentifyLatest(repo *git.Repository,
	refs map[string]*plumbing.Hash) (*object.Commit, error) {
	commitSeen := make(map[plumbing.Hash]bool)
	commits := make(CommitList, 0, len(refs))
	for _, hash := range refs {
		if commitSeen[*hash] {
			continue
		}
		commit, err := repo.CommitObject(*hash)
		if err != nil {
			return nil, errs.Wrap(err)
		}
		commitSeen[*hash] = true
		commits = append(commits, commit)
	}

	// reduce cpu work by guessing the latest commits first
	sort.Sort(commits)

	for _, testCommit := range commits {
		is, err := IsDescendent(testCommit, commits)
		if err != nil {
			return nil, errs.Wrap(err)
		}
		if is {
			return testCommit, nil
		}
	}
	return nil, fmt.Errorf("cannot find shared descendent, history has forked")
}

func IsDescendent(commit *object.Commit, potentials []*object.Commit) (
	bool, error) {
	potentialmap := make(map[plumbing.Hash]bool, len(potentials))
	for _, potential := range potentials {
		potentialmap[potential.Hash] = true
	}
	commitSeen := map[plumbing.Hash]bool{commit.Hash: true}
	ancestorStack := []*object.Commit{commit}
	for {
		if len(potentialmap) == 0 {
			return true, nil
		}
		if len(ancestorStack) == 0 {
			return false, nil
		}
		next := ancestorStack[len(ancestorStack)-1]
		ancestorStack = ancestorStack[:len(ancestorStack)-1]
		delete(potentialmap, next.Hash)
		err := next.Parents().ForEach(func(c *object.Commit) error {
			if !commitSeen[c.Hash] {
				ancestorStack = append(ancestorStack, c)
				commitSeen[c.Hash] = true
			}
			return nil
		})
		if err != nil {
			return false, err
		}
	}
}

func (r *Repo) branchByRemote(remote, branch string) string {
	for _, rename := range r.BranchRenames {
		parts := strings.Split(rename, ":")
		if len(parts) != 3 {
			continue
		}
		if remote == parts[0] && branch == parts[1] {
			return parts[2]
		}
	}
	return branch
}

func (r *Repo) SyncBranch(repoPath string, repo *git.Repository, branch string) error {
	refs := make(map[string]*plumbing.Hash, len(r.Remotes))
	for _, remote := range r.Remotes {
		hash, err := repo.ResolveRevision(plumbing.Revision(
			fmt.Sprintf("refs/remotes/%s/%s", remote, r.branchByRemote(remote, branch))))
		if err != nil {
			return errs.Wrap(err)
		}
		refs[remote] = hash
	}

	latest, err := IdentifyLatest(repo, refs)
	if err != nil {
		return errs.Errorf("problem syncing branch %v: %v", branch, err)
	}

	for remote, hash := range refs {
		if latest.Hash == *hash {
			continue
		}
		remoteBranch := r.branchByRemote(remote, branch)
		log.Printf("updating %v/%s to %v", remote, remoteBranch, latest.Hash.String())
		err = gitPush(repoPath, repo, remote, latest.Hash.String(), "refs/heads/"+remoteBranch)
		if err != nil {
			return errs.Wrap(err)
		}
	}
	return nil
}

func (r *Repo) FetchAll(repoPath string, repo *git.Repository) error {
	for _, remote := range r.Remotes {
		err := gitFetch(repoPath, repo, remote)
		if err != nil {
			return errs.Wrap(err)
		}
	}
	return nil
}

func (r *Repo) SyncTags(repoPath string, repo *git.Repository) error {
	unionTags := make(map[string]struct{})
	allTags := make(map[string]map[string]struct{})

	for _, remote := range r.Remotes {
		tagSet := make(map[string]struct{})
		allTags[remote] = tagSet

		rem, err := repo.Remote(remote)
		if err != nil {
			return errs.Wrap(err)
		}

		refs, err := rem.List(&git.ListOptions{})
		if err != nil {
			return errs.Wrap(err)
		}

		for _, ref := range refs {
			if ref.Name().IsTag() {
				tagSet[ref.Name().String()] = struct{}{}
				unionTags[ref.Name().String()] = struct{}{}
			}
		}
	}

	for _, remote := range r.Remotes {
		for tag := range unionTags {
			if _, ok := allTags[remote][tag]; ok {
				continue
			}
			if err := r.SyncTag(repoPath, repo, remote, tag); err != nil {
				return errs.Wrap(err)
			}
		}
	}

	return nil
}

func (r *Repo) SyncTag(repoPath string, repo *git.Repository, remote, tag string) error {
	hash, err := repo.ResolveRevision(plumbing.Revision(tag))
	if err != nil {
		return errs.Wrap(err)
	}

	log.Printf("pushing %s (%v) to %s", tag, hash, remote)
	err = gitPush(repoPath, repo, remote, tag, tag)
	if err != nil {
		return errs.Wrap(err)
	}

	return nil
}

func (r *Repo) FetchAndSync() error {
	repo, err := git.PlainOpen(r.Path)
	if err != nil {
		return errs.Wrap(err)
	}

	err = r.FetchAll(r.Path, repo)
	if err != nil {
		return errs.Wrap(err)
	}

	if r.Tags {
		err := r.SyncTags(r.Path, repo)
		if err != nil {
			return errs.Wrap(err)
		}
	}

	for _, branch := range r.Branches {
		err := r.SyncBranch(r.Path, repo, branch)
		if err != nil {
			return errs.Wrap(err)
		}
	}

	return nil
}

func Main() error {
	if *configPath == "" {
		return errs.Errorf("--config option expected")
	}

	var config struct {
		Addr     string
		SlackURL string
		Repo     []Repo
	}
	_, err := toml.DecodeFile(*configPath, &config)
	if err != nil {
		return errs.Errorf("failed parsing config: %w", err)
	}

	var slack *Slack
	if config.SlackURL != "" {
		slack = NewSlack(config.SlackURL)
	}

	log.Printf("listening on %v", config.Addr)
	return http.ListenAndServe(config.Addr, NewHandler(config.Repo, slack))
}

type Handler struct {
	repos map[string]Repo
	mtxs  map[string]*sync.Mutex
	slack *Slack
}

func NewHandler(repos []Repo, slack *Slack) *Handler {
	h := &Handler{
		repos: make(map[string]Repo, len(repos)),
		mtxs:  make(map[string]*sync.Mutex, len(repos)),
		slack: slack,
	}
	for _, repo := range repos {
		h.repos[repo.Webhook] = repo
		h.mtxs[repo.Webhook] = &sync.Mutex{}
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	repo, found := h.repos[r.URL.Path]
	if !found {
		log.Printf("no handler found for %q", r.URL.Path)
		http.NotFound(w, r)
		return
	}
	log.Printf("found handler for %v", repo.Webhook)

	h.mtxs[r.URL.Path].Lock()
	defer h.mtxs[r.URL.Path].Unlock()

	err := repo.FetchAndSync()
	if err != nil {
		msg := fmt.Sprintf("failed syncing %v: %+v", repo.Webhook, err)
		log.Println(msg)
		if h.slack != nil {
			err = h.slack.Message(msg)
			if err != nil {
				log.Printf("failed to alert slack: %v", err)
			}
		}
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	log.Printf("successfully synced")
	_, _ = w.Write([]byte("success\n"))
}

type Slack struct {
	webhook string
}

func NewSlack(webhook string) *Slack {
	return &Slack{webhook: webhook}
}

func (s *Slack) Message(msg string) error {
	type body struct {
		Text string `json:"text"`
	}
	data, err := json.Marshal(body{Text: msg})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, s.webhook, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response from slack: %v", resp.Status)
	}
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if string(data) != "ok" {
		return fmt.Errorf("unexpected response from slack: %v", string(data))
	}
	return nil
}
