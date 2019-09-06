package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/zeebo/errs/v2"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
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
	Webhook  string
	Path     string
	Remotes  []string
	Branches []string
	Tags     bool
}

type CommitList []*object.Commit

func (l CommitList) Len() int      { return len(l) }
func (l CommitList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l CommitList) Less(i, j int) bool {
	return l[i].Committer.When.Before(l[j].Committer.When)
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

var terribleMutex sync.Mutex

func (r *Repo) SyncBranch(repo *git.Repository, branch string) error {
	refs := make(map[string]*plumbing.Hash, len(r.Remotes))
	for _, remote := range r.Remotes {
		hash, err := repo.ResolveRevision(plumbing.Revision(
			fmt.Sprintf("refs/remotes/%s/%s", remote, branch)))
		if err != nil {
			return errs.Wrap(err)
		}
		refs[remote] = hash
	}

	latest, err := IdentifyLatest(repo, refs)
	if err != nil {
		return errs.Errorf("problem syncing branch %v: %v", branch, err)
	}

	// TODO: it would be really nice to ignore the worktree but i can't seem
	// to figure out how to push without updating a local branch and i can't
	// seem to figure out how to update a local branch without involving the
	// worktree. no, a push refspec of <hash>:refs/heads/<branch> does not
	// work. ALSO this means we have to synchronize all calls to this method
	// system-wide. UGH
	terribleMutex.Lock()
	defer terribleMutex.Unlock()
	wt, err := repo.Worktree()
	if err != nil {
		return errs.Wrap(err)
	}
	err = wt.Checkout(&git.CheckoutOptions{
		Branch: plumbing.ReferenceName(fmt.Sprintf("refs/heads/%s", branch)),
		Create: false,
	})
	if err != nil {
		return errs.Wrap(err)
	}
	err = wt.Reset(&git.ResetOptions{
		Commit: latest.Hash,
		Mode:   git.HardReset,
	})
	if err != nil {
		return errs.Wrap(err)
	}

	for remote, hash := range refs {
		if latest.Hash == *hash {
			continue
		}
		log.Printf("updating %v/%s to %v", remote, branch, latest.Hash.String())
		err = repo.Push(&git.PushOptions{
			RemoteName: remote,
			RefSpecs: []config.RefSpec{config.RefSpec(
				fmt.Sprintf("refs/heads/%s:refs/heads/%s", branch, branch))}})
		if err != nil && err != git.NoErrAlreadyUpToDate {
			return errs.Wrap(err)
		}
	}
	return nil
}

func (r *Repo) FetchAll(repo *git.Repository) error {
	for _, remote := range r.Remotes {
		err := repo.Fetch(&git.FetchOptions{
			RemoteName: remote,
			Tags:       git.AllTags,
		})
		if err != nil && err != git.NoErrAlreadyUpToDate {
			return errs.Wrap(err)
		}
	}
	return nil
}

func (r *Repo) SyncTags(repo *git.Repository) error {
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
			if err := r.SyncTag(repo, remote, tag); err != nil {
				return errs.Wrap(err)
			}
		}
	}

	return nil
}

func (r *Repo) SyncTag(repo *git.Repository, remote, tag string) error {
	hash, err := repo.ResolveRevision(plumbing.Revision(tag))
	if err != nil {
		return errs.Wrap(err)
	}

	// TODO: it would be really nice to ignore the worktree but i can't seem
	// to figure out how to push without updating a local branch and i can't
	// seem to figure out how to update a local branch without involving the
	// worktree. no, a push refspec of <hash>:refs/heads/<branch> does not
	// work. ALSO this means we have to synchronize all calls to this method
	// system-wide. UGH
	terribleMutex.Lock()
	defer terribleMutex.Unlock()
	wt, err := repo.Worktree()
	if err != nil {
		return errs.Wrap(err)
	}
	err = wt.Checkout(&git.CheckoutOptions{
		Branch: plumbing.ReferenceName(tag),
		Create: false,
	})
	if err != nil {
		return errs.Wrap(err)
	}
	err = wt.Reset(&git.ResetOptions{
		Commit: *hash,
		Mode:   git.HardReset,
	})
	if err != nil {
		return errs.Wrap(err)
	}

	log.Printf("pushing %s (%v) to %s", tag, hash.String(), remote)
	err = repo.Push(&git.PushOptions{
		RemoteName: remote,
		RefSpecs:   []config.RefSpec{config.RefSpec(fmt.Sprintf("%s:%s", tag, tag))}})
	if err != nil && err != git.NoErrAlreadyUpToDate {
		return errs.Wrap(err)
	}

	return nil
}

func (r *Repo) FetchAndSync() error {
	repo, err := git.PlainOpen(r.Path)
	if err != nil {
		return errs.Wrap(err)
	}

	err = r.FetchAll(repo)
	if err != nil {
		return errs.Wrap(err)
	}

	if r.Tags {
		err := r.SyncTags(repo)
		if err != nil {
			return errs.Wrap(err)
		}
	}

	for _, branch := range r.Branches {
		err := r.SyncBranch(repo, branch)
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
		Addr string
		Repo []Repo
	}
	_, err := toml.DecodeFile(*configPath, &config)
	if err != nil {
		return errs.Errorf("failed parsing config: %w", err)
	}
	log.Printf("listening on %v", config.Addr)
	return http.ListenAndServe(config.Addr, NewHandler(config.Repo))
}

type Handler struct {
	repos map[string]Repo
}

func NewHandler(repos []Repo) *Handler {
	m := make(map[string]Repo, len(repos))
	for _, repo := range repos {
		m[repo.Webhook] = repo
	}
	return &Handler{repos: m}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	repo, found := h.repos[r.URL.Path]
	if !found {
		log.Printf("no handler found for %q", r.URL.Path)
		http.NotFound(w, r)
		return
	}
	log.Printf("found handler for %v", repo.Webhook)

	err := repo.FetchAndSync()
	if err != nil {
		log.Printf("failed syncing %v: %+v", repo.Webhook, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("successfully synced")
	w.Write([]byte("success"))
}
