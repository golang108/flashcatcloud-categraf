package github

import (
	"context"
	"flashcat.cloud/categraf/types"
	"fmt"
	githubLib "github.com/google/go-github/v32/github"
	"golang.org/x/oauth2"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
)

const inputName = "jenkins"

type Github struct {
	config.PluginConfig
	Instances []*Instance `toml:"instances"`
}

func init() {
	inputs.Add(inputName, func() inputs.Input {
		return &Github{}
	})
}

func (j *Github) GetInstances() []inputs.Instance {
	ret := make([]inputs.Instance, len(j.Instances))
	for i := 0; i < len(j.Instances); i++ {
		ret[i] = j.Instances[i]
	}
	return ret
}

type Instance struct {
	config.InstanceConfig

	Repositories      []string `toml:"repositories"`
	AccessToken       string   `toml:"access_token"`
	AdditionalFields  []string `toml:"additional_fields"`
	EnterpriseBaseURL string   `toml:"enterprise_base_url"`

	// HTTP Timeout specified as a string - 3s, 1m, 1h
	ResponseTimeout config.Duration

	githubClient *githubLib.Client

	obfuscatedToken string
}

func (ins *Instance) Init() error {
	return nil
}

func (ins *Instance) Gather(slist *types.SampleList) {
	ctx := context.Background()

	if ins.githubClient == nil {
		client, err := ins.createGitHubClient(ctx)
		if err != nil {
			log.Println("E! failed to new githubClient:", err)
			return
		}
		ins.githubClient = client
	}

	var wg sync.WaitGroup
	wg.Add(len(ins.Repositories))

	for _, repository := range ins.Repositories {
		go func(repositoryName string, slist *types.SampleList) {
			defer wg.Done()

			owner, repository, err := splitRepositoryName(repositoryName)
			if err != nil {
				log.Println("E! splitRepositoryName", err)
				return
			}

			repositoryInfo, response, err := ins.githubClient.Repositories.Get(ctx, owner, repository)
			// ins.handleRateLimit(response, err)
			if err != nil {
				log.Println("E! get repositoryInfo err", err, response)
				return
			}

			tags := getTags(repositoryInfo)
			fields := getFields(repositoryInfo)
			// 如果有额外的 Fields 需要添加
			for _, field := range ins.AdditionalFields {
				switch field {
				case "pull-requests":
					// Pull request properties
					addFields, err := ins.getPullRequestFields(ctx, owner, repository)
					if err != nil {
						log.Println("E! get PullRequestFields err", err)
						continue
					}

					for k, v := range addFields {
						fields[k] = v
					}
				default:
					log.Println("E! unknown additional field", field)
					continue
				}
			}

			slist.PushSamples(inputName, fields, tags)
		}(repository, slist)
	} // end for range ins.Repositories

}

func getLicense(rI *githubLib.Repository) string {
	if licenseName := rI.GetLicense().GetName(); licenseName != "" {
		return licenseName
	}

	return "None"
}

func getTags(repositoryInfo *githubLib.Repository) map[string]string {
	return map[string]string{
		"owner":    repositoryInfo.GetOwner().GetLogin(),
		"name":     repositoryInfo.GetName(),
		"language": repositoryInfo.GetLanguage(),
		"license":  getLicense(repositoryInfo),
	}
}

func getFields(repositoryInfo *githubLib.Repository) map[string]interface{} {
	return map[string]interface{}{
		"stars":       repositoryInfo.GetStargazersCount(),
		"subscribers": repositoryInfo.GetSubscribersCount(),
		"watchers":    repositoryInfo.GetWatchersCount(),
		"networks":    repositoryInfo.GetNetworkCount(),
		"forks":       repositoryInfo.GetForksCount(),
		"open_issues": repositoryInfo.GetOpenIssuesCount(),
		"size":        repositoryInfo.GetSize(),
	}
}

func (ins *Instance) getPullRequestFields(ctx context.Context, owner, repo string) (map[string]interface{}, error) {
	options := githubLib.SearchOptions{
		TextMatch: false,
		ListOptions: githubLib.ListOptions{
			PerPage: 100,
			Page:    1,
		},
	}

	classes := []string{"open", "closed"}
	fields := make(map[string]interface{})
	for _, class := range classes {
		q := fmt.Sprintf("repo:%s/%s is:pr is:%s", owner, repo, class)
		searchResult, response, err := ins.githubClient.Search.Issues(ctx, q, &options)
		// ins.handleRateLimit(response, err)
		if err != nil {
			log.Println("E! github search failed", response)
			return fields, err
		}

		f := fmt.Sprintf("%s_pull_requests", class)
		fields[f] = searchResult.GetTotal()
	}

	return fields, nil
}

// Create GitHub Client
func (ins *Instance) createGitHubClient(ctx context.Context) (*githubLib.Client, error) {
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
		},
		Timeout: time.Duration(ins.ResponseTimeout),
	}

	ins.obfuscatedToken = "Unauthenticated"

	if ins.AccessToken != "" {
		tokenSource := oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: ins.AccessToken},
		)
		oauthClient := oauth2.NewClient(ctx, tokenSource)
		_ = context.WithValue(ctx, oauth2.HTTPClient, oauthClient)

		ins.obfuscatedToken = ins.AccessToken[0:4] + "..." + ins.AccessToken[len(ins.AccessToken)-3:]

		return ins.newGithubClient(oauthClient)
	}

	return ins.newGithubClient(httpClient)
}

func (ins *Instance) newGithubClient(httpClient *http.Client) (*githubLib.Client, error) {
	if ins.EnterpriseBaseURL != "" {
		return githubLib.NewEnterpriseClient(ins.EnterpriseBaseURL, "", httpClient)
	}
	return githubLib.NewClient(httpClient), nil
}

func splitRepositoryName(repositoryName string) (owner string, repository string, err error) {
	splits := strings.SplitN(repositoryName, "/", 2)

	if len(splits) != 2 {
		return "", "", fmt.Errorf("%v is not of format 'owner/repository'", repositoryName)
	}

	return splits[0], splits[1], nil
}
