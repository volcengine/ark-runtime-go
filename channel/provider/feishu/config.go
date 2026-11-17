// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/volcengine/ark-runtime-go/channel"
	"github.com/volcengine/ark-runtime-go/channel/internal/handles"
	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/oauth"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/userauth"
	"github.com/volcengine/ark-runtime-go/channel/secret"
	"github.com/volcengine/ark-runtime-go/channel/store"
)

// UserAuthorization configures user Device OAuth; the Store must have explicitly
// installed authorization structures and enabled payload encryption.
type UserAuthorization struct {
	// Namespace is a deployment-defined identity-isolation name, stable across restarts,
	// rather than a platform account identifier.
	Namespace       string
	Store           *channel.Store
	MA              *channel.AgentClient
	Codec           store.Codec
	AllowedHosts    []string
	AccountsBaseURL string
}

// Config holds non-secret settings and an explicit secret resolver.
type Config struct {
	Binding              channel.Binding
	AppSecretRef         string
	SecretResolver       secret.Resolver
	HTTPClient           *http.Client
	OpenBaseURL          string
	BotOpenID            string
	ReceiveMode          string
	VerificationTokenRef string
	EncryptKeyRef        string
	UserAuthorization    *UserAuthorization
}

// New constructs a Feishu channel without global credential discovery or implicit database creation.
func New(config Config) (*channel.ProviderHandle, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return NewWithContext(ctx, config)
}

// NewWithContext validates the cloud Environment and application user permissions
// for user authorization. Initialization honors ctx cancellation.
func NewWithContext(ctx context.Context, config Config) (*channel.ProviderHandle, error) {
	if config.SecretResolver == nil {
		return nil, errors.New("feishu secret resolver required")
	}
	if ctx == nil {
		return nil, errors.New("feishu initialization context required")
	}
	if a := config.UserAuthorization; a != nil && strings.TrimSpace(a.Namespace) == "" {
		return nil, errors.New("feishu user authorization requires stable namespace")
	}

	raw, err := json.Marshal(config.Binding)
	if err != nil {
		return nil, errors.New("invalid feishu binding")
	}
	var binding core.Binding
	if json.Unmarshal(raw, &binding) != nil {
		return nil, errors.New("invalid feishu binding")
	}
	var p *Provider
	var authorizer core.UserAuthorizer
	if a := config.UserAuthorization; a != nil {
		if !handles.UserAuthEncryptionReady(a.Store) {
			return nil, errors.New("feishu user authorization requires encrypted store")
		}
		if a.Codec == nil {
			return nil, errors.New("feishu user authorization requires encryption")
		}
		credentials, ok := handles.AgentValue(a.MA).(core.UserCredentialWriter)
		if !ok {
			return nil, errors.New("feishu MA credential writer unavailable")
		}
		authStore, ok := handles.StoreValue(a.Store).(core.UserAuthStore)
		if !ok {
			return nil, errors.New("feishu user authorization storage unavailable")
		}
		appSecret, err := config.SecretResolver.Resolve(config.AppSecretRef)
		if err != nil {
			return nil, errors.New("feishu app secret unavailable")
		}
		accounts := a.AccountsBaseURL
		if accounts == "" && config.OpenBaseURL == "https://open.larksuite.com" {
			accounts = "https://accounts.larksuite.com"
		}
		hosts := a.AllowedHosts
		if len(hosts) == 0 && config.OpenBaseURL == "https://open.larksuite.com" {
			hosts = []string{"open.larksuite.com"}
		}
		protocol, err := oauth.New(oauth.Config{AppID: binding.AppID, AppSecret: appSecret, AccountsBaseURL: accounts, OpenBaseURL: config.OpenBaseURL, HTTPClient: config.HTTPClient})
		if err != nil {
			return nil, err
		}
		environment, ok := handles.AgentValue(a.MA).(core.EnvironmentReader)
		if !ok {
			return nil, errors.New("feishu environment reader unavailable")
		}
		env, err := environment.GetEnvironment(ctx, binding.EnvironmentID)
		if err != nil || env.Type != "cloud" {
			return nil, errors.New("feishu user authorization requires cloud environment")
		}
		applicationScopes, err := protocol.ApplicationUserScopes(ctx)
		if err != nil {
			return nil, err
		}
		allowed := make(map[string]bool, len(applicationScopes))
		for _, scope := range applicationScopes {
			allowed[scope] = true
		}
		if len(binding.UserAuthScopes) == 0 {
			binding.UserAuthScopes = applicationScopes
		} else {
			for _, scope := range binding.UserAuthScopes {
				if !allowed[scope] {
					return nil, errors.New("feishu user scope unavailable")
				}
			}
		}
		binding.Owner = "local:" + a.Namespace
		binding.Project = "channel"
		binding.ChannelID = binding.ID
		binding.BindingEpoch = 1
		if _, err := authStore.GetUserAuth(ctx, core.UserPrincipal{Owner: binding.Owner, Project: binding.Project, ChannelID: binding.ChannelID, Epoch: binding.BindingEpoch, AppID: binding.AppID, TenantKey: "storage-probe", OpenID: "storage-probe"}); err != nil {
			return nil, errors.New("feishu user authorization storage not installed")
		}
		authorizer, err = userauth.New(userauth.Config{AppID: binding.AppID, OAuth: protocol, Credentials: credentials, Store: authStore, Codec: a.Codec, AllowedHosts: hosts})
		if err != nil {
			return nil, err
		}
	}
	p, err = NewProvider(ProviderConfig{Binding: binding, AppSecretRef: config.AppSecretRef, SecretResolver: config.SecretResolver, HTTPClient: config.HTTPClient, OpenBaseURL: config.OpenBaseURL, BotOpenID: config.BotOpenID, ReceiveMode: config.ReceiveMode, VerificationTokenRef: config.VerificationTokenRef, EncryptKeyRef: config.EncryptKeyRef})
	if err != nil {
		return nil, err
	}
	key, err := handles.BindingKey(config.Binding)
	if err != nil {
		return nil, errors.New("invalid feishu binding")
	}
	var authStore *channel.Store
	var authAgent *channel.AgentClient
	if config.UserAuthorization != nil {
		authStore = config.UserAuthorization.Store
		authAgent = config.UserAuthorization.MA
	}
	return handles.NewBoundProvider(p, authorizer, binding, key, authStore, authAgent), nil
}

// WebhookHandler returns the verified callback entry point for ReceiveWebhook.
func WebhookHandler(provider *channel.ProviderHandle) (http.Handler, error) {
	p, ok := handles.ProviderValue(provider).(interface{ WebhookHandler() http.Handler })
	if !ok {
		return nil, errors.New("feishu webhook unavailable")
	}
	h := p.WebhookHandler()
	if h == nil {
		return nil, errors.New("feishu webhook not initialized")
	}
	return h, nil
}
