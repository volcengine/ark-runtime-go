// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"encoding/base64"
	"errors"
	"sort"
	"strings"

	storecodec "github.com/volcengine/ark-runtime-go/channel/store"

	core "github.com/volcengine/ark-runtime-go/channel/internal/runtime"
	"github.com/volcengine/ark-runtime-go/channel/internal/storage/codec"
	"github.com/volcengine/ark-runtime-go/channel/ma"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/oauth"
	"github.com/volcengine/ark-runtime-go/channel/provider/feishu/userauth"
	"github.com/volcengine/ark-runtime-go/channel/secret"
)

// UserAuthConfig 定义部署的稳定身份与 AES-256 密钥引用；Owner 是显式配置，不能从 API key 推断。
type UserAuthConfig struct {
	Owner            string `json:"owner"`
	Project          string `json:"project"`
	EncryptionKeyRef string `json:"encryption_key_ref"`
}

// UserAuthConnectionConfig 定义渠道代次和实际要求的权限；请求 scope 不代表已授予。
type UserAuthConnectionConfig struct {
	ChannelID string   `json:"channel_id"`
	Epoch     uint64   `json:"epoch"`
	Scopes    []string `json:"scopes"`
}

func (c UserAuthConfig) enabled() bool {
	return c.Owner != "" || c.Project != "" || c.EncryptionKeyRef != ""
}
func (c UserAuthConfig) validate() error {
	if !c.enabled() {
		return nil
	}
	if !validID(c.Owner) || !validID(c.Project) || secret.Validate(c.EncryptionKeyRef) != nil {
		return errors.New("user_auth requires owner, project and encryption_key_ref")
	}
	return nil
}
func (c ConnectionConfig) validateUserAuth(global UserAuthConfig) error {
	if !global.enabled() || !validID(c.UserAuth.ChannelID) || c.UserAuth.Epoch == 0 {
		return errIdentityUserUnsupported
	}
	for _, scope := range c.UserAuth.Scopes {
		if !validID(scope) {
			return errors.New("user_auth scopes invalid")
		}
	}
	// 运行配置仅允许官方固定服务；测试协议端口可显式注入本地 HTTP server。
	if c.Feishu.OpenBaseURL != "" && c.Feishu.OpenBaseURL != "https://open.feishu.cn" && c.Feishu.OpenBaseURL != "https://open.larksuite.com" {
		return errors.New("user_auth requires a supported official feishu endpoint")
	}
	return nil
}
func userAuthCodec(config Config, resolver secret.Resolver) (codec.Codec, error) {
	if config.payloadCodec != nil {
		return config.payloadCodec, nil
	}
	if !config.UserAuth.enabled() {
		return nil, nil
	}
	if resolver == nil {
		resolver = secret.EnvFileResolver{}
	}
	value, err := resolver.Resolve(config.UserAuth.EncryptionKeyRef)
	if err != nil {
		return nil, errors.New("user_auth encryption key unavailable")
	}
	key, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSpace(value))
	if err != nil || len(key) != 32 {
		return nil, errors.New("user_auth encryption key must be base64 of 32 bytes")
	}
	crypt, err := storecodec.NewAESGCM("runner-user-auth-v1", key)
	clear(key)
	return crypt, err
}
func configureUserBinding(binding *core.Binding, config Config, item ConnectionConfig) {
	if !config.UserAuth.enabled() {
		return
	}
	binding.Owner = config.UserAuth.Owner
	binding.Project = config.UserAuth.Project
	if item.resolvedIdentity.P2P != core.IdentityUser {
		return
	}
	binding.ChannelID = item.UserAuth.ChannelID
	binding.BindingEpoch = item.UserAuth.Epoch
	binding.UserAuthScopes = append([]string(nil), item.UserAuth.Scopes...)
}
func newUserAuthorizer(ctx context.Context, config Config, item *ConnectionConfig, store core.Store, client core.MAClient, resolver secret.Resolver, factory func(oauth.Config) (*oauth.Client, error)) (core.UserAuthorizer, error) {
	if item.resolvedIdentity.P2P != core.IdentityUser {
		return nil, nil
	}
	authStore, ok := store.(core.UserAuthStore)
	credentials, cok := client.(ma.UserCredentialWriter)
	environment, eok := client.(core.EnvironmentReader)
	if !ok || !cok || !eok {
		return nil, errors.New("user_auth ports unavailable")
	}
	env, err := environment.GetEnvironment(ctx, item.EnvironmentID)
	if err != nil || env.Type != "cloud" {
		return nil, errors.New("user_auth requires a readable cloud environment")
	}
	appSecret, err := resolver.Resolve(item.Feishu.AppSecretRef)
	if err != nil || appSecret == "" {
		return nil, errors.New("user_auth app secret unavailable")
	}
	accounts := ""
	hosts := []string{"open.feishu.cn"}
	if item.Feishu.OpenBaseURL == "https://open.larksuite.com" {
		accounts = "https://accounts.larksuite.com"
		hosts = []string{"open.larksuite.com"}
	}
	if factory == nil {
		factory = oauth.New
	}
	protocol, err := factory(oauth.Config{AppID: item.Feishu.AppID, AppSecret: appSecret, OpenBaseURL: item.Feishu.OpenBaseURL, AccountsBaseURL: accounts})
	if err != nil {
		return nil, errors.New("user_auth protocol unavailable")
	}
	scopes, err := protocol.ApplicationUserScopes(ctx)
	if err != nil {
		return nil, errors.New("user_auth application scopes unavailable")
	}
	scopes, err = selectApplicationUserScopes(scopes, item.UserAuth.Scopes)
	if err != nil {
		return nil, err
	}
	item.UserAuth.Scopes = scopes
	return userauth.New(userauth.Config{AppID: item.Feishu.AppID, OAuth: protocol, Credentials: credentials, Store: authStore, Codec: config.payloadCodec, AllowedHosts: hosts})
}
func checkUserAuthStorage(ctx context.Context, config Config, store core.Store) error {
	if !config.UserAuth.enabled() {
		return nil
	}
	auth, ok := store.(core.UserAuthStore)
	if !ok {
		return errors.New("user_auth store unavailable")
	}
	for _, item := range config.Connections {
		if item.resolvedIdentity.P2P == core.IdentityUser {
			p := core.UserPrincipal{Owner: config.UserAuth.Owner, Project: config.UserAuth.Project, ChannelID: item.UserAuth.ChannelID, Epoch: item.UserAuth.Epoch, AppID: item.Feishu.AppID, TenantKey: "storage-probe", OpenID: "storage-probe"}
			if _, err := auth.GetUserAuth(ctx, p); err != nil {
				return errors.New("user_auth storage unavailable; stop runner and run channel-runner expand-user-auth --config PATH")
			}
		}
	}
	return nil
}

func selectApplicationUserScopes(application, explicit []string) ([]string, error) {
	if len(explicit) == 0 {
		return append([]string(nil), application...), nil
	}
	allowed := map[string]bool{}
	for _, scope := range application {
		allowed[scope] = true
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(explicit))
	for _, scope := range explicit {
		if !allowed[scope] {
			return nil, errors.New("user_auth scopes must belong to application user scopes")
		}
		if !seen[scope] {
			seen[scope] = true
			result = append(result, scope)
		}
	}
	sort.Strings(result)
	return result, nil
}
