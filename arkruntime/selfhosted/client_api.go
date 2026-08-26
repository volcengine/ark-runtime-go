// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package selfhosted

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/volcengine/ark-runtime-go/arkruntime"
	"github.com/volcengine/ark-runtime-go/arkruntime/model"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/environment"
	"github.com/volcengine/ark-runtime-go/arkruntime/model/session"
)

// ClientAPI adapts arkruntime.Client to the self-hosted worker control-plane API.
type ClientAPI struct {
	client *arkruntime.Client
}

// NewClientAPI creates a self-hosted worker API backed by arkruntime.Client.
func NewClientAPI(client *arkruntime.Client) *ClientAPI {
	return &ClientAPI{client: client}
}

// PollWork polls one work item from the Environment queue.
func (a *ClientAPI) PollWork(ctx context.Context, req PollWorkRequest) (*WorkItem, error) {
	if a == nil || a.client == nil {
		return nil, errors.New("selfhosted: arkruntime client is nil")
	}
	item, err := a.client.PollWork(ctx, &req)
	if err != nil {
		return nil, toWorkerAPIError(err)
	}
	return item, nil
}

// AckWork acknowledges one claimed work item.
func (a *ClientAPI) AckWork(ctx context.Context, req AckWorkRequest) error {
	if a == nil || a.client == nil {
		return errors.New("selfhosted: arkruntime client is nil")
	}
	err := a.client.AckWork(ctx, &req)
	return toWorkerAPIError(err)
}

// HeartbeatWork refreshes a claimed work lease.
func (a *ClientAPI) HeartbeatWork(ctx context.Context, req HeartbeatWorkRequest) (*HeartbeatResponse, error) {
	if a == nil || a.client == nil {
		return nil, errors.New("selfhosted: arkruntime client is nil")
	}
	expectedLastHeartbeat, ok := req.ExpectedLastHeartbeat.Get()
	if !ok || expectedLastHeartbeat == "" {
		req.ExpectedLastHeartbeat = environment.NewOptString(ExpectedLastHeartbeatNoHeartbeat)
	}
	resp, err := a.client.HeartbeatWork(ctx, &req)
	if err != nil {
		return nil, toWorkerAPIError(err)
	}
	return resp, nil
}

// StopWork releases or stops one claimed work item.
func (a *ClientAPI) StopWork(ctx context.Context, req StopWorkRequest) error {
	if a == nil || a.client == nil {
		return errors.New("selfhosted: arkruntime client is nil")
	}
	err := a.client.StopWork(ctx, &req)
	return toWorkerAPIError(err)
}

// GetSession reads a session snapshot.
func (a *ClientAPI) GetSession(ctx context.Context, req GetSessionRequest) (*Session, error) {
	if a == nil || a.client == nil {
		return nil, errors.New("selfhosted: arkruntime client is nil")
	}
	out, err := a.client.GetSession(ctx, req.SessionID)
	if err != nil {
		return nil, toWorkerAPIError(err)
	}
	return fromRuntimeSession(out), nil
}

// ListEvents lists session events in ascending order by default.
func (a *ClientAPI) ListEvents(ctx context.Context, req ListEventsRequest) (*ListEventsResponse, error) {
	if a == nil || a.client == nil {
		return nil, errors.New("selfhosted: arkruntime client is nil")
	}
	params := &session.SessionEventsListEventsParams{Types: req.Types}
	if req.Page != "" {
		params.Page = session.NewOptString(req.Page)
	}
	if req.CreatedAtGt != "" {
		params.CreatedAtGt = session.NewOptString(req.CreatedAtGt)
	}
	if req.Limit > 0 {
		params.Limit = session.NewOptInt32(int32(req.Limit))
	}
	switch req.Order {
	case EventListOrderDesc:
		params.Order = session.NewOptListSessionsOrder(session.ListSessionsOrderDesc)
	case EventListOrderAsc:
		params.Order = session.NewOptListSessionsOrder(session.ListSessionsOrderAsc)
	}
	resp, err := a.client.ListSessionEvents(ctx, req.SessionID, params)
	if err != nil {
		return nil, toWorkerAPIError(err)
	}
	out := &ListEventsResponse{}
	if resp == nil {
		return out, nil
	}
	if next, ok := resp.NextPage.Get(); ok {
		out.NextPage = next
	}
	out.Events = make([]Event, 0, len(resp.Events))
	for _, ev := range resp.Events {
		converted, err := fromManagedAgentEvent(ev)
		if err != nil {
			return nil, err
		}
		out.Events = append(out.Events, converted)
	}
	return out, nil
}

// StreamEvents opens a session event SSE stream.
func (a *ClientAPI) StreamEvents(ctx context.Context, req StreamEventsRequest) (*EventStream, error) {
	if a == nil || a.client == nil {
		return nil, errors.New("selfhosted: arkruntime client is nil")
	}
	streamCtx, streamCancel := context.WithCancel(ctx)
	decoder, err := a.client.StreamSessionEventsWithParams(streamCtx, req.SessionID, &session.SessionEventsStreamEventsParams{
		EventDeltas: req.EventDeltas,
	})
	if err != nil {
		streamCancel()
		return nil, toWorkerAPIError(err)
	}
	events := make(chan Event)
	var mu sync.Mutex
	var streamErr error
	var closeOnce sync.Once
	setErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		streamErr = err
	}
	stream := &EventStream{
		events: events,
		close: func() error {
			var err error
			closeOnce.Do(func() {
				streamCancel()
				err = decoder.Close()
			})
			return err
		},
		err: func() error {
			mu.Lock()
			defer mu.Unlock()
			return streamErr
		},
	}
	go func() {
		defer close(events)
		defer func() { _ = stream.Close() }()
		for decoder.Next() {
			event, err := fromStreamEvent(decoder.Event())
			if err != nil {
				setErr(err)
				return
			}
			select {
			case <-streamCtx.Done():
				if ctx.Err() != nil {
					setErr(ctx.Err())
				}
				return
			case events <- event:
			}
		}
		if err := decoder.Err(); err != nil && streamCtx.Err() == nil {
			setErr(toWorkerAPIError(err))
		}
	}()
	return stream, nil
}

// SendEvent writes one user-side event back to the session.
func (a *ClientAPI) SendEvent(ctx context.Context, req SendEventRequest) error {
	if a == nil || a.client == nil {
		return errors.New("selfhosted: arkruntime client is nil")
	}
	return toWorkerAPIError(a.client.SendSessionEventRaw(ctx, req.SessionID, req.Event))
}

// ResolveSkill enriches a session skill reference with control-plane metadata.
func (a *ClientAPI) ResolveSkill(ctx context.Context, ref SkillRef) (SkillRef, error) {
	if a == nil || a.client == nil {
		return SkillRef{}, errors.New("selfhosted: arkruntime client is nil")
	}
	skillID := strings.TrimSpace(ref.IDValue())
	if skillID == "" {
		return SkillRef{}, errors.New("skill id is required")
	}
	metadata, err := a.client.GetSkill(ctx, skillID)
	if err != nil {
		return SkillRef{}, toWorkerAPIError(err)
	}
	if metadata == nil {
		return SkillRef{}, fmt.Errorf("skill metadata is empty: %s", skillID)
	}
	name, _ := metadata.Name.Get()
	name = strings.TrimSpace(name)
	if name == "" {
		return SkillRef{}, fmt.Errorf("skill name is empty: %s", skillID)
	}

	ref.Name = name
	if ref.DisplayName == "" {
		ref.DisplayName = strings.TrimSpace(metadata.DisplayTitle)
	}
	if ref.Type == "" {
		ref.Type = strings.TrimSpace(metadata.Source)
	}
	if ref.Version == "" {
		ref.Version = strings.TrimSpace(metadata.LatestVersion)
	}
	return ref, nil
}

// OpenSkill opens a skill archive stream.
func (a *ClientAPI) OpenSkill(ctx context.Context, req OpenSkillRequest) (*SkillContent, error) {
	if a == nil || a.client == nil {
		return nil, errors.New("selfhosted: arkruntime client is nil")
	}
	if req.Skill.DownloadURL != "" {
		return openSignedSkillURL(ctx, a.client.HTTPClient(), req.Skill.DownloadURL)
	}
	if strings.EqualFold(strings.TrimSpace(req.Skill.Type), skillTypeSkillHub) {
		return a.openSkillHub(ctx, req.Skill)
	}
	content, err := a.client.OpenSkillContent(ctx, req.Skill.IDValue(), req.Skill.Version)
	if err != nil {
		return nil, toWorkerAPIError(err)
	}
	return &SkillContent{
		Body:          content.Body,
		ContentLength: content.ContentLength,
		FileName:      content.FileName,
		ContentType:   content.ContentType,
	}, nil
}

func fromRuntimeSession(in *session.Session) *Session {
	if in == nil {
		return nil
	}
	out := &Session{ID: in.ID}
	data, err := json.Marshal(in)
	if err != nil {
		return out
	}
	if err := json.Unmarshal(data, out); err != nil {
		out.ID = in.ID
		return out
	}
	if out.ID == "" {
		out.ID = in.ID
	}
	return out
}

func fromStreamEvent(frame session.StreamEvent) (Event, error) {
	if len(frame.RawPayload) > 0 {
		return eventFromRaw(frame.RawPayload, frame.Type, frame.Data.GetID())
	}
	return fromManagedAgentEvent(frame.Data)
}

func fromManagedAgentEvent(ev session.ManagedAgentsSessionEvent) (Event, error) {
	if unknown, ok := ev.(*session.ManagedAgentsUnknownSessionEvent); ok {
		return eventFromRaw(unknown.RawPayload, unknown.Type(), unknown.GetID())
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return Event{}, err
	}
	return eventFromRaw(data, ev.Type(), ev.GetID())
}

func eventFromRaw(data []byte, eventType, eventID string) (Event, error) {
	var out Event
	if err := json.Unmarshal(data, &out); err != nil {
		return Event{}, err
	}
	if out.Type == "" {
		out.Type = eventType
	}
	if out.ID == "" {
		out.ID = eventID
	}
	return out, nil
}

func openSignedSkillURL(ctx context.Context, httpClient *http.Client, rawURL string) (*SkillContent, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		defer resp.Body.Close() //nolint:errcheck // response body close errors are non-actionable
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if len(msg) == 0 {
			msg = []byte(http.StatusText(resp.StatusCode))
		}
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Message:    strings.TrimSpace(string(msg)),
			RequestID:  resp.Header.Get("X-Request-Id"),
		}
	}
	return &SkillContent{
		Body:          resp.Body,
		ContentLength: resp.ContentLength,
		FileName:      path.Base(resp.Request.URL.Path),
		ContentType:   resp.Header.Get("Content-Type"),
	}, nil
}

func toWorkerAPIError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr *model.APIError
	if errors.As(err, &apiErr) {
		return &APIError{
			StatusCode: apiErr.HTTPStatusCode,
			Message:    apiErr.Message,
			RequestID:  apiErr.RequestId,
		}
	}
	var reqErr *model.RequestError
	if errors.As(err, &reqErr) && reqErr.HTTPStatusCode >= http.StatusBadRequest {
		return &APIError{
			StatusCode: reqErr.HTTPStatusCode,
			Message:    fmt.Sprint(reqErr.Err),
			RequestID:  reqErr.RequestId,
		}
	}
	return err
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ API = (*ClientAPI)(nil)
var _ EventStreamer = (*ClientAPI)(nil)
