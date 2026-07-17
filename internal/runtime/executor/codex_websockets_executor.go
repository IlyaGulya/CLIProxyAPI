// Package executor provides runtime execution capabilities for various AI service providers.
// This file implements a Codex executor that uses the Responses API WebSocket transport.
package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/observability"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	cliproxyusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/net/proxy"
)

const (
	codexResponsesWebsocketBetaHeaderValue = "responses_websockets=2026-02-06"
	codexResponsesWebsocketIdleTimeout     = 5 * time.Minute
	codexResponsesWebsocketHandshakeTO     = 30 * time.Second
	codexWebsocketMaxIncrementalStateBytes = 8 << 20
)

// CodexWebsocketsExecutor executes Codex Responses requests using a WebSocket transport.
//
// It preserves the existing CodexExecutor HTTP implementation as a fallback for endpoints
// not available over WebSocket (e.g. /responses/compact) and for websocket upgrade failures.
type CodexWebsocketsExecutor struct {
	*CodexExecutor

	store         *codexWebsocketSessionStore
	pool          *codexWebsocketPreconnectPool
	draining      bool
	drainMu       sync.RWMutex
	drainWG       sync.WaitGroup
	runtimeCtx    context.Context
	runtimeCancel context.CancelFunc
	backgroundWG  sync.WaitGroup
}

type codexWebsocketSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*codexWebsocketSession
}

type codexWebsocketSession struct {
	sessionID string
	lastUsed  time.Time

	reqMu sync.Mutex

	stateMu            sync.Mutex
	httpFallback       bool
	lastRequest        []byte
	lastResponseID     string
	lastResponseOutput []byte
	pendingRequest     []byte

	connMu sync.Mutex
	conn   *websocket.Conn
	wsURL  string
	authID string

	writeMu sync.Mutex

	observationMu        sync.Mutex
	observedConn         *websocket.Conn
	observedConnAt       time.Time
	observedConnUseCount int64

	activeMu     sync.Mutex
	activeCh     chan codexWebsocketRead
	activeDone   <-chan struct{}
	activeCancel context.CancelFunc

	readerConn *websocket.Conn

	upstreamDisconnectOnce sync.Once
	upstreamDisconnectCh   chan error
}

func (s *codexWebsocketSession) observeConnectionUse(conn *websocket.Conn) (time.Duration, int64) {
	if s == nil || conn == nil {
		return 0, 1
	}
	now := time.Now()
	s.observationMu.Lock()
	if s.observedConn != conn {
		s.observedConn = conn
		s.observedConnAt = now
		s.observedConnUseCount = 0
	}
	s.observedConnUseCount++
	age := now.Sub(s.observedConnAt)
	count := s.observedConnUseCount
	s.observationMu.Unlock()
	return age, count
}

func (s *codexWebsocketSession) connectionObservation(conn *websocket.Conn) (time.Duration, int64) {
	if s == nil || conn == nil {
		return 0, 1
	}
	now := time.Now()
	s.observationMu.Lock()
	if s.observedConn != conn {
		s.observedConn = conn
		s.observedConnAt = now
		s.observedConnUseCount = 1
	}
	age := now.Sub(s.observedConnAt)
	count := s.observedConnUseCount
	s.observationMu.Unlock()
	return age, count
}

func NewCodexWebsocketsExecutor(cfg *config.Config) *CodexWebsocketsExecutor {
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	return &CodexWebsocketsExecutor{
		CodexExecutor: NewCodexExecutor(cfg),
		store:         newCodexWebsocketSessionStore(),
		pool:          newCodexWebsocketPreconnectPool(runtimeCtx),
		runtimeCtx:    runtimeCtx,
		runtimeCancel: runtimeCancel,
	}
}

func newCodexWebsocketSessionStore() *codexWebsocketSessionStore {
	return &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
}

type codexWebsocketRead struct {
	conn    *websocket.Conn
	msgType int
	payload []byte
	err     error
}

func (s *codexWebsocketSession) setActive(ch chan codexWebsocketRead) {
	if s == nil {
		return
	}
	s.activeMu.Lock()
	if s.activeCancel != nil {
		s.activeCancel()
		s.activeCancel = nil
		s.activeDone = nil
	}
	s.activeCh = ch
	if ch != nil {
		activeCtx, activeCancel := context.WithCancel(context.Background())
		s.activeDone = activeCtx.Done()
		s.activeCancel = activeCancel
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) clearActive(ch chan codexWebsocketRead) {
	if s == nil {
		return
	}
	s.activeMu.Lock()
	if s.activeCh == ch {
		s.activeCh = nil
		if s.activeCancel != nil {
			s.activeCancel()
		}
		s.activeCancel = nil
		s.activeDone = nil
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) writeMessage(conn *websocket.Conn, msgType int, payload []byte) error {
	if s == nil {
		return fmt.Errorf("codex websockets executor: session is nil")
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteMessage(msgType, payload)
}

func (s *codexWebsocketSession) configureConn(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	conn.SetPingHandler(func(appData string) error {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		// Reply pongs from the same write lock to avoid concurrent writes.
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
	})
}

func (s *codexWebsocketSession) notifyUpstreamDisconnect(err error) {
	if s == nil {
		return
	}
	s.upstreamDisconnectOnce.Do(func() {
		if s.upstreamDisconnectCh == nil {
			return
		}
		select {
		case s.upstreamDisconnectCh <- err:
		default:
		}
		close(s.upstreamDisconnectCh)
	})
}

func (e *CodexWebsocketsExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Alt == "responses/compact" {
		return e.CodexExecutor.executeCompact(ctx, auth, req, opts)
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("codex")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	originalTranslated, body := translateCodexRequestPair(from, to, baseModel, originalPayload, req.Payload, false)

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", body, originalTranslated, requestedModel, requestPath, opts.Headers)
	body, _ = sjson.SetBytes(body, "model", baseModel)
	body, _ = sjson.SetBytes(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body = normalizeCodexInstructions(body)
	if e.cfg == nil || e.cfg.DisableImageGeneration == config.DisableImageGenerationOff {
		body = ensureImageGenerationTool(body, baseModel, auth, opts.Headers)
	}
	body = sanitizeOpenAIResponsesReasoningEncryptedContent(ctx, "codex websockets executor", body)

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return resp, err
	}

	body, wsHeaders, errPromptCache := applyCodexPromptCacheHeadersWithContext(ctx, from, req, body, auth.ID)
	if errPromptCache != nil {
		return resp, errPromptCache
	}
	body = canonicalizeCodexCacheableRequest(body)
	clientBody := body
	var identityState codexIdentityConfuseState
	upstreamBody, identityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, body)
	reporter.SetTranslatedReasoningEffort(clientBody, to.String())
	wsHeaders = applyCodexWebsocketHeaders(ctx, wsHeaders, auth, apiKey, e.cfg)
	applyModelHeaderOverrides(wsHeaders, baseModel)
	applyCodexIdentityConfuseHeaders(wsHeaders, &identityState)

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		sess.reqMu.Lock()
		defer sess.reqMu.Unlock()
	}

	wsReqBody := buildCodexWebsocketRequestBody(upstreamBody)
	wsReqLog := helps.UpstreamRequestLog{
		URL:       wsURL,
		Method:    "WEBSOCKET",
		Headers:   wsHeaders.Clone(),
		Body:      wsReqBody,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	}
	helps.RecordAPIWebsocketRequest(ctx, e.cfg, wsReqLog)

	conn, respHS, errDial := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
	if errDial != nil {
		bodyErr := websocketHandshakeBody(respHS)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if shouldFallbackCodexWebsocketHandshake(ctx, respHS) {
			return e.CodexExecutor.Execute(ctx, auth, req, opts)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			return resp, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		return resp, errDial
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()
	if sess == nil {
		logCodexWebsocketConnected(executionSessionID, authID, wsURL)
		defer func() {
			reason := "completed"
			if err != nil {
				reason = "error"
			}
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, reason, err)
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}()
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = make(chan codexWebsocketRead, 4096)
		sess.setActive(readCh)
		defer sess.clearActive(readCh)
	}

	if errSend := writeCodexWebsocketMessage(sess, conn, wsReqBody); errSend != nil {
		if sess != nil {
			e.invalidateUpstreamConn(sess, conn, "send_error", errSend)

			// Retry once with a fresh websocket connection. This is mainly to handle
			// upstream closing the socket between sequential requests within the same
			// execution session.
			connRetry, respHSRetry, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
			if errDialRetry == nil && connRetry != nil {
				wsReqBodyRetry := buildCodexWebsocketRequestBody(upstreamBody)
				helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
					URL:       wsURL,
					Method:    "WEBSOCKET",
					Headers:   wsHeaders.Clone(),
					Body:      wsReqBodyRetry,
					Provider:  e.Identifier(),
					AuthID:    authID,
					AuthLabel: authLabel,
					AuthType:  authType,
					AuthValue: authValue,
				})
				recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
				reporter.StartResponseTTFT()
				if errSendRetry := writeCodexWebsocketMessage(sess, connRetry, wsReqBodyRetry); errSendRetry == nil {
					conn = connRetry
					wsReqBody = wsReqBodyRetry
				} else {
					e.invalidateUpstreamConn(sess, connRetry, "send_error", errSendRetry)
					helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
					return resp, errSendRetry
				}
			} else {
				closeHTTPResponseBody(respHSRetry, "codex websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				return resp, errDialRetry
			}
		} else {
			helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
			return resp, errSend
		}
	}

	for {
		if ctx != nil && ctx.Err() != nil {
			return resp, ctx.Err()
		}
		msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
		if errRead != nil {
			mappedErr := mapCodexWebsocketReadError(errRead)
			helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
			return resp, mappedErr
		}
		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				err = fmt.Errorf("codex websockets executor: unexpected binary message")
				if sess != nil {
					e.invalidateUpstreamConn(sess, conn, "unexpected_binary", err)
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", err)
				return resp, err
			}
			continue
		}

		payload = bytes.TrimSpace(payload)
		if len(payload) == 0 {
			continue
		}
		reporter.MarkFirstResponseByte()
		payload = applyCodexIdentityConfuseResponsePayload(payload, identityState)
		helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)

		if wsErr, ok := parseCodexWebsocketError(payload); ok {
			if sess != nil {
				e.invalidateUpstreamConn(sess, conn, "upstream_error", wsErr)
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
			return resp, wsErr
		}

		payload = normalizeCodexWebsocketCompletion(payload)
		eventType := gjson.GetBytes(payload, "type").String()
		if eventType == "response.completed" {
			if detail, ok := helps.ParseCodexUsage(payload); ok {
				reporter.Publish(ctx, detail)
			}
			var param any
			clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
			out := sdktranslator.TranslateNonStream(ctx, to, responseFormat, req.Model, originalPayload, clientBody, clientPayload, &param)
			resp = cliproxyexecutor.Response{Payload: out}
			return resp, nil
		}
	}
}

func (e *CodexWebsocketsExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	log.Debugf("Executing Codex Websockets stream request with auth ID: %s, model: %s", auth.ID, req.Model)
	if ctx == nil {
		ctx = context.Background()
	}
	traceStartedAt := time.Now()
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("codex")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	originalTranslated, body := translateCodexRequestPair(from, to, baseModel, originalPayload, req.Payload, true)

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", body, originalTranslated, requestedModel, requestPath, opts.Headers)
	body, _ = sjson.SetBytes(body, "model", baseModel)
	body, _ = sjson.SetBytes(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body, _ = sjson.DeleteBytes(body, "stream_options")
	body = normalizeCodexInstructions(body)
	if e.cfg == nil || e.cfg.DisableImageGeneration == config.DisableImageGenerationOff {
		body = ensureImageGenerationTool(body, baseModel, auth, opts.Headers)
	}
	body = sanitizeOpenAIResponsesReasoningEncryptedContent(ctx, "codex websockets executor", body)
	body = normalizeCodexParallelToolCallsForTools(body)
	body, replayScope, errReplay := applyCodexReasoningReplayCacheRequired(ctx, from, req, opts, body)
	if errReplay != nil {
		return nil, errReplay
	}

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return nil, err
	}

	body, wsHeaders, errPromptCache := applyCodexPromptCacheHeadersWithContext(ctx, from, req, body, auth.ID)
	if errPromptCache != nil {
		return nil, errPromptCache
	}
	body = canonicalizeCodexCacheableRequest(body)
	clientBody := body
	reporter.SetTranslatedReasoningEffort(clientBody, to.String())
	wsHeaders = applyCodexWebsocketHeaders(ctx, wsHeaders, auth, apiKey, e.cfg)
	applyModelHeaderOverrides(wsHeaders, baseModel)
	retryWSHeaders := wsHeaders.Clone()

	var authID, authLabel, authType, authValue string
	authID = auth.ID
	authLabel = auth.Label
	authType, authValue = auth.AccountInfo()

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	sessionOverflow := false
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		if sess != nil {
			if from.String() == "claude" && !sess.reqMu.TryLock() {
				sessionOverflow = true
				helps.RecordAPIWebsocketMetric(ctx, e.cfg, "session_busy", map[string]any{
					"session_id": executionSessionID,
					"overflow":   true,
				})
				if sess.codexHTTPFallback() {
					return e.CodexExecutor.ExecuteStream(ctx, auth, req, opts)
				}
				sess = nil
			} else {
				lockWait := time.Duration(0)
				sessionBusy := false
				if from.String() != "claude" {
					lockWait, sessionBusy = sess.lockRequest()
				}
				helps.RecordAPIWebsocketMetric(ctx, e.cfg, "session_lock_acquired", map[string]any{
					"session_id": executionSessionID,
					"wait_us":    lockWait.Microseconds(),
					"busy":       sessionBusy,
				})
				if sess.codexHTTPFallback() {
					sess.reqMu.Unlock()
					return e.CodexExecutor.ExecuteStream(ctx, auth, req, opts)
				}
			}
		}
	}
	requestBody := clientBody
	incrementalObservation := codexIncrementalObservation{resetReason: "not_applicable"}
	if sess != nil && from.String() == "claude" {
		requestBody, incrementalObservation = sess.prepareCodexIncrementalRequestObserved(clientBody)
	}
	cacheMetricFields := codexPromptCacheMetricFields(requestBody)
	chainSource := "full_replay"
	if incrementalObservation.incremental {
		chainSource = "incremental"
	}
	helps.RecordAPIWebsocketMetric(ctx, e.cfg, "request_prepared", map[string]any{
		"session_id":                executionSessionID,
		"model":                     baseModel,
		"source_format":             from.String(),
		"elapsed_us":                time.Since(traceStartedAt).Microseconds(),
		"client_body_bytes":         len(clientBody),
		"upstream_body_bytes":       len(requestBody),
		"input_items":               gjson.GetBytes(requestBody, "input.#").Int(),
		"incremental":               incrementalObservation.incremental,
		"incremental_reset_reason":  incrementalObservation.resetReason,
		"chain_source":              chainSource,
		"fresh_response_chain":      !incrementalObservation.incremental,
		"overflow":                  sessionOverflow,
		"has_previous_response":     strings.TrimSpace(gjson.GetBytes(requestBody, "previous_response_id").String()) != "",
		"prompt_cache_scope":        cacheMetricFields["prompt_cache_scope"],
		"prompt_prefix_fingerprint": cacheMetricFields["prompt_prefix_fingerprint"],
		"instructions_bytes":        cacheMetricFields["instructions_bytes"],
		"tools_count":               cacheMetricFields["tools_count"],
	})
	var identityState codexIdentityConfuseState
	upstreamBody, identityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, requestBody)
	applyCodexIdentityConfuseHeaders(wsHeaders, &identityState)

	wsReqBody := buildCodexWebsocketRequestBody(upstreamBody)
	wsReqLog := helps.UpstreamRequestLog{
		URL:       wsURL,
		Method:    "WEBSOCKET",
		Headers:   wsHeaders.Clone(),
		Body:      wsReqBody,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	}
	helps.RecordAPIWebsocketRequest(ctx, e.cfg, wsReqLog)

	connectStartedAt := time.Now()
	var conn *websocket.Conn
	var respHS *http.Response
	var connectionSource codexWebsocketConnectionSource
	var overflowBaseSource codexWebsocketConnectionSource
	var errDial error
	if sessionOverflow {
		conn, respHS, overflowBaseSource, errDial = e.ensureOverflowConnObserved(ctx, auth, authID, wsURL, wsHeaders, executionSessionID)
		connectionSource = codexWebsocketConnectionOverflow
	} else {
		conn, respHS, connectionSource, errDial = e.ensureUpstreamConnObserved(ctx, auth, sess, authID, wsURL, wsHeaders)
	}
	connectionAge, connectionRequestCount := time.Duration(0), int64(1)
	if errDial == nil && conn != nil && sess != nil {
		connectionAge, connectionRequestCount = sess.observeConnectionUse(conn)
	}
	helps.RecordAPIWebsocketMetric(ctx, e.cfg, "connection_ready", map[string]any{
		"session_id":               executionSessionID,
		"duration_us":              time.Since(connectStartedAt).Microseconds(),
		"connection_age_us":        connectionAge.Microseconds(),
		"connection_request_count": connectionRequestCount,
		"connection_source":        connectionSource,
		"overflow_base_source":     overflowBaseSource,
		"reused":                   connectionSource == codexWebsocketConnectionSessionReuse || connectionSource == codexWebsocketConnectionSpeculative,
		"success":                  errDial == nil,
	})
	var upstreamHeaders http.Header
	if respHS != nil {
		upstreamHeaders = respHS.Header.Clone()
	}
	if errDial != nil {
		bodyErr := websocketHandshakeBody(respHS)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if shouldFallbackCodexWebsocketHandshake(ctx, respHS) {
			if sess != nil {
				if shouldPersistCodexWebsocketFallback(respHS) {
					sess.activateCodexHTTPFallback()
				}
				sess.reqMu.Unlock()
			}
			return e.CodexExecutor.ExecuteStream(ctx, auth, req, opts)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			return nil, statusErr{code: respHS.StatusCode, msg: string(bodyErr)}
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		if sess != nil {
			sess.reqMu.Unlock()
		}
		return nil, errDial
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()

	if sess == nil {
		logCodexWebsocketConnected(executionSessionID, authID, wsURL)
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = make(chan codexWebsocketRead, 4096)
		sess.setActive(readCh)
	}

	transportRetries := 0
	sendStartedAt := time.Now()
	errSend := writeCodexWebsocketMessage(sess, conn, wsReqBody)
	helps.RecordAPIWebsocketMetric(ctx, e.cfg, "request_sent", map[string]any{
		"session_id":  executionSessionID,
		"duration_us": time.Since(sendStartedAt).Microseconds(),
		"elapsed_us":  time.Since(traceStartedAt).Microseconds(),
		"bytes":       len(wsReqBody),
		"success":     errSend == nil,
	})
	if errSend != nil {
		helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
		if sess != nil {
			e.invalidateUpstreamConn(sess, conn, "send_error", errSend)
			sess.clearActive(readCh)
			readCh = make(chan codexWebsocketRead, 4096)
			sess.setActive(readCh)

			// Retry once with a new websocket connection for the same execution session.
			transportRetries++
			fullUpstreamBody, retryIdentityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, clientBody)
			retryHeaders := retryWSHeaders.Clone()
			applyCodexIdentityConfuseHeaders(retryHeaders, &retryIdentityState)
			connRetry, respHSRetry, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, retryHeaders)
			if errDialRetry != nil || connRetry == nil {
				closeHTTPResponseBody(respHSRetry, "codex websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				sess.clearActive(readCh)
				sess.reqMu.Unlock()
				return nil, errDialRetry
			}
			identityState = retryIdentityState
			wsReqBodyRetry := buildCodexWebsocketRequestBody(fullUpstreamBody)
			helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
				URL:       wsURL,
				Method:    "WEBSOCKET",
				Headers:   retryHeaders,
				Body:      wsReqBodyRetry,
				Provider:  e.Identifier(),
				AuthID:    authID,
				AuthLabel: authLabel,
				AuthType:  authType,
				AuthValue: authValue,
			})
			recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
			reporter.StartResponseTTFT()
			if errSendRetry := writeCodexWebsocketMessage(sess, connRetry, wsReqBodyRetry); errSendRetry != nil {
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				e.invalidateUpstreamConn(sess, connRetry, "send_error", errSendRetry)
				sess.clearActive(readCh)
				sess.reqMu.Unlock()
				return nil, errSendRetry
			}
			helps.RecordAPIWebsocketMetric(ctx, e.cfg, "request_retry_sent", map[string]any{
				"session_id":        executionSessionID,
				"connection_source": "retry",
				"elapsed_us":        time.Since(traceStartedAt).Microseconds(),
				"bytes":             len(wsReqBodyRetry),
			})
			conn = connRetry
			wsReqBody = wsReqBodyRetry
		} else {
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, "send_error", errSend)
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
			return nil, errSend
		}
	}

	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		terminateReason := "completed"
		var terminateErr error
		var firstEventAt time.Time
		var firstReasoningDeltaAt time.Time
		var firstOutputTextDeltaAt time.Time
		var upstreamFrames int64
		var upstreamBytes int64
		var translatedChunks int64
		var translationDuration time.Duration
		var downstreamBlockedDuration time.Duration
		downstreamCommitted := false
		closeCode := 0
		var semanticState codexWebsocketSemanticState
		speculativeAgentCalls := make(map[string]struct{})

		defer close(out)
		defer func() {
			if sess != nil {
				connectionAge, connectionRequestCount = sess.connectionObservation(conn)
			}
			incompleteToolCalls := semanticState.incompleteToolCalls()
			helps.RecordAPIWebsocketMetric(ctx, e.cfg, "request_finished", map[string]any{
				"session_id":                 executionSessionID,
				"model":                      baseModel,
				"connection_source":          connectionSource,
				"reason":                     terminateReason,
				"close_code":                 closeCode,
				"last_event_type":            semanticState.lastEventType,
				"tool_call_started":          semanticState.toolCallsStarted > 0,
				"tool_call_completed":        semanticState.toolCallsStarted > 0 && incompleteToolCalls == 0,
				"tool_call_in_progress":      incompleteToolCalls > 0,
				"tool_calls_started":         semanticState.toolCallsStarted,
				"tool_calls_completed":       semanticState.toolCallsComplete,
				"tool_calls_incomplete":      incompleteToolCalls,
				"connection_age_us":          connectionAge.Microseconds(),
				"connection_request_count":   connectionRequestCount,
				"elapsed_us":                 time.Since(traceStartedAt).Microseconds(),
				"first_event_us":             elapsedSinceOrZero(traceStartedAt, firstEventAt).Microseconds(),
				"first_reasoning_delta_us":   elapsedSinceOrZero(traceStartedAt, firstReasoningDeltaAt).Microseconds(),
				"first_output_text_delta_us": elapsedSinceOrZero(traceStartedAt, firstOutputTextDeltaAt).Microseconds(),
				"upstream_frames":            upstreamFrames,
				"upstream_bytes":             upstreamBytes,
				"translated_chunks":          translatedChunks,
				"downstream_committed":       downstreamCommitted,
				"transport_retries":          transportRetries,
				"translation_us":             translationDuration.Microseconds(),
				"downstream_blocked_us":      downstreamBlockedDuration.Microseconds(),
			})
			if sess != nil {
				sess.clearActive(readCh)
				if terminateReason == "context_done" {
					e.invalidateUpstreamConn(sess, conn, terminateReason, terminateErr)
				}
				sess.reqMu.Unlock()
				return
			}
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, terminateReason, terminateErr)
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}()

		send := func(chunk cliproxyexecutor.StreamChunk) bool {
			blockedAt := time.Now()
			defer func() { downstreamBlockedDuration += time.Since(blockedAt) }()
			if ctx == nil {
				out <- chunk
				if len(chunk.Payload) > 0 {
					downstreamCommitted = true
				}
				return true
			}
			select {
			case out <- chunk:
				if len(chunk.Payload) > 0 {
					downstreamCommitted = true
				}
				return true
			case <-ctx.Done():
				return false
			}
		}

		var param any
		for {
			if ctx != nil && ctx.Err() != nil {
				terminateReason = "context_done"
				terminateErr = ctx.Err()
				_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
				return
			}
			msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
			if errRead != nil {
				if sess != nil && ctx != nil && ctx.Err() != nil {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
					return
				}
				mappedErr := mapCodexWebsocketReadError(errRead)
				closeCode = codexWebsocketCloseCode(mappedErr)
				retryable := shouldRetryCodexWebsocketReadError(mappedErr)
				if !downstreamCommitted && transportRetries == 0 && retryable {
					transportRetries++
					helpFields := map[string]any{
						"session_id":           executionSessionID,
						"attempt":              transportRetries,
						"boundary":             "pre_output",
						"reason":               codexWebsocketRetryReason(mappedErr),
						"downstream_committed": false,
					}
					helps.RecordAPIWebsocketMetric(ctx, e.cfg, "transport_retry_attempted", helpFields)

					if sess != nil {
						sess.clearActive(readCh)
						readCh = make(chan codexWebsocketRead, 4096)
						sess.setActive(readCh)
					} else if conn != nil {
						if errClose := conn.Close(); errClose != nil {
							log.Errorf("codex websockets executor: close websocket before retry error: %v", errClose)
						}
					}

					fullUpstreamBody, retryIdentityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, clientBody)
					retryHeaders := retryWSHeaders.Clone()
					applyCodexIdentityConfuseHeaders(retryHeaders, &retryIdentityState)
					wsReqBodyRetry := buildCodexWebsocketRequestBody(fullUpstreamBody)

					var connRetry *websocket.Conn
					var respHSRetry *http.Response
					var retrySource codexWebsocketConnectionSource
					var errDialRetry error
					retryStartedAt := time.Now()
					if sessionOverflow {
						connRetry, respHSRetry, _, errDialRetry = e.ensureOverflowConnObserved(ctx, auth, authID, wsURL, retryHeaders, executionSessionID)
						retrySource = codexWebsocketConnectionOverflow
					} else {
						connRetry, respHSRetry, retrySource, errDialRetry = e.ensureUpstreamConnObserved(ctx, auth, sess, authID, wsURL, retryHeaders)
					}
					if errDialRetry == nil && connRetry != nil {
						recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
						helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
							URL:       wsURL,
							Method:    "WEBSOCKET",
							Headers:   retryHeaders.Clone(),
							Body:      wsReqBodyRetry,
							Provider:  e.Identifier(),
							AuthID:    authID,
							AuthLabel: authLabel,
							AuthType:  authType,
							AuthValue: authValue,
						})
						errSendRetry := writeCodexWebsocketMessage(sess, connRetry, wsReqBodyRetry)
						if errSendRetry == nil {
							conn = connRetry
							connectionSource = retrySource
							if sess != nil {
								connectionAge, connectionRequestCount = sess.observeConnectionUse(connRetry)
							} else {
								connectionAge, connectionRequestCount = 0, 1
							}
							wsReqBody = wsReqBodyRetry
							identityState = retryIdentityState
							sendStartedAt = time.Now()
							param = nil
							closeCode = 0
							semanticState = codexWebsocketSemanticState{}
							speculativeAgentCalls = make(map[string]struct{})
							helpFields["duration_us"] = time.Since(retryStartedAt).Microseconds()
							helpFields["connection_source"] = retrySource
							helps.RecordAPIWebsocketMetric(ctx, e.cfg, "transport_retry_succeeded", helpFields)
							continue
						}
						errDialRetry = errSendRetry
						if sess != nil {
							e.invalidateUpstreamConn(sess, connRetry, "retry_send_error", errSendRetry)
						} else if errClose := connRetry.Close(); errClose != nil {
							log.Errorf("codex websockets executor: close failed retry websocket error: %v", errClose)
						}
					}
					closeHTTPResponseBody(respHSRetry, "codex websockets executor: close retry handshake response body error")
					if sess != nil {
						sess.clearActive(readCh)
					}
					if errDialRetry != nil {
						mappedErr = errDialRetry
					}
					helpFields["duration_us"] = time.Since(retryStartedAt).Microseconds()
					helpFields["connection_source"] = retrySource
					helps.RecordAPIWebsocketMetric(ctx, e.cfg, "transport_retry_exhausted", helpFields)
				} else {
					suppressionReason := "retry_exhausted"
					if downstreamCommitted {
						suppressionReason = "downstream_committed"
					} else if !retryable {
						suppressionReason = "non_retriable"
					}
					helps.RecordAPIWebsocketMetric(ctx, e.cfg, "transport_retry_suppressed", map[string]any{
						"session_id":           executionSessionID,
						"attempt":              transportRetries,
						"boundary":             codexWebsocketRetryBoundary(downstreamCommitted),
						"reason":               codexWebsocketRetryReason(mappedErr),
						"suppression_reason":   suppressionReason,
						"downstream_committed": downstreamCommitted,
					})
				}
				terminateReason = "read_error"
				terminateErr = mappedErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
				reporter.PublishFailure(ctx, mappedErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: mappedErr})
				return
			}
			if msgType != websocket.TextMessage {
				if msgType == websocket.BinaryMessage {
					err = fmt.Errorf("codex websockets executor: unexpected binary message")
					terminateReason = "unexpected_binary"
					terminateErr = err
					helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", err)
					reporter.PublishFailure(ctx, err)
					if sess != nil {
						e.invalidateUpstreamConn(sess, conn, "unexpected_binary", err)
					}
					_ = send(cliproxyexecutor.StreamChunk{Err: err})
					return
				}
				continue
			}

			payload = bytes.TrimSpace(payload)
			if len(payload) == 0 {
				continue
			}
			reporter.MarkFirstResponseByte()
			upstreamFrames++
			upstreamBytes += int64(len(payload))
			if firstEventAt.IsZero() {
				firstEventAt = time.Now()
				helps.RecordAPIWebsocketMetric(ctx, e.cfg, "first_upstream_event", map[string]any{
					"session_id":    executionSessionID,
					"elapsed_us":    firstEventAt.Sub(traceStartedAt).Microseconds(),
					"since_send_us": firstEventAt.Sub(sendStartedAt).Microseconds(),
					"bytes":         len(payload),
				})
			}
			payload = applyCodexIdentityConfuseResponsePayload(payload, identityState)
			helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)
			semanticState.observe(payload)

			if wsErr, ok := parseCodexWebsocketError(payload); ok {
				terminateReason = "upstream_error"
				terminateErr = wsErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
				reporter.PublishFailure(ctx, wsErr)
				if sess != nil {
					e.invalidateUpstreamConn(sess, conn, "upstream_error", wsErr)
				}
				_ = send(cliproxyexecutor.StreamChunk{Err: wsErr})
				return
			}

			eventType := gjson.GetBytes(payload, "type").String()
			if from.String() == "claude" && strings.HasPrefix(executionSessionID, helps.ClaudeCodeWebsocketSessionPrefix) {
				if agentCallKey, ok := codexAgentToolCallKey(payload); ok {
					if _, seen := speculativeAgentCalls[agentCallKey]; !seen {
						speculativeAgentCalls[agentCallKey] = struct{}{}
						e.scheduleSpeculativePreconnect(ctx, auth, authID, wsURL, wsHeaders, executionSessionID, upstreamBody)
					}
				}
			}
			if eventType == "response.reasoning_summary_text.delta" && firstReasoningDeltaAt.IsZero() {
				firstReasoningDeltaAt = time.Now()
				helps.RecordAPIWebsocketMetric(ctx, e.cfg, "first_reasoning_delta", map[string]any{
					"session_id":    executionSessionID,
					"elapsed_us":    firstReasoningDeltaAt.Sub(traceStartedAt).Microseconds(),
					"since_send_us": firstReasoningDeltaAt.Sub(sendStartedAt).Microseconds(),
				})
			}
			if eventType == "response.output_text.delta" && firstOutputTextDeltaAt.IsZero() {
				firstOutputTextDeltaAt = time.Now()
				helps.RecordAPIWebsocketMetric(ctx, e.cfg, "first_output_text_delta", map[string]any{
					"session_id":    executionSessionID,
					"elapsed_us":    firstOutputTextDeltaAt.Sub(traceStartedAt).Microseconds(),
					"since_send_us": firstOutputTextDeltaAt.Sub(sendStartedAt).Microseconds(),
				})
			}
			isTerminalEvent := eventType == "response.completed" || eventType == "response.done" || eventType == "error"
			clientPayload := applyCodexIdentityExposeResponsePayload(payload, identityState)
			if cliproxyexecutor.DownstreamWebsocket(ctx) {
				if eventType == "response.completed" || eventType == "response.done" {
					if detail, ok := helps.ParseCodexUsage(payload); ok {
						recordCodexWebsocketUsageMetric(ctx, e.cfg, executionSessionID, detail)
						reporter.Publish(ctx, detail)
					}
				}
				if !send(cliproxyexecutor.StreamChunk{Payload: clientPayload}) {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					return
				}
				if isTerminalEvent {
					return
				}
				continue
			}

			payload = normalizeCodexWebsocketCompletion(payload)
			eventType = gjson.GetBytes(payload, "type").String()
			if eventType == "response.completed" || eventType == "response.done" {
				if detail, ok := helps.ParseCodexUsage(payload); ok {
					recordCodexWebsocketUsageMetric(ctx, e.cfg, executionSessionID, detail)
					reporter.Publish(ctx, detail)
				}
				if eventType == "response.completed" {
					cacheCodexReasoningReplayFromCompleted(replayScope, payload)
				}
			}

			clientPayload = applyCodexIdentityExposeResponsePayload(payload, identityState)
			if sess != nil && (eventType == "response.completed" || eventType == "response.done") {
				sess.completeCodexIncrementalRequest(clientPayload)
			}
			translateStartedAt := time.Now()
			line := encodeCodexWebsocketAsSSE(clientPayload)
			chunks := sdktranslator.TranslateStream(ctx, to, responseFormat, req.Model, originalPayload, clientBody, line, &param)
			translationDuration += time.Since(translateStartedAt)
			translatedChunks += int64(len(chunks))
			for i := range chunks {
				if !send(cliproxyexecutor.StreamChunk{Payload: chunks[i]}) {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					return
				}
			}
			if eventType == "response.completed" || eventType == "response.done" {
				return
			}
		}
	}()

	return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
}

func (e *CodexWebsocketsExecutor) dialCodexWebsocket(ctx context.Context, auth *cliproxyauth.Auth, wsURL string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	dialer := newProxyAwareWebsocketDialer(e.cfg, auth)
	dialer.HandshakeTimeout = codexResponsesWebsocketHandshakeTO
	dialer.EnableCompression = true
	if ctx == nil {
		ctx = context.Background()
	}
	conn, resp, err := dialer.DialContext(observability.WithNetworkTrace(ctx), wsURL, headers)
	if conn != nil {
		// Avoid gorilla/websocket flate tail validation issues on some upstreams/Go versions.
		// Negotiating permessage-deflate is fine; we just don't compress outbound messages.
		conn.EnableWriteCompression(false)
	}
	return conn, resp, err
}

func writeCodexWebsocketMessage(sess *codexWebsocketSession, conn *websocket.Conn, payload []byte) error {
	if sess != nil {
		return sess.writeMessage(conn, websocket.TextMessage, payload)
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func mapCodexWebsocketReadError(err error) error {
	if err == nil {
		return nil
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseMessageTooBig {
		return statusErr{code: http.StatusRequestEntityTooLarge, msg: `{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}`}
	}
	return err
}

func shouldRetryCodexWebsocketReadError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status interface{ StatusCode() int }
	if errors.As(err, &status) && status.StatusCode() != 0 {
		return false
	}
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || isTemporaryCodexWebsocketNetworkError(err)
	}
	switch closeErr.Code {
	case websocket.CloseAbnormalClosure, websocket.CloseGoingAway, websocket.CloseServiceRestart, websocket.CloseTryAgainLater:
		return true
	default:
		return false
	}
}

func isTemporaryCodexWebsocketNetworkError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary())
}

func codexWebsocketRetryReason(err error) string {
	if err == nil {
		return "unknown"
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return "close_" + strconv.Itoa(closeErr.Code)
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, net.ErrClosed) {
		return "connection_closed"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "read_error"
}

func codexWebsocketCloseCode(err error) int {
	if err == nil {
		return 0
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return closeErr.Code
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return websocket.CloseAbnormalClosure
	}
	return 0
}

type codexWebsocketSemanticState struct {
	lastEventType     string
	toolCallsStarted  int64
	toolCallsComplete int64
}

func (s *codexWebsocketSemanticState) observe(payload []byte) {
	if s == nil {
		return
	}
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType == "" {
		return
	}
	s.lastEventType = eventType
	itemType := strings.TrimSpace(gjson.GetBytes(payload, "item.type").String())
	if itemType != "function_call" && itemType != "custom_tool_call" {
		return
	}
	switch eventType {
	case "response.output_item.added":
		s.toolCallsStarted++
	case "response.output_item.done":
		s.toolCallsComplete++
	}
}

func (s codexWebsocketSemanticState) incompleteToolCalls() int64 {
	incomplete := s.toolCallsStarted - s.toolCallsComplete
	if incomplete < 0 {
		return 0
	}
	return incomplete
}

func codexWebsocketRetryBoundary(downstreamCommitted bool) string {
	if downstreamCommitted {
		return "post_output"
	}
	return "pre_output"
}

func buildCodexWebsocketRequestBody(body []byte) []byte {
	if len(body) == 0 {
		return nil
	}

	// Match codex-rs websocket v2 semantics: every request is `response.create`.
	// Incremental follow-up turns continue on the same websocket using
	// `previous_response_id` + incremental `input`, not `response.append`.
	wsReqBody, errSet := sjson.SetBytes(bytes.Clone(body), "type", "response.create")
	if errSet == nil && len(wsReqBody) > 0 {
		return wsReqBody
	}
	fallback := bytes.Clone(body)
	fallback, _ = sjson.SetBytes(fallback, "type", "response.create")
	return fallback
}

func codexPromptCacheMetricFields(body []byte) map[string]any {
	instructionsBytes := len(gjson.GetBytes(body, "instructions").String())
	stableInputPrefix := make([]json.RawMessage, 0, 2)
	gjson.GetBytes(body, "input").ForEach(func(_, item gjson.Result) bool {
		role := strings.TrimSpace(item.Get("role").String())
		if role != "developer" && role != "system" {
			return false
		}
		stableInputPrefix = append(stableInputPrefix, json.RawMessage(item.Raw))
		item.Get("content").ForEach(func(_, content gjson.Result) bool {
			instructionsBytes += len(content.Get("text").String())
			return true
		})
		return true
	})
	fields := map[string]any{
		"prompt_cache_scope":        "",
		"prompt_prefix_fingerprint": "",
		"instructions_bytes":        instructionsBytes,
		"tools_count":               gjson.GetBytes(body, "tools.#").Int(),
	}
	cacheKey := strings.TrimSpace(gjson.GetBytes(body, "prompt_cache_key").String())
	if cacheKey == "" {
		return fields
	}
	fields["prompt_cache_scope"] = "codex-cache:" + uuid.NewSHA1(uuid.NameSpaceOID, []byte("cache-scope\x00"+cacheKey)).String()
	var prefix strings.Builder
	for _, path := range []string{"model", "instructions", "tools", "tool_choice", "parallel_tool_calls", "reasoning", "include", "text", "service_tier"} {
		value := gjson.GetBytes(body, path)
		prefix.WriteString(path)
		prefix.WriteByte(0)
		prefix.Write(canonicalCodexPrefixJSON(path, json.RawMessage(value.Raw)))
		prefix.WriteByte(0)
	}
	for _, item := range stableInputPrefix {
		prefix.WriteString("input_prefix")
		prefix.WriteByte(0)
		prefix.Write(canonicalCodexPrefixJSON("input_prefix", item))
		prefix.WriteByte(0)
	}
	fields["prompt_prefix_fingerprint"] = "codex-prefix:" + uuid.NewSHA1(uuid.NameSpaceOID, []byte(prefix.String())).String()
	return fields
}

func canonicalCodexPrefixJSON(path string, raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return nil
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return raw
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return canonical
}

// canonicalizeCodexCacheableRequest makes semantically equivalent translated
// requests byte-stable so a deterministic prompt_cache_key can reuse the same
// upstream prefix after a client or proxy process restart.
func canonicalizeCodexCacheableRequest(body []byte) []byte {
	if len(body) == 0 {
		return body
	}
	var request map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		return body
	}
	canonical, err := json.Marshal(request)
	if err != nil {
		return body
	}
	return canonical
}

type codexGenerateFalseWarmupResult struct {
	responseID   string
	inputTokens  int64
	outputTokens int64
}

func performCodexGenerateFalseWarmup(ctx context.Context, conn *websocket.Conn, request []byte) (codexGenerateFalseWarmupResult, error) {
	var result codexGenerateFalseWarmupResult
	if conn == nil {
		return result, fmt.Errorf("codex websocket warmup: websocket conn is nil")
	}
	if errContext := ctx.Err(); errContext != nil {
		return result, errContext
	}
	deadline := time.Now().Add(codexResponsesWebsocketIdleTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	_ = conn.SetWriteDeadline(deadline)
	if errWrite := conn.WriteMessage(websocket.TextMessage, request); errWrite != nil {
		return result, fmt.Errorf("codex websocket warmup: write request: %w", errWrite)
	}
	_ = conn.SetWriteDeadline(time.Time{})
	_ = conn.SetReadDeadline(deadline)
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	for {
		msgType, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			if errContext := ctx.Err(); errContext != nil {
				return result, errContext
			}
			return result, fmt.Errorf("codex websocket warmup: read response: %w", errRead)
		}
		if msgType != websocket.TextMessage {
			continue
		}
		if wsErr, ok := parseCodexWebsocketError(payload); ok {
			return result, wsErr
		}
		eventType := gjson.GetBytes(payload, "type").String()
		if eventType != "response.completed" && eventType != "response.done" {
			continue
		}
		result.responseID = strings.TrimSpace(gjson.GetBytes(payload, "response.id").String())
		result.inputTokens = gjson.GetBytes(payload, "response.usage.input_tokens").Int()
		result.outputTokens = gjson.GetBytes(payload, "response.usage.output_tokens").Int()
		return result, nil
	}
}

func codexGenerateFalseWarmupFailureReason(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	var status interface{ StatusCode() int }
	if errors.As(err, &status) {
		if status.StatusCode() == http.StatusTooManyRequests {
			return "rate_limited"
		}
		return "upstream_error"
	}
	return "transport_error"
}

func readCodexWebsocketMessage(ctx context.Context, sess *codexWebsocketSession, conn *websocket.Conn, readCh chan codexWebsocketRead) (int, []byte, error) {
	if sess == nil {
		if conn == nil {
			return 0, nil, fmt.Errorf("codex websockets executor: websocket conn is nil")
		}
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
		msgType, payload, errRead := conn.ReadMessage()
		return msgType, payload, errRead
	}
	if conn == nil {
		return 0, nil, fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	if readCh == nil {
		return 0, nil, fmt.Errorf("codex websockets executor: session read channel is nil")
	}
	for {
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case ev, ok := <-readCh:
			if !ok {
				return 0, nil, fmt.Errorf("codex websockets executor: session read channel closed")
			}
			if ev.conn != conn {
				continue
			}
			if ev.err != nil {
				return 0, nil, ev.err
			}
			return ev.msgType, ev.payload, nil
		}
	}
}

func newProxyAwareWebsocketDialer(cfg *config.Config, auth *cliproxyauth.Auth) *websocket.Dialer {
	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  codexResponsesWebsocketHandshakeTO,
		EnableCompression: true,
		NetDialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	proxyURL := ""
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}
	if proxyURL == "" {
		return dialer
	}

	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		log.Errorf("codex websockets executor: %v", errParse)
		return dialer
	}

	switch setting.Mode {
	case proxyutil.ModeDirect:
		dialer.Proxy = nil
		return dialer
	case proxyutil.ModeProxy:
	default:
		return dialer
	}

	switch setting.URL.Scheme {
	case "socks5", "socks5h":
		var proxyAuth *proxy.Auth
		if setting.URL.User != nil {
			username := setting.URL.User.Username()
			password, _ := setting.URL.User.Password()
			proxyAuth = &proxy.Auth{User: username, Password: password}
		}
		socksDialer, errSOCKS5 := proxy.SOCKS5("tcp", setting.URL.Host, proxyAuth, proxy.Direct)
		if errSOCKS5 != nil {
			log.Errorf("codex websockets executor: create SOCKS5 dialer failed: %v", errSOCKS5)
			return dialer
		}
		dialer.Proxy = nil
		dialer.NetDialContext = func(_ context.Context, network, addr string) (net.Conn, error) {
			return socksDialer.Dial(network, addr)
		}
	case "http", "https":
		dialer.Proxy = http.ProxyURL(setting.URL)
	default:
		log.Errorf("codex websockets executor: unsupported proxy scheme: %s", setting.URL.Scheme)
	}

	return dialer
}

func buildCodexResponsesWebsocketURL(httpURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(httpURL))
	if err != nil {
		return "", err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", fmt.Errorf("codex websockets executor: unsupported responses websocket URL scheme %q", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("codex websockets executor: responses websocket URL host is empty")
	}
	return parsed.String(), nil
}

func applyCodexPromptCacheHeaders(from sdktranslator.Format, req cliproxyexecutor.Request, rawJSON []byte) ([]byte, http.Header) {
	body, headers, _ := applyCodexPromptCacheHeadersWithContext(context.Background(), from, req, rawJSON, "")
	return body, headers
}

func applyCodexPromptCacheHeadersWithContext(ctx context.Context, from sdktranslator.Format, req cliproxyexecutor.Request, rawJSON []byte, authID string) ([]byte, http.Header, error) {
	headers := http.Header{}
	if len(rawJSON) == 0 {
		return rawJSON, headers, nil
	}

	var cache helps.CodexCache
	if sourceFormatEqual(from, sdktranslator.FormatClaude) {
		cached, ok, errCache := helps.ClaudeCodePromptCacheForAuth(ctx, "codex", req.Model, authID, req.Payload, nil)
		if errCache != nil {
			return nil, nil, errCache
		}
		if ok {
			cache = cached
		}
	} else if sourceFormatEqual(from, sdktranslator.FormatOpenAIResponse) {
		if promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key"); promptCacheKey.Exists() {
			cache.ID = promptCacheKey.String()
		}
	}

	if cache.ID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "prompt_cache_key", cache.ID)
		setHeaderCasePreserved(headers, "session_id", cache.ID)
		headers.Set("Conversation_id", cache.ID)
	}

	return rawJSON, headers, nil
}

func applyCodexWebsocketHeaders(ctx context.Context, headers http.Header, auth *cliproxyauth.Auth, token string, cfg *config.Config) http.Header {
	if headers == nil {
		headers = http.Header{}
	}
	if strings.TrimSpace(token) != "" {
		headers.Set("Authorization", "Bearer "+token)
	}

	var ginHeaders http.Header
	if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header.Clone()
	}

	isAPIKey := codexAuthUsesAPIKey(auth)
	cfgUserAgent, cfgBetaFeatures := codexHeaderDefaults(cfg, auth)
	ensureHeaderWithPriority(headers, ginHeaders, "x-codex-beta-features", cfgBetaFeatures, "")
	misc.EnsureHeader(headers, ginHeaders, "x-codex-turn-state", "")
	misc.EnsureHeader(headers, ginHeaders, "x-codex-turn-metadata", "")
	misc.EnsureHeader(headers, ginHeaders, "x-client-request-id", "")
	misc.EnsureHeader(headers, ginHeaders, "x-responsesapi-include-timing-metrics", "")
	misc.EnsureHeader(headers, ginHeaders, "Version", "")
	if isAPIKey {
		ensureHeaderWithPriority(headers, ginHeaders, "User-Agent", "", "")
	} else {
		ensureHeaderWithConfigPrecedence(headers, ginHeaders, "User-Agent", cfgUserAgent, codexUserAgent)
	}

	betaHeader := strings.TrimSpace(headers.Get("OpenAI-Beta"))
	if betaHeader == "" && ginHeaders != nil {
		betaHeader = strings.TrimSpace(ginHeaders.Get("OpenAI-Beta"))
	}
	if betaHeader == "" || !strings.Contains(betaHeader, "responses_websockets=") {
		betaHeader = codexResponsesWebsocketBetaHeaderValue
	}
	headers.Set("OpenAI-Beta", betaHeader)
	sessionFallback := ""
	if strings.Contains(headers.Get("User-Agent"), "Mac OS") {
		sessionFallback = uuid.NewString()
	}
	ensureCodexWebsocketSessionHeader(headers, ginHeaders, sessionFallback)
	if originator := strings.TrimSpace(ginHeaders.Get("Originator")); originator != "" {
		headers.Set("Originator", originator)
	} else if !isAPIKey {
		headers.Set("Originator", codexOriginator)
	}
	if !isAPIKey {
		if auth != nil && auth.Metadata != nil {
			if accountID, ok := auth.Metadata["account_id"].(string); ok {
				if trimmed := strings.TrimSpace(accountID); trimmed != "" {
					setHeaderCasePreserved(headers, "ChatGPT-Account-ID", trimmed)
				}
			}
		}
	}

	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(&http.Request{Header: headers}, attrs)

	return headers
}

func ensureCodexWebsocketSessionHeader(target http.Header, source http.Header, fallbackValue string) {
	if target == nil {
		return
	}
	sessionID := codexSessionHeaderValue(target)
	if sessionID == "" {
		sessionID = codexSessionHeaderValue(source)
	}
	if sessionID == "" {
		sessionID = strings.TrimSpace(fallbackValue)
	}
	if sessionID != "" {
		setHeaderCasePreserved(target, "session_id", sessionID)
	}
	deleteHeaderCaseInsensitive(target, "Session-Id")
}

func codexSessionHeaderValue(headers http.Header) string {
	for _, key := range []string{"Session-Id", "Session_id", "session_id"} {
		if value := strings.TrimSpace(headerValueCaseInsensitive(headers, key)); value != "" {
			return value
		}
	}
	return ""
}

func codexAuthUsesAPIKey(auth *cliproxyauth.Auth) bool {
	if auth == nil || auth.Attributes == nil {
		return false
	}
	return strings.TrimSpace(auth.Attributes["api_key"]) != ""
}

func ensureHeaderCasePreserved(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(headerValueCaseInsensitive(target, key)) != "" {
		return
	}
	if source != nil {
		if val := strings.TrimSpace(headerValueCaseInsensitive(source, key)); val != "" {
			setHeaderCasePreserved(target, key, val)
			return
		}
	}
	if val := strings.TrimSpace(configValue); val != "" {
		setHeaderCasePreserved(target, key, val)
		return
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		setHeaderCasePreserved(target, key, val)
	}
}

func setHeaderCasePreserved(headers http.Header, key string, value string) {
	if headers == nil {
		return
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return
	}
	deleteHeaderCaseInsensitive(headers, key)
	headers[key] = []string{value}
}

func setCodexSessionHeaderCasePreserved(headers http.Header, fallbackKey string, value string) {
	if headers == nil {
		return
	}
	fallbackKey = strings.TrimSpace(fallbackKey)
	value = strings.TrimSpace(value)
	if fallbackKey == "" || value == "" {
		return
	}

	selectedKey := ""
	if _, ok := headers[fallbackKey]; ok && codexSessionHeaderKeyUsesUnderscore(fallbackKey) {
		selectedKey = fallbackKey
	} else {
		for existingKey := range headers {
			if codexSessionHeaderKeyUsesUnderscore(existingKey) {
				selectedKey = existingKey
				break
			}
		}
	}
	if selectedKey == "" {
		selectedKey = fallbackKey
	}
	for existingKey := range headers {
		if codexSessionHeaderKey(existingKey) && existingKey != selectedKey {
			delete(headers, existingKey)
		}
	}
	headers[selectedKey] = []string{value}
}

func codexSessionHeaderKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	return normalized == "session_id" || normalized == "session-id"
}

func codexSessionHeaderKeyUsesUnderscore(key string) bool {
	return strings.ToLower(strings.TrimSpace(key)) == "session_id"
}

func headerValueCaseInsensitive(headers http.Header, key string) string {
	key = strings.TrimSpace(key)
	if headers == nil || key == "" {
		return ""
	}
	if val := strings.TrimSpace(headers.Get(key)); val != "" {
		return val
	}
	for existingKey, values := range headers {
		if !strings.EqualFold(existingKey, key) {
			continue
		}
		for _, value := range values {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func deleteHeaderCaseInsensitive(headers http.Header, key string) {
	for existingKey := range headers {
		if strings.EqualFold(existingKey, key) {
			delete(headers, existingKey)
		}
	}
}

func codexHeaderDefaults(cfg *config.Config, auth *cliproxyauth.Auth) (string, string) {
	if cfg == nil || auth == nil {
		return "", ""
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["api_key"]); v != "" {
			return "", ""
		}
	}
	return strings.TrimSpace(cfg.CodexHeaderDefaults.UserAgent), strings.TrimSpace(cfg.CodexHeaderDefaults.BetaFeatures)
}

func ensureHeaderWithPriority(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(target.Get(key)) != "" {
		return
	}
	if source != nil {
		if val := strings.TrimSpace(source.Get(key)); val != "" {
			target.Set(key, val)
			return
		}
	}
	if val := strings.TrimSpace(configValue); val != "" {
		target.Set(key, val)
		return
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		target.Set(key, val)
	}
}

func ensureHeaderWithConfigPrecedence(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(target.Get(key)) != "" {
		return
	}
	if val := strings.TrimSpace(configValue); val != "" {
		target.Set(key, val)
		return
	}
	if source != nil {
		if val := strings.TrimSpace(source.Get(key)); val != "" {
			target.Set(key, val)
			return
		}
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		target.Set(key, val)
	}
}

type statusErrWithHeaders struct {
	statusErr
	headers http.Header
}

func (e statusErrWithHeaders) Headers() http.Header {
	if e.headers == nil {
		return nil
	}
	return e.headers.Clone()
}

func parseCodexWebsocketError(payload []byte) (error, bool) {
	if len(payload) == 0 {
		return nil, false
	}
	if strings.TrimSpace(gjson.GetBytes(payload, "type").String()) != "error" {
		return nil, false
	}
	status := int(gjson.GetBytes(payload, "status").Int())
	if status == 0 {
		status = int(gjson.GetBytes(payload, "status_code").Int())
	}
	if status <= 0 {
		return nil, false
	}

	out := buildCodexWebsocketErrorPayload(payload, status)
	headers := parseCodexWebsocketErrorHeaders(payload)
	statusError := statusErr{code: status, msg: string(out)}
	if retryAfter := parseCodexRetryAfter(status, out, time.Now()); retryAfter != nil {
		statusError.retryAfter = retryAfter
	} else if isCodexWebsocketConnectionLimitError(payload) {
		retryAfter := time.Duration(0)
		statusError.retryAfter = &retryAfter
	}
	return statusErrWithHeaders{
		statusErr: statusError,
		headers:   headers,
	}, true
}

func buildCodexWebsocketErrorPayload(payload []byte, status int) []byte {
	out := []byte(`{}`)
	out, _ = sjson.SetBytes(out, "status", status)

	if bodyNode := gjson.GetBytes(payload, "body"); bodyNode.Exists() {
		out, _ = sjson.SetRawBytes(out, "body", []byte(bodyNode.Raw))
		if bodyErrorNode := bodyNode.Get("error"); bodyErrorNode.Exists() {
			out, _ = sjson.SetRawBytes(out, "error", []byte(bodyErrorNode.Raw))
			return out
		}
	}

	if errNode := gjson.GetBytes(payload, "error"); errNode.Exists() {
		out, _ = sjson.SetRawBytes(out, "error", []byte(errNode.Raw))
		return out
	}

	out, _ = sjson.SetBytes(out, "error.type", "server_error")
	out, _ = sjson.SetBytes(out, "error.message", http.StatusText(status))
	return out
}

func isCodexWebsocketConnectionLimitError(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	for _, path := range []string{"error.code", "error.type", "body.error.code", "body.error.type", "code", "error"} {
		if strings.TrimSpace(gjson.GetBytes(payload, path).String()) == "websocket_connection_limit_reached" {
			return true
		}
	}
	return false
}

func parseCodexWebsocketErrorHeaders(payload []byte) http.Header {
	headersNode := gjson.GetBytes(payload, "headers")
	if !headersNode.Exists() || !headersNode.IsObject() {
		return nil
	}
	mapped := make(http.Header)
	headersNode.ForEach(func(key, value gjson.Result) bool {
		name := strings.TrimSpace(key.String())
		if name == "" {
			return true
		}
		switch value.Type {
		case gjson.String:
			if v := strings.TrimSpace(value.String()); v != "" {
				mapped.Set(name, v)
			}
		case gjson.Number, gjson.True, gjson.False:
			if v := strings.TrimSpace(value.Raw); v != "" {
				mapped.Set(name, v)
			}
		default:
		}
		return true
	})
	if len(mapped) == 0 {
		return nil
	}
	return mapped
}

func normalizeCodexWebsocketCompletion(payload []byte) []byte {
	if strings.TrimSpace(gjson.GetBytes(payload, "type").String()) == "response.done" {
		updated, err := sjson.SetBytes(payload, "type", "response.completed")
		if err == nil && len(updated) > 0 {
			return updated
		}
	}
	return payload
}

func encodeCodexWebsocketAsSSE(payload []byte) []byte {
	if len(payload) == 0 {
		return nil
	}
	line := make([]byte, 0, len("data: ")+len(payload))
	line = append(line, []byte("data: ")...)
	line = append(line, payload...)
	return line
}

func websocketUpgradeRequestLog(info helps.UpstreamRequestLog) helps.UpstreamRequestLog {
	upgradeInfo := info
	upgradeInfo.URL = helps.WebsocketUpgradeRequestURL(info.URL)
	upgradeInfo.Method = http.MethodGet
	upgradeInfo.Body = nil
	upgradeInfo.Headers = info.Headers.Clone()
	if upgradeInfo.Headers == nil {
		upgradeInfo.Headers = make(http.Header)
	}
	if strings.TrimSpace(upgradeInfo.Headers.Get("Connection")) == "" {
		upgradeInfo.Headers.Set("Connection", "Upgrade")
	}
	if strings.TrimSpace(upgradeInfo.Headers.Get("Upgrade")) == "" {
		upgradeInfo.Headers.Set("Upgrade", "websocket")
	}
	return upgradeInfo
}

func recordAPIWebsocketHandshake(ctx context.Context, cfg *config.Config, resp *http.Response) {
	if resp == nil {
		return
	}
	helps.RecordAPIWebsocketHandshake(ctx, cfg, resp.StatusCode, resp.Header.Clone())
	closeHTTPResponseBody(resp, "codex websockets executor: close handshake response body error")
}

func websocketHandshakeBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	closeHTTPResponseBody(resp, "codex websockets executor: close handshake response body error")
	if len(body) == 0 {
		return nil
	}
	return body
}

func shouldFallbackCodexWebsocketHandshake(ctx context.Context, resp *http.Response) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if resp == nil {
		return true
	}
	switch resp.StatusCode {
	case http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusMethodNotAllowed,
		http.StatusUpgradeRequired,
		http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func shouldPersistCodexWebsocketFallback(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	switch resp.StatusCode {
	case http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusMethodNotAllowed,
		http.StatusUpgradeRequired,
		http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func closeHTTPResponseBody(resp *http.Response, logPrefix string) {
	if resp == nil || resp.Body == nil {
		return
	}
	if errClose := resp.Body.Close(); errClose != nil {
		log.Errorf("%s: %v", logPrefix, errClose)
	}
}

func executionSessionIDFromOptions(opts cliproxyexecutor.Options) string {
	if len(opts.Metadata) == 0 {
		return ""
	}
	raw, ok := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		return ""
	}
}

func (e *CodexWebsocketsExecutor) getOrCreateSession(sessionID string) *codexWebsocketSession {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	if e == nil {
		return nil
	}
	e.drainMu.RLock()
	draining := e.draining
	e.drainMu.RUnlock()
	if draining {
		return nil
	}
	store := e.store
	if store == nil {
		return nil
	}
	now := time.Now()
	runtimeCfg := e.cfg.NormalizedCodexWebsocketConfig()
	ttl := runtimeCfg.SessionTTL
	maxSessions := runtimeCfg.MaxSessions

	store.mu.Lock()
	if store.sessions == nil {
		store.sessions = make(map[string]*codexWebsocketSession)
	}
	if sess, ok := store.sessions[sessionID]; ok && sess != nil {
		sess.lastUsed = now
		store.mu.Unlock()
		return sess
	}
	managedClaudeSession := strings.HasPrefix(sessionID, helps.ClaudeCodeWebsocketSessionPrefix)

	toClose := make([]*codexWebsocketSession, 0)
	managedSessionCount := 0
	for existingID, existing := range store.sessions {
		if existing == nil {
			delete(store.sessions, existingID)
			continue
		}
		if !strings.HasPrefix(existingID, helps.ClaudeCodeWebsocketSessionPrefix) {
			continue
		}
		if managedClaudeSession && !existing.lastUsed.IsZero() && now.Sub(existing.lastUsed) > ttl && !codexWebsocketSessionActive(existing) {
			delete(store.sessions, existingID)
			toClose = append(toClose, existing)
			continue
		}
		managedSessionCount++
	}

	if managedClaudeSession && maxSessions > 0 && managedSessionCount >= maxSessions {
		oldestID := ""
		var oldest *codexWebsocketSession
		for existingID, existing := range store.sessions {
			if existing == nil || !strings.HasPrefix(existingID, helps.ClaudeCodeWebsocketSessionPrefix) || codexWebsocketSessionActive(existing) {
				continue
			}
			if oldest == nil || existing.lastUsed.Before(oldest.lastUsed) {
				oldestID = existingID
				oldest = existing
			}
		}
		if oldest == nil {
			store.mu.Unlock()
			for i := range toClose {
				closeCodexWebsocketSession(toClose[i], "session_expired")
			}
			return nil
		}
		delete(store.sessions, oldestID)
		toClose = append(toClose, oldest)
	}
	sess := &codexWebsocketSession{
		sessionID:            sessionID,
		lastUsed:             now,
		upstreamDisconnectCh: make(chan error, 1),
	}
	store.sessions[sessionID] = sess
	store.mu.Unlock()
	for i := range toClose {
		closeCodexWebsocketSession(toClose[i], "session_evicted")
	}
	return sess
}

func codexWebsocketSessionActive(sess *codexWebsocketSession) bool {
	if sess == nil {
		return false
	}
	sess.activeMu.Lock()
	active := sess.activeCh != nil
	sess.activeMu.Unlock()
	return active
}

func (s *codexWebsocketSession) lockRequest() (time.Duration, bool) {
	startedAt := time.Now()
	if s.reqMu.TryLock() {
		return time.Since(startedAt), false
	}
	s.reqMu.Lock()
	return time.Since(startedAt), true
}

func (s *codexWebsocketSession) codexHTTPFallback() bool {
	if s == nil {
		return false
	}
	s.stateMu.Lock()
	enabled := s.httpFallback
	s.stateMu.Unlock()
	return enabled
}

func (s *codexWebsocketSession) activateCodexHTTPFallback() {
	if s == nil {
		return
	}
	s.stateMu.Lock()
	s.httpFallback = true
	s.resetCodexIncrementalStateLocked()
	s.stateMu.Unlock()
}

func (s *codexWebsocketSession) resetCodexIncrementalState() {
	if s == nil {
		return
	}
	s.stateMu.Lock()
	s.resetCodexIncrementalStateLocked()
	s.stateMu.Unlock()
}

func (s *codexWebsocketSession) resetCodexIncrementalStateLocked() {
	s.lastRequest = nil
	s.lastResponseID = ""
	s.lastResponseOutput = nil
	s.pendingRequest = nil
}

type codexIncrementalObservation struct {
	incremental bool
	resetReason string
}

func (s *codexWebsocketSession) prepareCodexIncrementalRequest(fullRequest []byte) []byte {
	request, _ := s.prepareCodexIncrementalRequestObserved(fullRequest)
	return request
}

func (s *codexWebsocketSession) prepareCodexIncrementalRequestObserved(fullRequest []byte) ([]byte, codexIncrementalObservation) {
	if s == nil || len(fullRequest) == 0 {
		return fullRequest, codexIncrementalObservation{resetReason: "not_applicable"}
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if len(fullRequest) > codexWebsocketMaxIncrementalStateBytes {
		s.resetCodexIncrementalStateLocked()
		return fullRequest, codexIncrementalObservation{resetReason: "state_too_large"}
	}
	s.pendingRequest = bytes.Clone(fullRequest)
	if s.lastResponseID == "" || len(s.lastRequest) == 0 {
		return fullRequest, codexIncrementalObservation{resetReason: "no_previous_response"}
	}
	delta, ok := codexIncrementalInput(s.lastRequest, s.lastResponseOutput, fullRequest)
	if !ok {
		return fullRequest, codexIncrementalObservation{resetReason: "request_mismatch"}
	}
	incremental, errSet := sjson.SetRawBytes(fullRequest, "input", delta)
	if errSet != nil {
		return fullRequest, codexIncrementalObservation{resetReason: "delta_encode_failed"}
	}
	incremental, errSet = sjson.SetBytes(incremental, "previous_response_id", s.lastResponseID)
	if errSet != nil {
		return fullRequest, codexIncrementalObservation{resetReason: "previous_response_encode_failed"}
	}
	return incremental, codexIncrementalObservation{incremental: true}
}

func (s *codexWebsocketSession) completeCodexIncrementalRequest(completedPayload []byte) {
	if s == nil || len(completedPayload) == 0 {
		return
	}
	responseID := strings.TrimSpace(gjson.GetBytes(completedPayload, "response.id").String())
	if responseID == "" {
		return
	}
	responseOutput := gjson.GetBytes(completedPayload, "response.output")
	output := []byte("[]")
	if responseOutput.IsArray() {
		output = []byte(responseOutput.Raw)
	}
	s.stateMu.Lock()
	if len(s.pendingRequest) != 0 {
		if len(s.pendingRequest)+len(output) > codexWebsocketMaxIncrementalStateBytes {
			s.resetCodexIncrementalStateLocked()
			s.stateMu.Unlock()
			return
		}
		s.lastRequest = bytes.Clone(s.pendingRequest)
		s.lastResponseID = responseID
		s.lastResponseOutput = bytes.Clone(output)
	}
	s.pendingRequest = nil
	s.stateMu.Unlock()
}

func codexIncrementalInput(previousRequest, previousOutput, currentRequest []byte) ([]byte, bool) {
	var previousObject map[string]any
	var currentObject map[string]any
	if json.Unmarshal(previousRequest, &previousObject) != nil || json.Unmarshal(currentRequest, &currentObject) != nil {
		return nil, false
	}
	previousInput, previousInputOK := previousObject["input"].([]any)
	currentInput, currentInputOK := currentObject["input"].([]any)
	if !previousInputOK || !currentInputOK {
		return nil, false
	}
	delete(previousObject, "input")
	delete(currentObject, "input")
	for _, ignored := range []string{"client_metadata", "stream_options", "type"} {
		delete(previousObject, ignored)
		delete(currentObject, ignored)
	}
	if !reflect.DeepEqual(previousObject, currentObject) {
		return nil, false
	}

	var responseOutput []any
	if len(previousOutput) != 0 && json.Unmarshal(previousOutput, &responseOutput) != nil {
		return nil, false
	}
	baseline := make([]any, 0, len(previousInput)+len(responseOutput))
	baseline = append(baseline, previousInput...)
	baseline = append(baseline, responseOutput...)
	if len(currentInput) < len(baseline) {
		return nil, false
	}
	for index := range baseline {
		if !reflect.DeepEqual(normalizeCodexIncrementalItem(baseline[index]), normalizeCodexIncrementalItem(currentInput[index])) {
			return nil, false
		}
	}
	delta, errMarshal := json.Marshal(currentInput[len(baseline):])
	if errMarshal != nil {
		return nil, false
	}
	return delta, true
}

func normalizeCodexIncrementalItem(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(typed))
		for key, child := range typed {
			switch key {
			case "id", "status", "internal_chat_message_metadata_passthrough":
				continue
			case "annotations":
				if annotations, ok := child.([]any); ok && len(annotations) == 0 {
					continue
				}
			}
			normalized[key] = normalizeCodexIncrementalItem(child)
		}
		return normalized
	case []any:
		normalized := make([]any, len(typed))
		for index := range typed {
			normalized[index] = normalizeCodexIncrementalItem(typed[index])
		}
		return normalized
	default:
		return value
	}
}

func (e *CodexWebsocketsExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	sess := e.getOrCreateSession(sessionID)
	if sess == nil {
		return nil
	}
	return sess.upstreamDisconnectCh
}

type codexWebsocketConnectionSource string

const (
	codexWebsocketConnectionCold         codexWebsocketConnectionSource = "cold"
	codexWebsocketConnectionSessionReuse codexWebsocketConnectionSource = "session_reuse"
	codexWebsocketConnectionSpeculative  codexWebsocketConnectionSource = "speculative"
	codexWebsocketConnectionOverflow     codexWebsocketConnectionSource = "overflow"
)

func (e *CodexWebsocketsExecutor) ensureUpstreamConn(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, authID string, wsURL string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	conn, resp, _, err := e.ensureUpstreamConnObserved(ctx, auth, sess, authID, wsURL, headers)
	return conn, resp, err
}

func (e *CodexWebsocketsExecutor) ensureOverflowConnObserved(ctx context.Context, auth *cliproxyauth.Auth, authID string, wsURL string, headers http.Header, sessionID string) (*websocket.Conn, *http.Response, codexWebsocketConnectionSource, error) {
	if pooledConn := e.takeSpeculativePreconnect(ctx, auth, authID, wsURL, headers, sessionID); pooledConn != nil {
		return pooledConn, nil, codexWebsocketConnectionSpeculative, nil
	}
	conn, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
	return conn, resp, codexWebsocketConnectionCold, errDial
}

func (e *CodexWebsocketsExecutor) ensureUpstreamConnObserved(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, authID string, wsURL string, headers http.Header) (*websocket.Conn, *http.Response, codexWebsocketConnectionSource, error) {
	if sess == nil {
		conn, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
		return conn, resp, codexWebsocketConnectionCold, errDial
	}

	sess.connMu.Lock()
	conn := sess.conn
	readerConn := sess.readerConn
	existingAuthID := sess.authID
	existingWSURL := sess.wsURL
	credentialMismatch := conn != nil && (existingAuthID != authID || existingWSURL != wsURL)
	if credentialMismatch {
		sess.conn = nil
		if sess.readerConn == conn {
			sess.readerConn = nil
		}
		readerConn = nil
	}
	sess.connMu.Unlock()
	if credentialMismatch {
		sess.resetCodexIncrementalState()
		logCodexWebsocketDisconnected(sess.sessionID, existingAuthID, existingWSURL, "route_changed", nil)
		if errClose := conn.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		conn = nil
	}
	if conn != nil {
		if readerConn != conn {
			sess.connMu.Lock()
			sess.readerConn = conn
			sess.connMu.Unlock()
			sess.configureConn(conn)
			go e.readUpstreamLoop(sess, conn)
		}
		return conn, nil, codexWebsocketConnectionSessionReuse, nil
	}
	if pooledConn := e.takeSpeculativePreconnect(ctx, auth, authID, wsURL, headers, sess.sessionID); pooledConn != nil {
		sess.connMu.Lock()
		sess.conn = pooledConn
		sess.wsURL = wsURL
		sess.authID = authID
		sess.readerConn = pooledConn
		sess.connMu.Unlock()
		sess.configureConn(pooledConn)
		go e.readUpstreamLoop(sess, pooledConn)
		logCodexWebsocketConnected(sess.sessionID, authID, wsURL)
		return pooledConn, nil, codexWebsocketConnectionSpeculative, nil
	}

	conn, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
	if errDial != nil {
		return nil, resp, codexWebsocketConnectionCold, errDial
	}

	sess.connMu.Lock()
	if sess.conn != nil {
		previous := sess.conn
		sess.connMu.Unlock()
		if errClose := conn.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		return previous, nil, codexWebsocketConnectionSessionReuse, nil
	}
	sess.conn = conn
	sess.wsURL = wsURL
	sess.authID = authID
	sess.readerConn = conn
	sess.connMu.Unlock()

	sess.configureConn(conn)
	go e.readUpstreamLoop(sess, conn)
	logCodexWebsocketConnected(sess.sessionID, authID, wsURL)
	return conn, resp, codexWebsocketConnectionCold, nil
}

func (e *CodexWebsocketsExecutor) readUpstreamLoop(sess *codexWebsocketSession, conn *websocket.Conn) {
	if e == nil || sess == nil || conn == nil {
		return
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
		msgType, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			sess.activeMu.Lock()
			ch := sess.activeCh
			done := sess.activeDone
			sess.activeMu.Unlock()
			if ch != nil {
				select {
				case ch <- codexWebsocketRead{conn: conn, err: errRead}:
				case <-done:
				default:
				}
				sess.clearActive(ch)
				close(ch)
			}
			e.invalidateUpstreamConn(sess, conn, "upstream_disconnected", errRead)
			return
		}

		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
				sess.activeMu.Lock()
				ch := sess.activeCh
				done := sess.activeDone
				sess.activeMu.Unlock()
				if ch != nil {
					select {
					case ch <- codexWebsocketRead{conn: conn, err: errBinary}:
					case <-done:
					default:
					}
					sess.clearActive(ch)
					close(ch)
				}
				e.invalidateUpstreamConn(sess, conn, "unexpected_binary", errBinary)
				return
			}
			continue
		}

		sess.activeMu.Lock()
		ch := sess.activeCh
		done := sess.activeDone
		sess.activeMu.Unlock()
		if ch == nil {
			continue
		}
		select {
		case ch <- codexWebsocketRead{conn: conn, msgType: msgType, payload: payload}:
		case <-done:
		}
	}
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConn(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	if sess == nil || conn == nil {
		return
	}

	sess.connMu.Lock()
	current := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	sessionID := sess.sessionID
	if current == nil || current != conn {
		sess.connMu.Unlock()
		return
	}
	sess.conn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sess.connMu.Unlock()

	sess.resetCodexIncrementalState()
	logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, err)
	sess.notifyUpstreamDisconnect(err)
	if errClose := conn.Close(); errClose != nil {
		log.Errorf("codex websockets executor: close websocket error: %v", errClose)
	}
}

func (e *CodexWebsocketsExecutor) CloseExecutionSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if e == nil {
		return
	}
	if sessionID == "" {
		return
	}
	if sessionID == cliproxyauth.CloseAllExecutionSessionsID {
		e.drainMu.Lock()
		e.draining = true
		e.drainMu.Unlock()
		if e.runtimeCancel != nil {
			e.runtimeCancel()
		}
		if e.pool != nil {
			e.pool.closeAll()
		}
		e.drainExecutionSessions("executor_drained")
		return
	}

	store := e.store
	if store == nil {
		return
	}
	store.mu.Lock()
	sess := store.sessions[sessionID]
	delete(store.sessions, sessionID)
	store.mu.Unlock()

	e.closeExecutionSession(sess, "session_closed")
}

func (e *CodexWebsocketsExecutor) closeAllExecutionSessions(reason string) {
	if e == nil {
		return
	}

	store := e.store
	if store == nil {
		return
	}
	store.mu.Lock()
	sessions := make([]*codexWebsocketSession, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		delete(store.sessions, sessionID)
		if sess != nil {
			sessions = append(sessions, sess)
		}
	}
	store.mu.Unlock()

	for i := range sessions {
		e.closeExecutionSession(sessions[i], reason)
	}
}

func (e *CodexWebsocketsExecutor) drainExecutionSessions(reason string) {
	if e == nil || e.store == nil {
		return
	}
	e.store.mu.Lock()
	sessions := make([]*codexWebsocketSession, 0, len(e.store.sessions))
	for sessionID, sess := range e.store.sessions {
		delete(e.store.sessions, sessionID)
		if sess != nil {
			sessions = append(sessions, sess)
		}
	}
	e.store.mu.Unlock()

	for _, sess := range sessions {
		e.drainWG.Add(1)
		go func(session *codexWebsocketSession) {
			defer e.drainWG.Done()
			session.reqMu.Lock()
			defer session.reqMu.Unlock()
			e.closeExecutionSession(session, reason)
		}(sess)
	}
}

func (e *CodexWebsocketsExecutor) closeExecutionSession(sess *codexWebsocketSession, reason string) {
	closeCodexWebsocketSession(sess, reason)
}

func closeCodexWebsocketSession(sess *codexWebsocketSession, reason string) {
	if sess == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "session_closed"
	}

	sess.connMu.Lock()
	conn := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	sess.conn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sessionID := sess.sessionID
	sess.connMu.Unlock()

	if conn == nil {
		return
	}
	logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, nil)
	if errClose := conn.Close(); errClose != nil {
		log.Errorf("codex websockets executor: close websocket error: %v", errClose)
	}
}

func logCodexWebsocketConnected(sessionID string, authID string, wsURL string) {
	log.Infof("codex websockets: upstream connected session=%s auth=%s url=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL))
}

func elapsedSinceOrZero(start time.Time, end time.Time) time.Duration {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return end.Sub(start)
}

func recordCodexWebsocketUsageMetric(ctx context.Context, cfg *config.Config, sessionID string, detail cliproxyusage.Detail) {
	fields := helps.CodexUsageMetricFields(detail)
	fields["session_id"] = sessionID
	helps.RecordAPIWebsocketMetric(ctx, cfg, "usage", fields)
}

func logCodexWebsocketDisconnected(sessionID string, authID string, wsURL string, reason string, err error) {
	if err != nil {
		log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s reason=%s err=%v", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason), err)
		return
	}
	log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s reason=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason))
}

// CloseAuthExecutionSessions closes this executor's Codex resources for an auth ID.
func (e *CodexWebsocketsExecutor) CloseAuthExecutionSessions(authID string, reason string) {
	authID = strings.TrimSpace(authID)
	if e == nil || authID == "" {
		return
	}
	if e.pool != nil {
		e.pool.closeAuth(authID)
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "auth_removed"
	}

	store := e.store
	if store == nil {
		return
	}

	type sessionItem struct {
		sessionID string
		sess      *codexWebsocketSession
	}

	store.mu.Lock()
	items := make([]sessionItem, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		items = append(items, sessionItem{sessionID: sessionID, sess: sess})
	}
	store.mu.Unlock()

	matches := make([]sessionItem, 0)
	for i := range items {
		sess := items[i].sess
		if sess == nil {
			continue
		}
		sess.connMu.Lock()
		sessAuthID := strings.TrimSpace(sess.authID)
		sess.connMu.Unlock()
		if sessAuthID == authID {
			matches = append(matches, items[i])
		}
	}
	if len(matches) == 0 {
		return
	}

	toClose := make([]*codexWebsocketSession, 0, len(matches))
	store.mu.Lock()
	for i := range matches {
		current, ok := store.sessions[matches[i].sessionID]
		if !ok || current == nil || current != matches[i].sess {
			continue
		}
		delete(store.sessions, matches[i].sessionID)
		toClose = append(toClose, current)
	}
	store.mu.Unlock()

	for i := range toClose {
		closeCodexWebsocketSession(toClose[i], reason)
	}
}

// CodexAutoExecutor routes Codex requests to the websocket transport only when:
//  1. The downstream transport is websocket or upstream websocket is preferred, and
//  2. The selected auth enables websockets.
//
// For non-websocket downstream requests, it always uses the legacy HTTP implementation.
type CodexAutoExecutor struct {
	httpExec *CodexExecutor
	wsExec   *CodexWebsocketsExecutor
}

func NewCodexAutoExecutor(cfg *config.Config) *CodexAutoExecutor {
	return &CodexAutoExecutor{
		httpExec: NewCodexExecutor(cfg),
		wsExec:   NewCodexWebsocketsExecutor(cfg),
	}
}

func (e *CodexAutoExecutor) Identifier() string { return "codex" }

func (e *CodexAutoExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if e == nil || e.httpExec == nil {
		return nil
	}
	return e.httpExec.PrepareRequest(req, auth)
}

func (e *CodexAutoExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.HttpRequest(ctx, auth, req)
}

func (e *CodexAutoExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.httpExec == nil || e.wsExec == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codex auto executor: executor is nil")
	}
	if codexShouldUseWebsockets(ctx, auth) {
		return e.wsExec.Execute(ctx, auth, req, opts)
	}
	return e.httpExec.Execute(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if e == nil || e.httpExec == nil || e.wsExec == nil {
		return nil, fmt.Errorf("codex auto executor: executor is nil")
	}
	if codexShouldUseWebsockets(ctx, auth) {
		return e.wsExec.ExecuteStream(ctx, auth, req, opts)
	}
	return e.httpExec.ExecuteStream(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.Refresh(ctx, auth)
}

func (e *CodexAutoExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.httpExec == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.CountTokens(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) CloseExecutionSession(sessionID string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseExecutionSession(sessionID)
}

func (e *CodexAutoExecutor) CloseAuthExecutionSessions(authID, reason string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseAuthExecutionSessions(authID, reason)
}

func (e *CodexAutoExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	if e == nil || e.wsExec == nil {
		return nil
	}
	return e.wsExec.UpstreamDisconnectChan(sessionID)
}

func codexWebsocketsEnabled(auth *cliproxyauth.Auth) bool {
	if auth == nil {
		return false
	}
	if len(auth.Attributes) > 0 {
		if raw := strings.TrimSpace(auth.Attributes["websockets"]); raw != "" {
			parsed, errParse := strconv.ParseBool(raw)
			if errParse == nil {
				return parsed
			}
		}
	}
	if len(auth.Metadata) == 0 {
		return false
	}
	raw, ok := auth.Metadata["websockets"]
	if !ok || raw == nil {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(v))
		if errParse == nil {
			return parsed
		}
	default:
	}
	return false
}

func codexShouldUseWebsockets(ctx context.Context, auth *cliproxyauth.Auth) bool {
	if !codexWebsocketsEnabled(auth) {
		return false
	}
	return cliproxyexecutor.DownstreamWebsocket(ctx) || cliproxyexecutor.PreferUpstreamWebsocket(ctx)
}
