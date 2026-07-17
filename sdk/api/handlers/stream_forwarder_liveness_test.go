package handlers

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestForwardStreamIdleTimeoutIsNotResetByKeepAlive(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	idle := 35 * time.Millisecond
	keepAlive := 5 * time.Millisecond
	cancelled := make(chan error, 1)
	h := NewBaseAPIHandlers(&config.SDKConfig{}, nil)

	done := make(chan struct{})
	go func() {
		h.ForwardStream(c, recorder, func(err error) { cancelled <- err }, data, errs, StreamForwardOptions{
			IdleTimeout:       &idle,
			KeepAliveInterval: &keepAlive,
			WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
				_, _ = recorder.WriteString("event: error\ndata: idle\n\n")
			},
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not terminate after idle timeout")
	}
	if got := strings.Count(recorder.Body.String(), ": keep-alive\n\n"); got < 2 {
		t.Fatalf("keepalive count = %d, want at least 2; body=%q", got, recorder.Body.String())
	}
	if got := strings.Count(recorder.Body.String(), "event: error"); got != 1 {
		t.Fatalf("terminal errors = %d, want 1; body=%q", got, recorder.Body.String())
	}
	if err := <-cancelled; !errors.Is(err, ErrStreamIdleTimeout) {
		t.Fatalf("cancel error = %v, want ErrStreamIdleTimeout", err)
	}
}

func TestForwardStreamDataResetsIdleTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	data := make(chan []byte, 2)
	errs := make(chan *interfaces.ErrorMessage)
	idle := 40 * time.Millisecond
	h := NewBaseAPIHandlers(&config.SDKConfig{}, nil)
	cancelled := make(chan error, 1)

	go h.ForwardStream(c, recorder, func(err error) { cancelled <- err }, data, errs, StreamForwardOptions{
		IdleTimeout: &idle,
		WriteChunk:  func(chunk []byte) { _, _ = recorder.Write(chunk) },
	})
	time.Sleep(25 * time.Millisecond)
	data <- []byte("thinking")
	time.Sleep(25 * time.Millisecond)
	close(data)

	if err := <-cancelled; err != nil {
		t.Fatalf("cancel error = %v, want nil", err)
	}
	if recorder.Body.String() != "thinking" {
		t.Fatalf("body = %q", recorder.Body.String())
	}
}

func TestStreamingIdleTimeoutConfiguration(t *testing.T) {
	if got := StreamingIdleTimeout(&config.SDKConfig{Streaming: config.StreamingConfig{IdleTimeoutSeconds: 7}}); got != 7*time.Second {
		t.Fatalf("timeout = %s, want 7s", got)
	}
	if got := StreamingIdleTimeout(&config.SDKConfig{}); got != 0 {
		t.Fatalf("unset timeout = %s, want disabled", got)
	}
}
