package executor

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/tidwall/gjson"
)

type codexTextDeltaFingerprintData struct {
	Fingerprint  string
	Bytes        int64
	OutputIndex  int64
	ContentIndex int64
}

func codexTextDeltaFingerprint(payload []byte) (codexTextDeltaFingerprintData, bool) {
	if !gjson.ValidBytes(payload) || gjson.GetBytes(payload, "type").String() != "response.output_text.delta" {
		return codexTextDeltaFingerprintData{}, false
	}
	text := gjson.GetBytes(payload, "delta").String()
	if text == "" {
		return codexTextDeltaFingerprintData{}, false
	}
	sum := sha256.Sum256([]byte(text))
	return codexTextDeltaFingerprintData{
		Fingerprint:  hex.EncodeToString(sum[:8]),
		Bytes:        int64(len(text)),
		OutputIndex:  gjson.GetBytes(payload, "output_index").Int(),
		ContentIndex: gjson.GetBytes(payload, "content_index").Int(),
	}, true
}
