package claude

import (
	"errors"
	"fmt"
)

type claudePipelinePhase uint8

const (
	claudePhaseReceived claudePipelinePhase = iota
	claudePhaseDecoded
	claudePhaseNormalized
	claudePhaseClassified
	claudePhaseRepaired
	claudePhaseShaped
	claudePhaseBudgeted
	claudePhasePreflighted
	claudePhaseRouted
	claudePhaseDispatched
	claudePhaseRejected
)

var errClaudePipelineTransition = errors.New("invalid Claude request pipeline transition")

type claudeRequestPipeline struct {
	phase           claudePipelinePhase
	raw             []byte
	document        *claudeRequestDocument
	classifierModel string
	kind            claudeRequestKind
}

type claudePipelineResult struct {
	Body             []byte
	Model            string
	Kind             claudeRequestKind
	Pressure         claudeContextPressureResult
	Repair           claudeToolHistoryRepairResult
	Replay           claudeCompactionReplayObservation
	Edit             claudeContextEditResult
	CompactBudget    claudeReactiveCompactBudgetObservation
	AdaptiveBudget   claudeAdaptiveOutputBudgetObservation
	ClassifierRouted bool
	Rejected         bool
	Err              error
}

func newClaudeRequestPipeline(raw []byte, classifierModel string) *claudeRequestPipeline {
	return &claudeRequestPipeline{phase: claudePhaseReceived, raw: raw, classifierModel: classifierModel, kind: claudeRequestInteractive}
}

func (p *claudeRequestPipeline) transition(next claudePipelinePhase) error {
	if p == nil || next != p.phase+1 || p.phase >= claudePhaseDispatched {
		return fmt.Errorf("%w: %d -> %d", errClaudePipelineTransition, p.phase, next)
	}
	p.phase = next
	return nil
}

func (p *claudeRequestPipeline) run(countTokens bool) claudePipelineResult {
	result := claudePipelineResult{}
	document, errDocument := newClaudeRequestDocument(p.raw)
	if errDocument != nil {
		result.Err = errDocument
		return result
	}
	p.document = document
	if errTransition := p.transition(claudePhaseDecoded); errTransition != nil {
		result.Err = errTransition
		return result
	}

	body, _ := document.bytes()
	normalized := rewriteClaudeDDModelInBody(body)
	if string(normalized) != string(body) {
		document, errDocument = newClaudeRequestDocument(normalized)
		if errDocument != nil {
			result.Err = errDocument
			return result
		}
		p.document = document
	}
	if errTransition := p.transition(claudePhaseNormalized); errTransition != nil {
		result.Err = errTransition
		return result
	}

	body, _ = p.document.bytes()
	if countTokens {
		p.kind = claudeRequestCountTokens
	} else if rewritten, routed := rewriteClaudeCodeAutoModeClassifierModel(body, p.classifierModel); routed {
		p.kind = claudeRequestClassifier
		p.document, _ = newClaudeRequestDocument(rewritten)
		result.ClassifierRouted = true
	} else if isClaudeReactiveCompactDocument(p.document) {
		p.kind = claudeRequestReactiveCompact
	}
	if errTransition := p.transition(claudePhaseClassified); errTransition != nil {
		result.Err = errTransition
		return result
	}

	if p.document.hasContentType("tool_use") {
		body, _ = p.document.bytes()
		repaired, repair, errRepair := repairInterruptedClaudeToolHistory(body)
		if errRepair != nil {
			result.Err = errRepair
			return result
		}
		result.Repair = repair
		if repair.Applied {
			p.document, _ = newClaudeRequestDocument(repaired)
		}
	}
	if errTransition := p.transition(claudePhaseRepaired); errTransition != nil {
		result.Err = errTransition
		return result
	}

	if p.document.hasContentType("compaction") {
		body, _ = p.document.bytes()
		shaped, replay := applyClaudeCompactionReplay(body, claudeCompactionV2RetainedTokenBudget)
		result.Replay = replay
		if replay.Applied {
			p.document, _ = newClaudeRequestDocument(shaped)
		}
	}
	if p.document.hasContextEdits() {
		body, _ = p.document.bytes()
		shaped, edit := applyClaudeContextEditing(body)
		result.Edit = edit
		if edit.Applied {
			p.document, _ = newClaudeRequestDocument(shaped)
		}
	}
	if errTransition := p.transition(claudePhaseShaped); errTransition != nil {
		result.Err = errTransition
		return result
	}

	policy := claudePolicyFor(p.document.model(), p.kind)
	if p.kind == claudeRequestReactiveCompact && p.document.maxTokens() > policy.MaximumOutput {
		result.CompactBudget = claudeReactiveCompactBudgetObservation{Applied: true, OriginalMaxTokens: p.document.maxTokens(), BudgetedMaxTokens: policy.MaximumOutput}
		p.document.setMaxTokens(policy.MaximumOutput)
	}
	estimated, method := p.document.estimateInputTokens()
	budgetedOutput, adapted := adaptClaudeOutputBudget(policy, estimated, p.document.maxTokens())
	if !countTokens && adapted {
		result.AdaptiveBudget = claudeAdaptiveOutputBudgetObservation{Applied: true, OriginalMaxTokens: p.document.maxTokens(), BudgetedMaxTokens: budgetedOutput, EstimatedInput: estimated, Method: method}
		p.document.setMaxTokens(budgetedOutput)
	}
	if errTransition := p.transition(claudePhaseBudgeted); errTransition != nil {
		result.Err = errTransition
		return result
	}

	estimated, method = p.document.estimateInputTokens()
	result.Pressure = claudeContextPressureResult{
		EstimatedInput: estimated, ReservedOutput: p.document.maxTokens(), EffectiveWindow: policy.EffectiveWindow,
		SafetyMargin: policy.SafetyMargin, Method: method,
	}
	result.Pressure.Overflow = !countTokens && estimated+result.Pressure.ReservedOutput+policy.SafetyMargin > policy.EffectiveWindow
	if errTransition := p.transition(claudePhasePreflighted); errTransition != nil {
		result.Err = errTransition
		return result
	}
	if errTransition := p.transition(claudePhaseRouted); errTransition != nil {
		result.Err = errTransition
		return result
	}
	result.Body, result.Err = p.document.bytes()
	result.Model, result.Kind = p.document.model(), p.kind
	if result.Err != nil {
		return result
	}
	if result.Pressure.Overflow {
		p.phase = claudePhaseRejected
		result.Rejected = true
		return result
	}
	p.phase = claudePhaseDispatched
	return result
}

func isClaudeReactiveCompactDocument(document *claudeRequestDocument) bool {
	if document == nil {
		return false
	}
	messages, _ := document.root["messages"].([]any)
	if len(messages) == 0 {
		return false
	}
	last, _ := messages[len(messages)-1].(map[string]any)
	return stringValue(last["role"]) == "user" && isClaudeReactiveCompactPrompt(claudeMessageText(last["content"]))
}
