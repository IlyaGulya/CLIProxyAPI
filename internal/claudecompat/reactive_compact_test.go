package claudecompat

import "testing"

func TestIsReactiveCompactPrompt(t *testing.T) {
	t.Parallel()

	prompt := "Your task is to CREATE A DETAILED SUMMARY OF THE CONVERSATION SO FAR. " +
		"Before providing your final summary, WRAP YOUR ANALYSIS IN <ANALYSIS> TAGS. " +
		"Your entire response must be an <ANALYSIS> BLOCK FOLLOWED BY A <SUMMARY> BLOCK."
	if !IsReactiveCompactPrompt(prompt) {
		t.Fatal("reactive compact prompt was not detected")
	}
	if IsReactiveCompactPrompt("Create an ordinary summary of this document") {
		t.Fatal("ordinary summary was detected as reactive compact")
	}
}
