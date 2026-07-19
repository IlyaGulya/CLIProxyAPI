package executor

func codexEnumString(index int, names []string) string {
	if index < 0 || index >= len(names) {
		return "unknown"
	}
	return names[index]
}
