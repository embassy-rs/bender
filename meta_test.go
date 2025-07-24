package main

import (
	"reflect"
	"testing"
)

func TestParseMeta(t *testing.T) {
	contents := `
#!/bin/bash
# on push branch=nope
## on push branch=main
## on push branch=wtflol
## on push branch~=gh-readonly-queue/main/.*
## on pull_request
on alalalalalaaaaa
`
	want := &Meta{
		Events: []MetaEvent{
			{
				Event:      "push",
				Conditions: []DirectiveCondition{{Key: "branch", Op: "=", Value: "main"}},
			},
			{
				Event:      "push",
				Conditions: []DirectiveCondition{{Key: "branch", Op: "=", Value: "wtflol"}},
			},
			{
				Event:      "push",
				Conditions: []DirectiveCondition{{Key: "branch", Op: "~=", Value: "gh-readonly-queue/main/.*"}},
			},
			{
				Event:      "pull_request",
				Conditions: []DirectiveCondition{},
			},
		},
		Priority:        0,         // Default priority when not specified
		Dedup:           DedupNone, // Default dedup mode when not specified
		Permissions:     map[string]string{},
		PermissionRepos: []string{},
	}

	got, err := parseMeta(contents)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParseDirective(t *testing.T) {
	tests := []struct {
		in      string
		want    *Directive
		wantErr bool
	}{
		{
			in: "on push branch=main",
			want: &Directive{
				Args: []string{"on", "push"},
				Conditions: []DirectiveCondition{
					{Key: "branch", Op: "=", Value: "main"},
				},
			},
		},

		{
			in: "on push branch!=main branch!=foo",
			want: &Directive{
				Args: []string{"on", "push"},
				Conditions: []DirectiveCondition{
					{Key: "branch", Op: "!=", Value: "main"},
					{Key: "branch", Op: "!=", Value: "foo"},
				},
			}},
		{
			in:      "on push branch=main branch=foo bar",
			wantErr: true,
		},
		{
			in: "on push branch=main foo~=foo bar!~=baz",
			want: &Directive{
				Args: []string{"on", "push"},
				Conditions: []DirectiveCondition{
					{Key: "branch", Op: "=", Value: "main"},
					{Key: "foo", Op: "~=", Value: "foo"},
					{Key: "bar", Op: "!~=", Value: "baz"},
				},
			},
		},
		{
			in: "on push branch=\"foo\"",
			want: &Directive{
				Args: []string{"on", "push"},
				Conditions: []DirectiveCondition{
					{Key: "branch", Op: "=", Value: "foo"},
				},
			},
		},
		{
			in: "on push branch=\"\\\"\"",
			want: &Directive{
				Args: []string{"on", "push"},
				Conditions: []DirectiveCondition{
					{Key: "branch", Op: "=", Value: "\""},
				},
			},
		},
		{
			in: "on push branch=\"\\\\\\n\" branch!=asdf",
			want: &Directive{
				Args: []string{"on", "push"},
				Conditions: []DirectiveCondition{
					{Key: "branch", Op: "=", Value: "\\\n"},
					{Key: "branch", Op: "!=", Value: "asdf"},
				},
			},
		},
	}

	for _, test := range tests {
		got, err := parseDirective(test.in)
		if test.wantErr {
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			continue
		}

		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("got %v, want %v", got, test.want)
		}
	}

}

func TestParsePriority(t *testing.T) {
	content := `#!/bin/bash
## on push branch=main
## priority 10
echo "test"`

	meta, err := parseMeta(content)
	if err != nil {
		t.Fatalf("Failed to parse meta: %v", err)
	}

	if meta.Priority != 10 {
		t.Errorf("Expected priority 10, got %d", meta.Priority)
	}

	if len(meta.Events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(meta.Events))
	}

	if meta.Events[0].Event != "push" {
		t.Errorf("Expected event 'push', got '%s'", meta.Events[0].Event)
	}
}

func TestParseDedupMode(t *testing.T) {
	tests := []struct {
		input    string
		expected DedupMode
		hasError bool
	}{
		{"none", DedupNone, false},
		{"dequeue", DedupDequeue, false},
		{"kill", DedupKill, false},
		{"invalid", DedupNone, true},
		{"", DedupNone, true},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result, err := ParseDedupMode(test.input)

			if test.hasError {
				if err == nil {
					t.Errorf("Expected error for input %q, but got none", test.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input %q: %v", test.input, err)
				}
				if result != test.expected {
					t.Errorf("Expected %v for input %q, got %v", test.expected, test.input, result)
				}
			}
		})
	}
}

func TestDedupModeString(t *testing.T) {
	tests := []struct {
		mode     DedupMode
		expected string
	}{
		{DedupNone, "none"},
		{DedupDequeue, "dequeue"},
		{DedupKill, "kill"},
		{DedupMode(999), "none"}, // Invalid mode should default to "none"
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			result := test.mode.String()
			if result != test.expected {
				t.Errorf("Expected %q for mode %v, got %q", test.expected, test.mode, result)
			}
		})
	}
}

func TestParseDedupInMeta(t *testing.T) {
	contents := `## priority 5
## dedup kill
## on push branch=main
## permission repo read
`

	meta, err := parseMeta(contents)
	if err != nil {
		t.Fatal(err)
	}

	if meta.Dedup != DedupKill {
		t.Errorf("Expected DedupKill, got %v", meta.Dedup)
	}
	if meta.Priority != 5 {
		t.Errorf("Expected priority 5, got %d", meta.Priority)
	}
}
