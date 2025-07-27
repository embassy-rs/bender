package main

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sqlbunny/errors"
)

type Directive struct {
	Args       []string
	Conditions []DirectiveCondition
}

type DirectiveCondition struct {
	Key   string
	Op    string
	Value string
}

func (c *DirectiveCondition) matches(attributes map[string]string) bool {
	switch c.Op {
	case "=":
		return attributes[c.Key] == c.Value
	case "!=":
		return attributes[c.Key] != c.Value
	case "~=":
		ok, err := regexp.MatchString(fmt.Sprintf("^%s$", c.Value), attributes[c.Key])
		if err != nil {
			log.Printf("warning: invalid regexp in condition '%s': %v", c.Value, err)
			return false
		}
		return ok
	case "!~=":
		ok, err := regexp.MatchString(fmt.Sprintf("^%s$", c.Value), attributes[c.Key])
		if err != nil {
			log.Printf("warning: invalid regexp in condition '%s': %v", c.Value, err)
			return false
		}
		return !ok
	default:
		panic("unreachable")
	}
}

func parseDirective(src string) (*Directive, error) {
	src = strings.TrimSpace(src)
	whitespace := regexp.MustCompile("^[ \t\n]+")
	item := "(\"(?:\\\\.|[^\\\\\\\"])*\"|[^ !~=;\t\n\"\\\\]*)"
	condition := regexp.MustCompile("^" + item + "(=|!=|~=|!~=)" + item)
	arg := regexp.MustCompile("^" + item)

	t := []byte(src)

	res := Directive{
		Args:       []string{},
		Conditions: []DirectiveCondition{},
	}

	for len(t) > 0 {
		if m := whitespace.FindSubmatch(t); m != nil {
			t = t[len(m[0]):]
		} else if m := condition.FindSubmatch(t); m != nil {
			key, err := unstring(string(m[1]))
			if err != nil {
				return nil, err
			}
			val, err := unstring(string(m[3]))
			if err != nil {
				return nil, err
			}

			res.Conditions = append(res.Conditions, DirectiveCondition{
				Key:   key,
				Op:    string(m[2]),
				Value: val,
			})

			t = t[len(m[0]):]
		} else if m := arg.FindSubmatch(t); m != nil {
			if len(res.Conditions) > 0 {
				return nil, errors.Errorf("positional argument after condition argument: %s", t)
			}
			arg, err := unstring(string(m[1]))
			if err != nil {
				return nil, err
			}

			res.Args = append(res.Args, arg)
			t = t[len(m[0]):]
		} else {
			return nil, errors.Errorf("unknown: %s", t)
		}
	}

	return &res, nil
}

// Parse backslash escapes.
func unstring(s string) (string, error) {
	if len(s) == 0 || s[0] != '"' {
		return s, nil
	}

	out := []byte{}
	for i := 1; i+1 < len(s); i++ {
		if s[i] == '\\' {
			if i == len(s)-1 {
				return "", errors.Errorf("invalid escape sequence: %s", s)
			}
			i++
			switch s[i] {
			case '\\':
				out = append(out, '\\')
			case '"':
				out = append(out, '"')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			default:
				return "", errors.Errorf("invalid escape sequence: \\%c", s[i])
			}
		} else {
			out = append(out, s[i])
		}
	}
	return string(out), nil
}

// DedupMode represents the job deduplication mode
type DedupMode int

const (
	DedupNone    DedupMode = iota // No deduplication
	DedupDequeue                  // Remove queued duplicates, wait for running duplicates
	DedupKill                     // Remove queued duplicates and kill running duplicates
)

func (d DedupMode) String() string {
	switch d {
	case DedupNone:
		return "none"
	case DedupDequeue:
		return "dequeue"
	case DedupKill:
		return "kill"
	default:
		return "none"
	}
}

// ParseDedupMode converts a string to DedupMode
func ParseDedupMode(s string) (DedupMode, error) {
	switch s {
	case "none":
		return DedupNone, nil
	case "dequeue":
		return DedupDequeue, nil
	case "kill":
		return DedupKill, nil
	default:
		return DedupNone, fmt.Errorf("invalid dedup mode: %s", s)
	}
}

type Meta struct {
	Events          []MetaEvent
	Priority        int
	Dedup           DedupMode     // Deduplication mode
	Cooldown        time.Duration // Cooldown duration before job can start
	Permissions     map[string]string
	PermissionRepos []string
}

type MetaEvent struct {
	Event      string
	Conditions []DirectiveCondition
}

func parseMeta(content string) (*Meta, error) {
	res := Meta{
		Events:          []MetaEvent{},
		Dedup:           DedupNone, // Default deduplication mode
		Permissions:     map[string]string{},
		PermissionRepos: []string{},
	}

	lineNum := 0
	for _, line := range strings.Split(content, "\n") {
		lineNum++

		directiveStr, ok := strings.CutPrefix(line, "##")
		if !ok {
			continue
		}

		directive, err := parseDirective(directiveStr)
		if err != nil {
			return nil, errors.Errorf("line %d: %s", lineNum, err)
		}

		if len(directive.Args) == 0 {
			return nil, errors.Errorf("line %d: no arguments", lineNum)
		}

		switch directive.Args[0] {
		case "on":
			if len(directive.Args) != 2 {
				return nil, errors.Errorf("line %d: 'on' directive must have exactly one argument", lineNum)
			}

			event := MetaEvent{
				Event:      directive.Args[1],
				Conditions: directive.Conditions,
			}

			res.Events = append(res.Events, event)
		case "permission":
			if len(directive.Args) != 3 {
				return nil, errors.Errorf("line %d: 'permission' directive must have exactly two arguments", lineNum)
			}
			if len(directive.Conditions) != 0 {
				return nil, errors.Errorf("line %d: 'permission' directive cannot have conditions", lineNum)
			}

			res.Permissions[directive.Args[1]] = directive.Args[2]
		case "permission_repo":
			if len(directive.Args) != 2 {
				return nil, errors.Errorf("line %d: 'permission_repo' directive must have exactly one argument", lineNum)
			}
			if len(directive.Conditions) != 0 {
				return nil, errors.Errorf("line %d: 'permission_repo' directive cannot have conditions", lineNum)
			}

			res.PermissionRepos = append(res.PermissionRepos, directive.Args[1])
		case "priority":
			if len(directive.Args) != 2 {
				return nil, errors.Errorf("line %d: 'priority' directive must have exactly one argument", lineNum)
			}
			if len(directive.Conditions) != 0 {
				return nil, errors.Errorf("line %d: 'priority' directive cannot have conditions", lineNum)
			}

			priority, err := strconv.Atoi(directive.Args[1])
			if err != nil {
				return nil, errors.Errorf("line %d: 'priority' must be a valid integer: %v", lineNum, err)
			}
			res.Priority = priority
		case "dedup":
			if len(directive.Args) != 2 {
				return nil, errors.Errorf("line %d: 'dedup' directive must have exactly one argument", lineNum)
			}
			if len(directive.Conditions) != 0 {
				return nil, errors.Errorf("line %d: 'dedup' directive cannot have conditions", lineNum)
			}

			dedup := directive.Args[1]
			if dedup != "none" && dedup != "dequeue" && dedup != "kill" {
				return nil, errors.Errorf("line %d: 'dedup' must be 'none', 'dequeue', or 'kill', got '%s'", lineNum, dedup)
			}
			dedupMode, err := ParseDedupMode(dedup)
			if err != nil {
				return nil, errors.Errorf("line %d: %v", lineNum, err)
			}
			res.Dedup = dedupMode
		case "cooldown":
			if len(directive.Args) != 2 {
				return nil, errors.Errorf("line %d: 'cooldown' directive must have exactly one argument", lineNum)
			}
			if len(directive.Conditions) != 0 {
				return nil, errors.Errorf("line %d: 'cooldown' directive cannot have conditions", lineNum)
			}

			cooldown, err := time.ParseDuration(directive.Args[1])
			if err != nil {
				return nil, errors.Errorf("line %d: 'cooldown' must be a valid duration: %v", lineNum, err)
			}
			res.Cooldown = cooldown
		default:
			return nil, errors.Errorf("line %d: unknown directive '%s'", lineNum, directive.Args[0])
		}
	}

	return &res, nil
}
