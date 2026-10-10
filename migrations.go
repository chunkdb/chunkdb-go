package chunkdb

import (
	"context"
	"fmt"
	"strings"
)

// Migration is one named schema statement. Name is [a-z_][a-z0-9_]*, at most
// 63 bytes. Statement is a single CQL CREATE/ALTER/DROP TABLE, GRANT/REVOKE,
// or CREATE/DROP SLOT statement without parameters.
type Migration struct {
	Name, Statement string
}

// MigrationResult reports a completed step. Status is "applied" or "skipped".
type MigrationResult struct {
	Name, Status string
}

// MigrationError identifies the first failed step of Migrate. Index is zero
// based. Err preserves the underlying server, validation, or transport error
// for errors.Is and errors.As. Earlier results are returned alongside it.
type MigrationError struct {
	Index int
	Name  string
	Err   error
}

func (e *MigrationError) Error() string {
	return fmt.Sprintf("chunkdb: migration %d %q: %v", e.Index+1, e.Name, e.Err)
}

func (e *MigrationError) Unwrap() error { return e.Err }

// Migrate runs steps in order, stopping at the first error. Run the same list
// at every application start: the server skips names already applied with the
// same statement text and reports CONFLICT for a changed statement. Leading
// and trailing spaces/tabs are removed; interior spacing and case are kept.
// Each step is durable independently; this is not a transaction. After a
// transport failure its outcome may be unknown: retry the same named step.
func (c *Client) Migrate(ctx context.Context, steps []Migration) ([]MigrationResult, error) {
	results := make([]MigrationResult, 0, len(steps))
	for i, step := range steps {
		status, err := c.migrateStep(ctx, step)
		if err != nil {
			return results, &MigrationError{Index: i, Name: step.Name, Err: err}
		}
		results = append(results, MigrationResult{Name: step.Name, Status: status})
	}
	return results, nil
}

func (c *Client) migrateStep(ctx context.Context, step Migration) (string, error) {
	const command = "MIGRATE"
	if len(step.Name) > 63 {
		return "", requestErrorf(command, "migration name exceeds 63 bytes")
	}
	if err := checkNames(command, "migration", step.Name); err != nil {
		return "", err
	}
	statement := strings.Trim(step.Statement, " \t")
	if statement == "" || strings.ContainsAny(statement, "\r\n\x00") {
		return "", requestErrorf(command, "migration statement must be a nonempty single line without NUL")
	}
	reply, err := c.Do(ctx, "MIGRATE '"+step.Name+"' "+statement)
	if err != nil {
		return "", err
	}
	if reply.Kind != ReplySimple || (reply.Text != "applied" && reply.Text != "skipped") {
		return "", protocolErrorf(command, "expected applied or skipped simple string")
	}
	return reply.Text, nil
}

// Migrate runs Client.Migrate on one leased client for the whole list.
func (p *Pool) Migrate(ctx context.Context, steps []Migration) ([]MigrationResult, error) {
	if len(steps) == 0 {
		return []MigrationResult{}, nil
	}
	results, err := withPooledClient(ctx, p, func(ctx context.Context, c *Client) ([]MigrationResult, error) {
		return c.Migrate(ctx, steps)
	})
	if err != nil && results == nil {
		return results, &MigrationError{Index: 0, Name: steps[0].Name, Err: err}
	}
	return results, err
}
