package chunkdb

import (
	"errors"
	"testing"
)

func TestEveryServerCodeDecodes(t *testing.T) {
	for code, class := range map[string]RetryClass{
		CodeProtocol: RetryFatal, CodeBadRequest: RetryFatal, CodeAuthRequired: RetryFatal, CodeAuthFailed: RetryFatal,
		CodePermissionDenied: RetryFatal, CodeSyntax: RetryFatal, CodeInvalidArgument: RetryFatal, CodeOutOfRange: RetryFatal,
		CodeNoTable: RetryFatal, CodeTableExists: RetryFatal, CodeUnknownCommand: RetryFatal, CodeCheckFailed: RetryFatal,
		CodeMigrationMismatch: RetryFatal, CodeSlotBusy: RetryFatal, CodeSlotLost: RetryFatal, CodeResyncRequired: RetryFatal,
		CodeCancelled: RetryFatal, CodeTimeout: RetryFatal, CodeKilled: RetryFatal, CodeInternal: RetryFatal,
		CodeVersionMismatch: RetryReread, CodeConditionFailed: RetryReread, CodeSchemaMismatch: RetryReread,
		CodeConflict: RetryRerun,
		CodeBusy:     RetryBackoff, CodeDiskFull: RetryBackoff, CodeRateLimited: RetryBackoff,
		CodeOutcomeUnknown: RetryUnknownOutcome,
	} {
		t.Run(code, func(t *testing.T) {
			err := replyError(PhaseResponse, "SHOW TABLES", Reply{Kind: ReplyError, Code: code, Message: "reason=why current=3 detail"})
			var typed *Error
			if !errors.As(err, &typed) || typed.ServerCode != code || typed.RetryClass != class || typed.Metadata.Reason != "why" {
				t.Fatalf("%v, %+v", err, typed)
			}
			if !IsCode(err, code) || IsCode(err, "OTHER") || !errors.Is(err, ErrServer) {
				t.Fatal("code predicates", err)
			}
			if wantAuth := code == CodeAuthRequired || code == CodeAuthFailed; errors.Is(err, ErrAuth) != wantAuth {
				t.Fatal("ErrAuth", code)
			}
		})
	}
	if IsCode(nil, CodeBusy) || IsCode(errors.New(CodeBusy), CodeBusy) {
		t.Fatal("IsCode matched a non-server error")
	}
}
