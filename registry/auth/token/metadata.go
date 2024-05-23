package token

import "context"

const (
	// EgressProjectIdKey is used to get the GitLab project ID present in a context for egress instrumentation
	EgressProjectIdKey = "meta.project_id"
	// EgressNamespaceIdKey is used to get the GitLab root namespace ID present in a context for egress instrumentation
	EgressNamespaceIdKey = "meta.namespace_id"
)

type egressMetadataContext struct {
	context.Context
	namespaceID int64
	projectID   int64
}

func (ec egressMetadataContext) Value(key any) any {
	switch key {
	case EgressNamespaceIdKey:
		return ec.namespaceID
	case EgressProjectIdKey:
		return ec.projectID
	}

	return ec.Context.Value(key)
}

// WithEgressMetadata returns a context with GitLab metadata related to egress instrumentation.
func WithEgressMetadata(ctx context.Context, accesses []*ResourceActions) context.Context {
	var namespaceID, projectID int64

	// For most cases, there will only be one access object, as most requests only target one repository.
	// Cross-repository blob mount requests are the exception, as we always have two repositories (one to `pull` from
	// and another to `push` to), and hence two access objects, each with their own project/namespace IDs. As these
	// attributes are related to *egress* transfer instrumentation, here we only pick the ones related to the `pull`
	// action (if any).
	for _, access := range accesses {
		if access.Meta != nil {
			for _, action := range access.Actions {
				if action == "pull" {
					namespaceID = access.Meta.NamespaceID
					projectID = access.Meta.ProjectID
					break
				}
			}
		}
	}

	return egressMetadataContext{
		Context:     ctx,
		namespaceID: namespaceID,
		projectID:   projectID,
	}
}
