package whitewater

import "context"

// SubscriptionMemberClient is one cooperating member of a named
// Subscription. Delivery is at-least-once and fenced at commit: fetch
// under a held lease, then ack the page's Cursor/Positions verbatim.
type SubscriptionMemberClient struct {
	transport    *transport
	subscription string
}

func (m *SubscriptionMemberClient) leaseBody(
	member Joined, workID string, leaseEpoch *int64,
) map[string]any {
	body := map[string]any{
		"subscription": m.subscription,
		"request_id":   newUUID(),
		"member_id":    member.MemberID,
		"member_epoch": member.MemberEpoch,
		"work_id":      workID,
	}
	if leaseEpoch != nil {
		body["lease_epoch"] = *leaseEpoch
	}
	return body
}

// Join joins the Subscription; returns the fencing member epoch.
func (m *SubscriptionMemberClient) Join(
	ctx context.Context, memberID string,
) (Joined, error) {
	raw, err := m.transport.post(ctx, "/v1/subscriptions/members/join", map[string]any{
		"subscription": m.subscription,
		"request_id":   newUUID(),
		"member_id":    memberID,
	})
	if err != nil {
		return Joined{}, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return Joined{}, err
	}
	return Joined{
		MemberID:       memberID,
		MemberEpoch:    intField(body, "member_epoch"),
		OwnershipEpoch: intField(body, "ownership_epoch"),
		Tick:           intField(body, "tick"),
	}, nil
}

func (m *SubscriptionMemberClient) lease(
	ctx context.Context, path string,
	member Joined, workID string, leaseEpoch *int64, leaseTicks int,
) (WorkLease, error) {
	body := m.leaseBody(member, workID, leaseEpoch)
	if leaseTicks > 0 {
		body["lease_ticks"] = leaseTicks
	}
	raw, err := m.transport.post(ctx, path, body)
	if err != nil {
		return WorkLease{}, err
	}
	decoded, err := decodeBody(raw)
	if err != nil {
		return WorkLease{}, err
	}
	lease, ok := decoded["lease"].(map[string]any)
	if !ok {
		return WorkLease{}, errorOf(
			KindConflict, 409, "lease.refused", "work claim was not granted")
	}
	return WorkLease{
		WorkID:        stringField(lease, "work_id"),
		MemberID:      stringField(lease, "member_id"),
		MemberEpoch:   intField(lease, "member_epoch"),
		LeaseEpoch:    intField(lease, "lease_epoch"),
		ExpiresAtTick: intField(lease, "expires_at_tick"),
	}, nil
}

// Claim claims a work item under a bounded lease.
func (m *SubscriptionMemberClient) Claim(
	ctx context.Context, member Joined, workID string, leaseTicks int,
) (WorkLease, error) {
	return m.lease(ctx, "/v1/subscriptions/members/claim",
		member, workID, nil, leaseTicks)
}

// Renew extends an outstanding lease.
func (m *SubscriptionMemberClient) Renew(
	ctx context.Context, member Joined, lease WorkLease, leaseTicks int,
) (WorkLease, error) {
	return m.lease(ctx, "/v1/subscriptions/members/renew",
		member, lease.WorkID, &lease.LeaseEpoch, leaseTicks)
}

// Release releases a held lease without acknowledging.
func (m *SubscriptionMemberClient) Release(
	ctx context.Context, member Joined, lease WorkLease,
) error {
	_, err := m.transport.post(ctx, "/v1/subscriptions/members/release",
		m.leaseBody(member, lease.WorkID, &lease.LeaseEpoch))
	return err
}

// Fetch returns a bounded page after the shared frontier.
func (m *SubscriptionMemberClient) Fetch(
	ctx context.Context, member Joined, lease WorkLease, limit int,
) (MemberPage, error) {
	body := m.leaseBody(member, lease.WorkID, &lease.LeaseEpoch)
	if limit > 0 {
		body["limit"] = limit
	}
	raw, err := m.transport.post(ctx, "/v1/subscriptions/members/fetch", body)
	if err != nil {
		return MemberPage{}, err
	}
	decoded, err := decodeBody(raw)
	if err != nil {
		return MemberPage{}, err
	}
	return decodeMemberPage(decoded), nil
}

// Ack commits the fetched frontier; overlapping members are fenced here.
func (m *SubscriptionMemberClient) Ack(
	ctx context.Context, member Joined, lease WorkLease, page MemberPage,
) error {
	body := m.leaseBody(member, lease.WorkID, &lease.LeaseEpoch)
	if page.Cursor != nil {
		body["cursor"] = string(*page.Cursor)
	}
	positions := make(map[string]any, len(page.Positions))
	for range_, cursor := range page.Positions {
		positions[range_] = cursor
	}
	body["positions"] = positions
	_, err := m.transport.post(ctx, "/v1/subscriptions/members/ack", body)
	return err
}
