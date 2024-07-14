package enums

type PractitionerStatus string

const (
	PractitionerStatusActive     PractitionerStatus = "active"
	PractitionerStatusInactive   PractitionerStatus = "inactive"
	PractitionerStatusSuspended  PractitionerStatus = "suspended"
	PractitionerStatusTerminated PractitionerStatus = "terminated"
	PractitionerStatusDeleted    PractitionerStatus = "deleted"
	PractitionerStatusPending    PractitionerStatus = "pending"
	PractitionerStatusUnverified PractitionerStatus = "unverified"
)
