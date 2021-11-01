package statuses

type Status string

const (
	Pending   Status = "pending"
	Processed Status = "processed"
	Error     Status = "error"
)
