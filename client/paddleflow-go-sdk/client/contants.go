package client

const (
	GET    = "GET"
	PUT    = "PUT"
	POST   = "POST"
	DELETE = "DELETE"

	HeaderKeyRequestID = "x-pf-request-id"
	HeaderAuthKey      = "x-pf-authorization"
	HeaderClientIDKey  = "x-pf-client-id"

	DefaultConnectionTimeout = 10

	HeartBeatSignal = "heartbeat"

	PrefixConnection = "conn"
)
