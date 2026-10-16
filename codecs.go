package whitewater

import (
	"encoding/json"
	"fmt"
)

// Codec is a bidirectional object ↔ wire-bytes serialization — the
// wire always carries raw bytes; a Codec keeps that plumbing inside
// the SDK so application code works with its own types:
//
//	orders := whitewater.FeedOf(client, "orders.events",
//	    whitewater.StringCodec(), whitewater.JSONCodec[Order]())
//	writer, _ := orders.Writer(ctx, "ingest")
//	writer.Append(ctx, "order-1", order)          // encoded for you
//
// StringCodec for text keys/payloads, BytesCodec for raw binary,
// JSONCodec for any encoding/json-mapped type, and FuncCodec for
// anything else (protobuf, msgpack, application framing).
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
}

type stringCodec struct{}

func (stringCodec) Encode(value string) ([]byte, error) { return []byte(value), nil }
func (stringCodec) Decode(data []byte) (string, error)  { return string(data), nil }

type bytesCodec struct{}

func (bytesCodec) Encode(value []byte) ([]byte, error) { return value, nil }
func (bytesCodec) Decode(data []byte) ([]byte, error)  { return data, nil }

type jsonCodec[T any] struct{}

func (jsonCodec[T]) Encode(value T) ([]byte, error) {
	return json.Marshal(value)
}

func (jsonCodec[T]) Decode(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

type funcCodec[T any] struct {
	encode func(T) ([]byte, error)
	decode func([]byte) (T, error)
}

func (f funcCodec[T]) Encode(value T) ([]byte, error) { return f.encode(value) }
func (f funcCodec[T]) Decode(data []byte) (T, error)  { return f.decode(data) }

// StringCodec is UTF-8 text — the common case for keys and JSON/text
// payloads.
func StringCodec() Codec[string] { return stringCodec{} }

// BytesCodec is the identity codec — raw bytes through to the wire.
func BytesCodec() Codec[[]byte] { return bytesCodec{} }

// JSONCodec maps any encoding/json-mapped type — structs round-trip
// field-for-field.
func JSONCodec[T any]() Codec[T] { return jsonCodec[T]{} }

// FuncCodec builds a codec from caller-supplied functions — protobuf,
// msgpack, application framing:
//
//	proto := whitewater.FuncCodec(
//	    func(u *User) ([]byte, error) { return proto.Marshal(u) },
//	    func(b []byte) (*User, error) {
//	        var u User
//	        return &u, proto.Unmarshal(b, &u)
//	    })
func FuncCodec[T any](
	encode func(T) ([]byte, error),
	decode func([]byte) (T, error),
) Codec[T] {
	return funcCodec[T]{encode: encode, decode: decode}
}

// CodecFor picks a codec by type: string is UTF-8, []byte is raw
// identity, everything else is JSON-mapped — so FeedFor[Order] just
// works without spelling JSONCodec[Order]().
func CodecFor[T any]() Codec[T] {
	var zero T
	switch any(zero).(type) {
	case string:
		return any(StringCodec()).(Codec[T])
	case []byte:
		return any(BytesCodec()).(Codec[T])
	default:
		return JSONCodec[T]()
	}
}

// Ptr returns a pointer to v — for optional fields like FeedEvent.Key:
// FeedEvent[string, Order]{Key: whitewater.Ptr("order-1"), Value: o}.
func Ptr[T any](v T) *T { return &v }

func encodeOrNil[T any](codec Codec[T], value *T) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return codec.Encode(*value)
}

func decodeOrNil[T any](codec Codec[T], data []byte) (*T, error) {
	if data == nil {
		return nil, nil
	}
	value, err := codec.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("decode through codec: %w", err)
	}
	return &value, nil
}
