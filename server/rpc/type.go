package rpc

// Type is type constants for binary serialization/deserialization
type Type byte

const (
	Stop   Type = 0
	Void   Type = 1
	Bool   Type = 2
	Byte   Type = 3
	Float  Type = 4
	I16    Type = 5
	I32    Type = 6
	String Type = 7
	Struct Type = 8
	List   Type = 9
)

var typeNames = map[Type]string{
	Stop:  "STOP",
	Void:  "VOID",
	Bool:  "BOOL",
	Byte:  "BYTE",
	Float: "FLOAT",
	I16:   "I16",
	I32:   "I32",
	// I64:    "I64",
	String: "STRING",
	Struct: "STRUCT",
	// Map:    "MAP",
	// Set:    "SET",
	List: "LIST",
	// Utf8:   "UTF8",
	// Utf16:  "UTF16",
}

func (p Type) String() string {
	if s, ok := typeNames[p]; ok {
		return s
	}
	return "Unknown"
}

type MessageType byte

const (
	Call      MessageType = 1
	Reply     MessageType = 2
	Exception MessageType = 3
)
