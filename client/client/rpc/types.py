class Type(object):
    STOP = 0
    VOID = 1
    BOOL = 2
    BYTE = 3
    FLOAT = 4
    I16 = 5
    I32 = 6
    STRING = 7
    STRUCT = 8
    LIST = 9

    _VALUES_TO_NAMES = (
        "STOP",
        "VOID",
        "BOOL",
        "BYTE",
        "FLOAT",
        "I16",
        "I32",
        "STRING",
        "STRUCT",
        "LIST",
    )


class MessageType(object):
    CALL = 1
    REPLY = 2
    EXCEPTION = 3
