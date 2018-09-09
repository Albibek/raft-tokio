@0x82ecb5c0e24516e6;

struct Handshake {
    union {
        hello @0 :UInt64;
        ehlo @1 :UInt64;
    }
}

