module Amqp
#if INTERACTIVE
#load "Pervasives.fsx"
#endif
type Bit = bool
type Octet = byte
type Short = uint16
type Long = uint32
type LongLong = uint64
type ShortStr = string
type Timestamp = uint64 
type LongStr = byte []

type Field =
    | BitField of Bit
    | OctetField of byte
    | ShortField of uint16
    | LongField of uint32
    | LongLongField of uint64
    | ShortStrField of string
    | LongStrField of byte []
    | TimestampField of System.DateTime
    | TableField of Table

//and Table = Map<string, Field>
and Table = LongStr //TEMP

let readOctet (data: byte[]) offset =
    offset + 1, data.[offset]

let readShort (data: byte[]) offset =
    offset + 2, asShort data offset

let readLong (data: byte[]) offset =
    offset + 4, asLong data offset

let readLongLong (data: byte[]) offset =
    offset + 8, asLongLong data offset

let readLongStr (data: byte[]) offset =
    let len = asLong data offset |> int
    offset + 4 + len, (data.[offset + 3 .. offset + 3 + len])

let readTable = readLongStr //TODO
let readTimestamp = readLongLong //TODO
    
let readShortStr (data: byte[]) offset =
    let len = data.[0] |> int
    offset + 2 + len, (System.Text.Encoding.UTF8.GetString(data.[offset + 1 .. offset + 1 + len]))

let writeOctet (o: Octet) =
    [| o |]

let writeShort (o: Short) =
    [| yield! fromShort o |]

let writeLong (o: Long) =
    [| yield! fromLong o |]

let writeLongLong (o: LongLong) =
    [| yield! fromLongLong o |]

let writeTimestamp = writeLongLong

let writeLongStr (str: LongStr) =
    [| yield! fromLong (uint32 str.Length)
       yield! str |]

let writeTable = writeLongStr

let writeShortStr (str: ShortStr) =
    [| yield (byte str.Length)
       yield! (System.Text.Encoding.UTF8.GetBytes str) |]

let readBit (pl: byte []) off bit = 
    let x = pl.[off]
    let mask = 1uy <<< bit
    let result = x &&& mask <> 0uy
    let nextOff, nextBit =
        if bit = 7 then off + 1, 0
        else off, bit + 1
    nextBit, nextOff, result

