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
    | OctetField of Octet
    | ShortField of Short
    | LongField of Long
    | LongLongField of Long
    | ShortStrField of ShortStr
    | LongStrField of LongStr 
    | TimestampField of Timestamp
    | TableField of Table
//and Table = Map<string, Field>
and Table = LongStr //TEMP

let ProtocolHeader = 
    [| yield! "AMQP"B; yield! [| 0uy;0uy;9uy;1uy |] |]

let AMQPFrameEnd = [|0uy; 206uy|]

let (|FrameEnd|_|) =
    function
    | 206 -> Some ()
    | _ -> None

type FrameReadResult =
    | FrameOk of Frame
    | FrameErr of string
and Frame =
    | Method of chan: uint16 * payload: byte []
    | Header of chan: uint16 * payload: byte []
    | Body of chan: uint16 * payload: byte []
    | Heartbeat
    member x.Type () =
        match x with
        | Method _ -> 1uy
        | Header _ -> 2uy
        | Body _ -> 3uy
        | Heartbeat -> 4uy
    static member read (s: System.IO.Stream) =
        async {
            let! header = s.AsyncRead 7
            let t = header.[0]
            let chan = toShort header 1
            let size = asLong header 3 //TODO: casting unsigned to signed!
            let! payload = s.AsyncRead (int size)
            let trailer = s.ReadByte() 
            match trailer with
            | FrameEnd ->
                match t with
                | 1uy -> return Method (chan, payload) |> FrameOk
                | 2uy -> return Header (chan, payload) |> FrameOk
                | 3uy -> return Body (chan, payload) |> FrameOk
                | 4uy -> return Heartbeat |> FrameOk
                | _ -> return FrameErr "invalid frame-type"
            | _ -> return FrameErr "invalid frame-end" }
    static member write (s: System.IO.Stream) (frame: Frame) =
        async {
            match frame with
            | Method (c, pl) | Header (c, pl) | Body (c, pl) -> 
                let header = 
                    [| yield frame.Type() 
                       yield! fromShort c
                       yield! pl.Length |> uint32 |> fromLong |]
                do! s.AsyncWrite header
                do! s.AsyncWrite pl
            | Heartbeat ->
                let header = [|4uy;0uy;0uy;0uy;0uy;0uy;0uy|]
                do! s.AsyncWrite header
            do! s.AsyncWrite AMQPFrameEnd }

let readOctet (data: byte[]) offset =
    offset + 1, data.[offset]

let readShort (data: byte[]) offset =
    offset + 2, toShort data offset

let readLong (data: byte[]) offset =
    offset + 4, asLong data offset

let readLongLong (data: byte[]) offset =
    offset + 8, asLongLong data offset

let readLongStr (data: byte[]) offset =
    let len = asLong data offset |> int
    offset + 4 + len, (data.[offset + 3 .. offset + 3 + len])

    (*
amqp = protocol-header *amqp-unit

protocol-header = literal-AMQP protocol-id protocol-version
literal-AMQP = %d65.77.81.80 ; "AMQP"
protocol-id = %d0 ; Must be 0
protocol-version = %d0.9.1 ; 0-9-1

method = method-frame [ content ]
method-frame = %d1 frame-properties method-payload frame-end
frame-properties = channel payload-size
channel = short-uint ; Non-zero
payload-size = long-uint
method-payload = class-id method-id *amqp-field
class-id = %x00.01-%xFF.FF
method-id = %x00.01-%xFF.FF
amqp-field = BIT / OCTET
 / short-uint / long-uint / long-long-uint
 / short-string / long-string
 / timestamp
 / field-table
short-uint = 2*OCTET
long-uint = 4*OCTET
long-long-uint = 8*OCTET
short-string = OCTET *string-char ; length + content
string-char = %x01 .. %xFF
long-string = long-uint *OCTET ; length + content
timestamp = long-long-uint ; 64-bit POSIX 
field-table = long-uint *field-value-pair
field-value-pair = field-name field-value
field-name = short-string
field-value = 't' boolean
             / 'b' short-short-int
             / 'B' short-short-uint
             / 'U' short-int
             / 'u' short-uint
             / 'I' long-int
             / 'i' long-uint
             / 'L' long-long-int
             / 'l' long-long-uint
             / 'f' float
             / 'd' double
             / 'D' decimal-value
             / 's' short-string
             / 'S' long-string
             / 'A' field-array
             / 'T' timestamp
             / 'F' field-table
             / 'V'
boolean = OCTET ; 0 = FALSE, else TRUE
short-short-int = OCTET
short-short-uint = OCTET
short-int = 2*OCTET
long-int = 4*OCTET
long-long-int = 8*OCTET
float = 4*OCTET ; IEEE-754
double = 8*OCTET ; rfc1832 XDR double
decimal-value = scale long-uint
scale = OCTET ; number of decimal digits
field-array = long-int *field-value ; array of values
frame-end = %xCE
content = %d2 content-header *content-body
content-header = frame-properties header-payload frame-end
header-payload = content-class content-weight content-body-size
 property-flags property-list
content-class = OCTET
content-weight = %x00
content-body-size = long-long-uint
property-flags = 15*BIT %b0 / 15*BIT %b1 property-flags
property-list = *amqp-field
content-body = %d3 frame-properties body-payload frame-end
body-payload = *OCTET

heartbeat = %d8 %d0 %d0 frame-end
    *)
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

let writeBits (bits: bool list) : byte [] =
    bits 
    |> List.chunkBySize 8
    |> List.map (fun xs ->
        xs
        |> List.mapi (fun i x -> i,x)   
        |> List.filter snd
        |> List.fold (fun s (i, b) ->
            let m = 1uy <<< i
            s ||| m) 0uy)
    |> Seq.toArray
        

