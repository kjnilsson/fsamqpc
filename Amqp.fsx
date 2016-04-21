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
        

