open System.Net.Sockets
#if INTERACTIVE
#load "Pervasives.fsx"
#endif

[<AutoOpen>]
module AMQP =
    type AMQPOctet = byte

    let AMQPProtocolHeader = 
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
            let chan = asShort header 1
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

type Bit = bool
type Octet = byte
type Short = byte
type LongStr = LongStr of byte []
    with 
        override x.ToString () =
            let (LongStr data) = x
            System.Text.Encoding.ASCII.GetString data 
            
type Field =
    | Bit of Bit
    | Octet of byte
    | Short of uint16
    | Long of uint32
    | LongLong of uint64
    | ShortStr of string
    | LongStrField of byte []
    | Timestamp of System.DateTime
    | Table of Table

//and Table = Map<string, Field>
and Table = LongStr //TEMP

let readOctet (data: byte[]) offset =
    offset + 1, data.[offset]

let readLongStr (data: byte[]) offset =
    let len = asLong data offset |> int
    offset + 4 + len, LongStr (data.[offset + 3 .. offset + 3 + len])
    
let writeLongStr (LongStr str) =
    [| yield! fromLong (uint32 str.Length)
       yield! str |]

type StartData =
    { VersionMajor: byte
      VersionMinor: byte
      ServerProperties: LongStr //TODO: for now
      Mechanisms: LongStr //not null
      Locales: LongStr } //not null
    static member parse (payload: byte []) =
        let off = 0
        let off, versionMajor = readOctet payload off 
        let off, versionMinor = readOctet payload off 
        let off, serverProperties = readLongStr payload off
        let off, mechanisms = readLongStr payload off
        let off, locales = readLongStr payload off
        { VersionMajor = versionMajor 
          VersionMinor = versionMinor 
          ServerProperties = serverProperties
          Mechanisms = mechanisms
          Locales = locales }
    static member pickle (x: StartData) = [| 
        yield x.VersionMajor
        yield x.VersionMinor
        yield! writeLongStr x.ServerProperties
        yield! writeLongStr x.Mechanisms
        yield! writeLongStr x.Locales 
    |]

type AMQPConnection =
    | Start of StartData


let parseMethod (payload: byte []) =
    match asShort (payload.[0..1]) 0,  asShort (payload.[2..3]) 0 with
    | 10us, 10us -> StartData.parse payload.[4 ..] |> Start
    | x -> failwith (sprintf "%A not implemented" x)


async {
    let sock = new TcpClient("localhost", 5672)
    let s = sock.GetStream()
    do! s.AsyncWrite AMQPProtocolHeader
    let! f = Frame.read s
    match f with
    | FrameOk (Method (0us, payload)) ->
        parseMethod payload |> printfn "%A"
    | _ -> failwith "oh noes" }
|> Async.RunSynchronously


