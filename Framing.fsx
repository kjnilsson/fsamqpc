open System.Net.Sockets

[<AutoOpen>]
module Pervasives =
    open System
    module Option =
        let protect f =
            try f() |> Some with _ -> None

    let (</>) a b = System.IO.Path.Combine(a,  b)
    let asLongLong (data: byte[]) offset =
        let data = data.[offset .. offset + 7]
        BitConverter.ToUInt64(Array.rev data, 0)//network byte order
    let asLong (data: byte[]) offset =
        let data = data.[offset .. offset + 3]
        BitConverter.ToUInt32(Array.rev data, 0)//network byte order
    let asShort (data: byte[]) offset =
        let data = data.[offset .. offset + 1]
        BitConverter.ToUInt16(Array.rev data, 0)
    let toShort (v: UInt16) =
        BitConverter.GetBytes v |> Array.rev
    let toLong (v: UInt32) =
        BitConverter.GetBytes v |> Array.rev

    let (|LongLong|_|) (data: byte[]) =
        Option.protect (fun() -> asLongLong data 0)

    let (|Long|_|) (data: byte[]) =
        Option.protect (fun() -> asLong data 0)

    let (|Short|_|) (data: byte[]) =
        Option.protect (fun() -> asShort data 0)

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
                       yield! toShort c
                       yield! pl.Length |> uint32 |> toLong |]
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
    [| yield! toLong (uint32 str.Length)
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


//gen
module Casing =
    open System.Text
    let (|ToUpper|) = System.Char.ToUpper
    let rec private upper (sb: StringBuilder) chars =
        match chars with
        | [] -> sb.ToString()
        | ToUpper c :: rest -> 
            c |> sb.Append |> ignore
            next sb rest
    and next sb chars =
        match chars with
        | [] -> sb.ToString()
        | '-' :: rest -> upper sb rest
        | c :: rest ->
            sb.Append c |> ignore
            next sb rest
    let pascal (s: string) =
        let sb = new StringBuilder()
        s.ToCharArray()
        |> Array.toList
        |> upper sb
    let camel (s: string) =
        let sb = new StringBuilder()
        s.ToCharArray()
        |> Array.toList
        |> next sb
        
Casing.pascal "start-ok"
Casing.camel "start-ok"++

type GenType =
    | Bit
    | Octet
    | Short
    | Long
    | LongLong
    | ShortStr
    | LongStr
    | Timestamp
    | Table
    static member parse =
        function
        | "bit" -> Bit
        | "octet" -> Octet
        | "short" -> Short
        | "long" -> Long
        | "longlong" -> LongLong
        | "shortstr" -> ShortStr
        | "longstr"-> LongStr
        | "timestamp" -> Timestamp
        | "table" -> Table
        | _ -> failwith "invalid gentype"

type GenMethod =
    { Name: string
      Index: int
      Fields: string * GenType }
type GenClass =
    { Name: string
      Index: int
      Properties: (string * GenType) list
      Methods: GenMethod list }

#r "System.Xml.Linq"
open System.Xml.Linq
open System.IO
File.ReadAllText (__SOURCE_DIRECTORY__ </> "amqp0-9-1.stripped.xml")  |> XElement.Parse