open System
open System.Text
open System.Net.Sockets
#if INTERACTIVE
#load "Pervasives.fsx"
#load "Amqp.fsx"
#load "GenChannel.fsx"
#load "GenBasic.fsx"
#load "GenConnection.fsx"
#endif

open Amqp
open Connection

module Basic =
    open Basic
    type BasicContentHeader =
        { BodySize: LongLong
          Props: BasicPropsData }
        static member parse (payload: byte[]) =
            match readShort payload 0 with
            | _, 60us ->
                let off, size = readLongLong payload 4
                let props = BasicPropsData.parse payload
                { BodySize = size
                  Props = props }
                |> Some
            | _ -> None
        static member pickle (x: BasicContentHeader) =
            [|
                yield! writeShort 60us
                yield! writeShort 0us
                yield! BasicPropsData.pickle x.Props
            |]

    let (|BasicContentHeader|_|) = BasicContentHeader.parse


    let props () =
        { ContentType = "" 
          ContentEncoding = ""
          Headers = Map.empty
          DeliveryMode = 0uy
          Priority = 0uy
          CorrelationId = ""
          ReplyTo = ""
          Expiration = ""
          MessageId = ""
          Timestamp = System.DateTime.UtcNow |> DateTime.toUnix
          Type = ""
          UserId = ""
          AppId = ""
          ClusterId = "" }

type Connection with
    static member opn vhost =
        { VirtualHost  = vhost
          Reserved1 = ""
          Reserved2 = true }
        |> Open

type Channel.Channel with 
    static member opn () =
        Channel.Open { Channel.OpenData.Reserved1 = "" }
    static member close cid =
        { Channel.CloseData.ClassId = cid 
          Channel.CloseData.MethodId = 0us
          Channel.CloseData.ReplyCode = 0us
          Channel.CloseData.ReplyText = "" }
        |> Channel.Close 
        
        
type Ctx =
    { Socket: IO.Stream
      StartData: StartData
      TuneData: TuneData
      ChannelDeliveries: Event<Frame> }

type Cfg =
    { Vhost: string
      Creds: string * string }

let connectSock addr port =
    let sock = new TcpClient(addr, port)
    sock.NoDelay <- true
    sock.GetStream()

//TODO: better error handling
let handshake { Vhost = vhost; Creds = user, pass } (s: IO.Stream) =
    let ch0Method = Connection.pickle >> Frame.meth 0us
    async {
        do! s.AsyncWrite ProtocolHeader
        let! f = Frame.read s
        match f with
        | FrameOk 
            (Method 
                (0us, Connection (Start 
                    ( { Mechanisms = AsUTF8 (SplitBy " " (mech :: _)) 
                        Locales = AsUTF8 (SplitBy " " (locale :: _))
                        ServerProperties = props } as startData)))) ->
                let startOk =
                    { ClientProperties = [ "product", LongStrField "fsamqpc"B ] |> Map.ofList 
                      Locale = locale
                      Mechanism = mech
                      Response = [|0uy|] ++ "guest"B ++ [|0uy|] ++ "guest"B }
                    |> StartOk
                    |> ch0Method 
                do! Frame.write s startOk
                let! f = Frame.read s
                match f with
                | FrameOk (Method (0us, Connection (Tune tuneData))) ->
                    let tuneOk = 
                        TuneOk
                            { ChannelMax = tuneData.ChannelMax
                              FrameMax = tuneData.FrameMax
                              Heartbeat = 0us }
                        |> ch0Method
                    do! Frame.write s tuneOk
                    let opn = 
                        Connection.opn vhost
                        |> ch0Method
                    do! Frame.write s opn
                    let! f = Frame.read s
                    match f with
                    | FrameOk (Method (0us, (Connection (OpenOk _)))) ->
                        return Some 
                                { Socket = s
                                  StartData = startData
                                  TuneData = tuneData
                                  ChannelDeliveries = new Event<_>() }
                    | _ -> 
                        return None 
                | _ -> return None 
            | _ -> return None 
        | _ -> return None }


type WriterProto =
    | Frame of Frame
    | Exit
let writer (s: IO.Stream) err =
    let agent = new MailboxProcessor<_>(fun inbox ->
        let rec loop () =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Frame frame ->
                    do! Frame.write s frame
                    return! loop ()
                | Exit ->
                    s.Dispose() }
        loop ())
    agent.Error.Add err
    agent.Start()
    agent

let reader (s: IO.Stream) escalate =
    let loop =
        async {
            while true do
                let! received = Frame.read s
                escalate received }
    Async.StartDisposable loop

type ChanProto =
    | Receive of Frame
    | Publish of Basic.PublishData * Basic.BasicPropsData * byte []

type ConnProto =
    | Delivery of FrameReadResult
    | OpenChannel of AsyncReplyChannel<Channel>
    | CloseChannel of Short 
    | Send of Frame
    | Err of exn

and Channel (num: Short, post: ConnProto -> unit) =
    let agent = new MailboxProcessor<_>(fun inbox ->
        let rec loop state =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Receive (Method (chan, Channel.Channel msg)) -> 
                    printfn "channel method %A" msg
                | Publish (pub, props, data) ->
                    let pub = Basic.PublishData.pickle pub
                    let props = 
                        { Basic.BodySize = uint64 data.Length
                          Basic.Props = props }
                        |> Basic.BasicContentHeader.pickle
                    let command = Method (num, pub)
                    let header = Header (num, props)
                    let body = Body (num, data) //TODO max body size
                    post (Send command)
                    post (Send header)
                    post (Send body)
                return! loop state }
        and initial state =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Receive (Method (chan, Channel.Channel (Channel.OpenOk x))) -> 
                    printfn "open ok %A" x
                    return! loop state
                | frame -> printfn "expected Openok got %A" frame
                return! loop state }
        initial [])
    do
        agent.Start()
    with 
    member internal __.Post = agent.Post
    member __.Publish(exchange, routingKey, data) =
        let publishData =
            { Basic.PublishData.Reserved1 = 0us
              Basic.PublishData.Exchange = exchange
              Basic.PublishData.RoutingKey = routingKey
              Basic.PublishData.Mandatory = false
              Basic.PublishData.Immediate = false }
        let props = Basic.props ()
        agent.Post(Publish (publishData, props, data))
    interface IDisposable with
        member x.Dispose () = post (CloseChannel num)

type Connection (ctx: Ctx, err) =
    let agent = new MailboxProcessor<_>(fun inbox ->
        let writer = writer ctx.Socket (fun ex -> inbox.Post (Err ex))
        let reader = reader ctx.Socket (fun res -> Delivery res |> inbox.Post)
        let (|Chan|_|) (_, chans) = fun k -> Map.tryFind k chans
        let rec loop (state: Short * Map<Short, Channel>) =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Delivery (FrameOk (Method (0us, Connection (Close close)))) ->
                    printfn "connection closed: %A" close
                    writer.Post Exit
                    reader.Dispose()
                | Delivery (FrameOk (Method (0us, Connection connMsg))) ->
                    printfn "connection msg: %A" connMsg
                    return! loop state 
                | Delivery (FrameOk (Method (Chan state chan, payload) as frame)) -> //channel messages
                    chan.Post (Receive frame)
                    return! loop state 
                | Delivery (FrameOk (Method (chan, msg))) ->
                    printfn "some msg: %A" msg
                    return! loop state 
                | Send frame ->
                    writer.Post (Frame frame)
                    return! loop state 
                | CloseChannel num ->
                    let cls =
                        Channel.Channel.close Channel.classId
                        |> Channel.Channel.pickle
                        |> Frame.meth num
                    writer.Post (Frame cls)
                    let c, chans = state
                    return! loop (c, Map.remove num chans)
                | OpenChannel rc ->
                    let chCount, chans = state
                    let chNum = chCount + 1us
                    let meth = 
                        Channel.Channel.opn ()
                        |> Channel.Channel.pickle
                        |> Frame.meth chNum 
                    let chan = new Channel(chNum, inbox.Post)
                    let chans = Map.add chNum chan chans
                    writer.Post (Frame meth)
                    rc.Reply chan
                    return! loop (chNum, chans)
                | Err ex ->
                    printfn "connection error %A" ex
                    writer.Post Exit
                    reader.Dispose() }
        loop (0us, Map.empty))
    do 
        agent.Error.Add err
        agent.Start()
    with 
    member internal x.Post = agent.Post
    member x.OpenChannel () = 
        agent.PostAndAsyncReply ConnProto.OpenChannel
    static member start (conf: Cfg) =
        async {
            let s = connectSock "localhost" 5672
            let! x = handshake conf s
            match x with
            | Some ctx -> return Connection(ctx, raise)
            | None -> return failwith "handshake error" }

let c =
    { Vhost = "/"
      Creds = "guest", "guest" }

let conn =
    Connection.start c
    |> Async.RunSynchronously

let ch = conn.OpenChannel() |> Async.RunSynchronously            
ch.Publish("/", "test", "hi"B)
dispose ch
(*
                        let chOpen =
                            { Channel.OpenData.Reserved1 = "" }
                            |> Channel.Open
                            |> Channel.Channel.pickle
                        let chMeth = Method (1us, chOpen)
                        do! Frame.write s chMeth
                        let! f = Frame.read s
                        match f with
                        | FrameOk (Method (1us, payload)) ->
                            Channel.Channel.parse payload |> printfn "%A"
                        | f -> printfn "got %A" f
                        *)
