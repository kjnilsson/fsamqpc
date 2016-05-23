[<AutoOpen>]
module ConnectionExt
open System
open System.Net.Sockets
open Connection
open Amqp

type Connection with
    static member opn vhost =
        { VirtualHost  = vhost
          Reserved1 = ""
          Reserved2 = true }
        |> Open

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
                printfn "start data %A" startData
                let startOk =
                    { ClientProperties = [ "product", LongStrField "fsamqpc"B ] |> Map.ofList 
                      Locale = locale
                      Mechanism = "PLAIN"
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

type ConnProto =
    | Delivery of FrameReadResult
    | OpenChannel of AsyncReplyChannel<Channel>
    | CloseChannel of Short 
    | Send of Frame
    | Err of exn

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
                    let chan = new Channel(chNum, Send >> inbox.Post)
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

