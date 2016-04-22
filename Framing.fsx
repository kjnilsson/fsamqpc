
open System
open System.Text
open System.Net.Sockets
#if INTERACTIVE
#load "Pervasives.fsx"
#load "Amqp.fsx"
#load "GenConnection.fsx"
#load "GenChannel.fsx"
#endif

open Amqp
open Connection

type Ctx =
    { Socket: IO.Stream
      StartData: StartData
      TuneData: TuneData }

let handshake () =
    async {
        let sock = new TcpClient("localhost", 5672)
        sock.NoDelay <- true
        let s = sock.GetStream()
        do! s.AsyncWrite ProtocolHeader
        let! f = Frame.read s
        match f with
        | FrameOk (Method (0us, payload)) ->
            match Connection.parse payload with
            | Start 
                ( { Mechanisms = AsUTF8 (SplitBy " " (mech :: _)) 
                    Locales = AsUTF8 (SplitBy " " (locale :: _))
                    ServerProperties = props } as startData) ->
                let startOk =
                    { StartOkData.ClientProperties = Map.empty
                      StartOkData.Locale = locale
                      StartOkData.Mechanism = mech
                      StartOkData.Response = [|0uy|] ++ "guest"B ++ [|0uy|] ++ "guest"B }
                let startOkPayload = Connection.pickle (StartOk startOk)
                let startOkFrame = (Method (0us, startOkPayload)) 
                do! Frame.write s startOkFrame
                let! f = Frame.read s
                match f with
                | FrameOk (Method (0us, payload)) ->
                    let (Tune tuneData) = Connection.parse payload
                    let tuneOk = 
                        TuneOk
                            { TuneOkData.ChannelMax = tuneData.ChannelMax
                              TuneOkData.FrameMax = tuneData.FrameMax
                              TuneOkData.Heartbeat = 0us }
                        |> Connection.pickle
                    let meth = Method (0us, tuneOk)
                    do! Frame.write s meth
                    let opn =
                        Open
                            { OpenData.VirtualHost  = "/"
                              OpenData.Reserved1 = ""
                              OpenData.Reserved2 = true }
                        |> Connection.pickle
                    let meth = Method (0us, opn)
                    do! Frame.write s meth
                    let! f = Frame.read s
                    match f with
                    | FrameOk (Method (0us, payload)) ->
                        Connection.parse payload |> printfn "%A"
                        return Some { Socket = s; StartData = startData; TuneData = tuneData }
                    | _ -> return None 
                | _ -> return None 
            | _ -> return None 
        | _ -> return None }

handshake () |> Async.RunSynchronously
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
