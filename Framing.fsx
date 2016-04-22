
open System
open System.Text
open System.Net.Sockets
#if INTERACTIVE
#load "Pervasives.fsx"
#load "Amqp.fsx"
#endif

open Amqp

#load "GenConnection.fsx"
#load "GenChannel.fsx"
async {
    let sock = new TcpClient("localhost", 5672)
    sock.NoDelay <- true
    let s = sock.GetStream()
    do! s.AsyncWrite ProtocolHeader
    let! f = Frame.read s
    match f with
    | FrameOk (Method (0us, payload)) ->
        match Connection.Connection.parse payload with
        | Connection.Start 
            { Mechanisms = AsUTF8 (SplitBy " " (mech :: _)) 
              Locales = AsUTF8 (SplitBy " " (locale :: _))
              ServerProperties = props } as startData ->
            let startOk =
                { Connection.StartOkData.ClientProperties = Map.empty
                  Connection.StartOkData.Locale = locale
                  Connection.StartOkData.Mechanism = mech
                  Connection.StartOkData.Response = [|0uy|] ++ "guest"B ++ [|0uy|] ++ "guest"B }
            let startOkPayload = Connection.Connection.pickle (Connection.StartOk startOk)
            let startOkFrame = (Method (0us, startOkPayload)) 
            do! Frame.write s startOkFrame
            let! f = Frame.read s
            match f with
            | FrameOk (Method (0us, payload)) ->
                let (Connection.Tune tuneData) = Connection.Connection.parse payload
                printfn "tune: %A" tuneData
                let tuneOk = 
                    Connection.TuneOk
                        { Connection.TuneOkData.ChannelMax = tuneData.ChannelMax
                          Connection.TuneOkData.FrameMax = tuneData.FrameMax
                          Connection.TuneOkData.Heartbeat = 0us }
                    |> Connection.Connection.pickle
                let meth = Method (0us, tuneOk)
                do! Frame.write s meth
                printfn "tuneOk written"
                let opn =
                    Connection.Open
                        { Connection.OpenData.VirtualHost  = "/"
                          Connection.OpenData.Reserved1 = ""
                          Connection.OpenData.Reserved2 = true }
                    |> Connection.Connection.pickle
                let meth = Method (0us, opn)
                do! Frame.write s meth
                let! f = Frame.read s
                match f with
                | FrameOk (Method (0us, payload)) ->
                    Connection.Connection.parse payload |> printfn "%A"
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
                            
                | _ -> ()
        | _ -> ()
            
    | _ -> failwith "oh noes" }
|> Async.RunSynchronously
