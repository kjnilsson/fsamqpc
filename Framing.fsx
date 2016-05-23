open System
open System.Text
open System.Net.Sockets
#if INTERACTIVE
#load "./src/fsamqp/Pervasives.fs"
#load "./src/fsamqp/Amqp.fs"
#load "./src/fsamqp/GenConnection.fs"
#load "./src/fsamqp/GenChannel.fs"
#load "./src/fsamqp/GenBasic.fs"
#load "./src/fsamqp/Basic.fs"
#load "./src/fsamqp/Channel.fs"
#load "./src/fsamqp/Connection.fs"
#endif

open Connection
let c =
    { Vhost = "/"
      Creds = "guest", "guest" }

let conn =
    Connection.start c
    |> Async.RunSynchronously

let ch = conn.OpenChannel() |> Async.RunSynchronously            
ch.Publish("", "test", "hi"B)
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

[|0uy; 10uy; 0uy; 11uy; 0uy; 0uy; 0uy; 20uy; 7uy; 112uy; 114uy; 111uy; 100uy;
  117uy; 99uy; 116uy; 83uy; 0uy; 0uy; 0uy; 7uy; 102uy; 115uy; 97uy; 109uy; 113uy;
  112uy; 99uy; 8uy; 65uy; 77uy; 81uy; 80uy; 76uy; 65uy; 73uy; 78uy; 0uy; 0uy;
  0uy; 12uy; 0uy; 103uy; 117uy; 101uy; 115uy; 116uy; 0uy; 103uy; 117uy; 101uy;
  115uy; 116uy; 5uy; 101uy; 110uy; 95uy; 85uy; 83uy|]
|> Connection.StartOkData.parse


//let (AsUTF8 data) = [|65uy; 77uy; 81uy; 80uy; 76uy; 65uy; 73uy; 78uy; 32uy; 80uy; 76uy; 65uy; 73uy; 78uy|]

