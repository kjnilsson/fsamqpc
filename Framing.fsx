open System.Net.Sockets
#if INTERACTIVE
#load "Pervasives.fsx"
#load "Amqp.fsx"
#endif

open Amqp


#load "GenConnection.fsx"
async {
    let sock = new TcpClient("localhost", 5672)
    let s = sock.GetStream()
    do! s.AsyncWrite AMQPProtocolHeader
    let! f = Frame.read s
    match f with
    | FrameOk (Method (0us, payload)) ->
        Connection.Connection.parse payload |> printfn "%A"
    | _ -> failwith "oh noes" }
|> Async.RunSynchronously
