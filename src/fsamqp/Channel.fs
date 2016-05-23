[<AutoOpen>]
module ChannelExt
open Channel

type Channel with 
    static member opn () =
        Open { OpenData.Reserved1 = "" }
    static member close cid =
        { CloseData.ClassId = cid 
          CloseData.MethodId = 0us
          CloseData.ReplyCode = 0us
          CloseData.ReplyText = "" }
        |> Close 

open Amqp
open Basic
open BasicExt
type ChanProto =
    | Receive of Frame
    | Publish of Basic.PublishData * BasicExt.BasicPropsData * byte []
    | CloseCh of AsyncReplyChannel<unit>

type Channel (num: Short, send: Frame -> unit) =
    let agent = new MailboxProcessor<_>(fun inbox ->
        let rec loop state =
            async {
                let! msg = inbox.Receive()
                match msg with
                | Receive (Method (chan, Channel.Channel msg)) -> 
                    printfn "channel method %A" msg
                | Publish (pub, props, data) ->
                    let pub = Basic.Basic.pickle (Basic.Publish pub)
                    let props = 
                        { BodySize = uint64 data.Length
                          Props = props }
                        |> BasicExt.BasicContentHeader.pickle
                    let command = Method (num, pub)
                    let header = Header (num, props)
                    let body = Body (num, data) //TODO max body size
                    send command
                    send header
                    send body
                | CloseCh rc ->
                    printfn "channel close"
                    Method (num, Channel.Channel.close Channel.classId |> Channel.Channel.pickle)
                    |> send
                    rc.Reply()
                    //TODO await closeok?
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
        agent.Post(Publish (publishData, Map.empty, data))
    interface System.IDisposable with
        member x.Dispose () = agent.PostAndReply CloseCh

