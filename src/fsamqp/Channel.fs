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
