module Channel
open Amqp


let classId = 20us


type OpenData = {
    Reserved1: ShortStr
} with
    static member id = 10us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShortStr payload off
        {
            Reserved1 = reserved1
        }
    static member pickle (x: OpenData) =
        [|
            yield! writeShortStr x.Reserved1
        |]


type OpenOkData = {
    Reserved1: LongStr
} with
    static member id = 11us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readLongStr payload off
        {
            Reserved1 = reserved1
        }
    static member pickle (x: OpenOkData) =
        [|
            yield! writeLongStr x.Reserved1
        |]


type FlowData = {
    Active: Bit
} with
    static member id = 20us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let bit, off, active = readBit payload off bit
        let bit = 0
        {
            Active = active
        }
    static member pickle (x: FlowData) =
        [|
            let bits = [ x.Active ]
            yield! writeBits bits
        |]


type FlowOkData = {
    Active: Bit
} with
    static member id = 21us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let bit, off, active = readBit payload off bit
        let bit = 0
        {
            Active = active
        }
    static member pickle (x: FlowOkData) =
        [|
            let bits = [ x.Active ]
            yield! writeBits bits
        |]


type CloseData = {
    ReplyCode: Short
    ReplyText: ShortStr
    ClassId: Short
    MethodId: Short
} with
    static member id = 40us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, replyCode = readShort payload off
        let off, replyText = readShortStr payload off
        let off, classId = readShort payload off
        let off, methodId = readShort payload off
        {
            ReplyCode = replyCode
            ReplyText = replyText
            ClassId = classId
            MethodId = methodId
        }
    static member pickle (x: CloseData) =
        [|
            yield! writeShort x.ReplyCode
            yield! writeShortStr x.ReplyText
            yield! writeShort x.ClassId
            yield! writeShort x.MethodId
        |]




type Channel =
    | Open of OpenData
    | OpenOk of OpenOkData
    | Flow of FlowData
    | FlowOk of FlowOkData
    | Close of CloseData
    | CloseOk
with
    static member parse (payload: byte []) =
        match toShort payload 0 with
        | 20us ->
            match toShort payload 2 with
            | 10us -> OpenData.parse payload |> Open
            | 11us -> OpenOkData.parse payload |> OpenOk
            | 20us -> FlowData.parse payload |> Flow
            | 21us -> FlowOkData.parse payload |> FlowOk
            | 40us -> CloseData.parse payload |> Close
            | 41us -> CloseOk
            | x -> failwith (sprintf "%A not implemented" x)
            |> Some
        | _ -> None
    static member pickle (x: Channel) = [|
        yield! fromShort 20us
        match x with
        | Open data -> yield! fromShort 10us; yield! OpenData.pickle data
        | OpenOk data -> yield! fromShort 11us; yield! OpenOkData.pickle data
        | Flow data -> yield! fromShort 20us; yield! FlowData.pickle data
        | FlowOk data -> yield! fromShort 21us; yield! FlowOkData.pickle data
        | Close data -> yield! fromShort 40us; yield! CloseData.pickle data
        | CloseOk -> yield! fromShort 41us
    |]

let (|Channel|_|) = Channel.parse