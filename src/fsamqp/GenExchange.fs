module Exchange
open Amqp


let classId = 40us


type DeclareData = {
    Reserved1: Short
    Exchange: ShortStr
    Type: ShortStr
    Passive: Bit
    Durable: Bit
    AutoDelete: Bit
    Internal: Bit
    NoWait: Bit
    Arguments: Table
} with
    static member id = 10us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, exchange = readShortStr payload off
        let off, type' = readShortStr payload off
        let bit, off, passive = readBit payload off bit
        let bit, off, durable = readBit payload off bit
        let bit, off, autoDelete = readBit payload off bit
        let bit, off, internal' = readBit payload off bit
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        let off, arguments = readTable payload off
        {
            Reserved1 = reserved1
            Exchange = exchange
            Type = type'
            Passive = passive
            Durable = durable
            AutoDelete = autoDelete
            Internal = internal'
            NoWait = noWait
            Arguments = arguments
        }
    static member pickle (x: DeclareData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Exchange
            yield! writeShortStr x.Type
            let bits = [ x.Passive; x.Durable; x.AutoDelete; x.Internal; x.NoWait ]
            yield! writeBits bits
            yield! writeTable x.Arguments
        |]




type DeleteData = {
    Reserved1: Short
    Exchange: ShortStr
    IfUnused: Bit
    NoWait: Bit
} with
    static member id = 20us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, exchange = readShortStr payload off
        let bit, off, ifUnused = readBit payload off bit
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        {
            Reserved1 = reserved1
            Exchange = exchange
            IfUnused = ifUnused
            NoWait = noWait
        }
    static member pickle (x: DeleteData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Exchange
            let bits = [ x.IfUnused; x.NoWait ]
            yield! writeBits bits
        |]




type BindData = {
    Reserved1: Short
    Destination: ShortStr
    Source: ShortStr
    RoutingKey: ShortStr
    NoWait: Bit
    Arguments: Table
} with
    static member id = 30us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, destination = readShortStr payload off
        let off, source = readShortStr payload off
        let off, routingKey = readShortStr payload off
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        let off, arguments = readTable payload off
        {
            Reserved1 = reserved1
            Destination = destination
            Source = source
            RoutingKey = routingKey
            NoWait = noWait
            Arguments = arguments
        }
    static member pickle (x: BindData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Destination
            yield! writeShortStr x.Source
            yield! writeShortStr x.RoutingKey
            let bits = [ x.NoWait ]
            yield! writeBits bits
            yield! writeTable x.Arguments
        |]




type UnbindData = {
    Reserved1: Short
    Destination: ShortStr
    Source: ShortStr
    RoutingKey: ShortStr
    NoWait: Bit
    Arguments: Table
} with
    static member id = 40us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, destination = readShortStr payload off
        let off, source = readShortStr payload off
        let off, routingKey = readShortStr payload off
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        let off, arguments = readTable payload off
        {
            Reserved1 = reserved1
            Destination = destination
            Source = source
            RoutingKey = routingKey
            NoWait = noWait
            Arguments = arguments
        }
    static member pickle (x: UnbindData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Destination
            yield! writeShortStr x.Source
            yield! writeShortStr x.RoutingKey
            let bits = [ x.NoWait ]
            yield! writeBits bits
            yield! writeTable x.Arguments
        |]




type Exchange =
    | Declare of DeclareData
    | DeclareOk
    | Delete of DeleteData
    | DeleteOk
    | Bind of BindData
    | BindOk
    | Unbind of UnbindData
    | UnbindOk
with
    static member parse (payload: byte []) =
        match toShort payload 0 with
        | 40us ->
            match toShort payload 2 with
            | 10us -> DeclareData.parse payload |> Declare
            | 11us -> DeclareOk
            | 20us -> DeleteData.parse payload |> Delete
            | 21us -> DeleteOk
            | 30us -> BindData.parse payload |> Bind
            | 31us -> BindOk
            | 40us -> UnbindData.parse payload |> Unbind
            | 51us -> UnbindOk
            | x -> failwith (sprintf "%A not implemented" x)
            |> Some
        | _ -> None
    static member pickle (x: Exchange) = [|
        yield! fromShort 40us
        match x with
        | Declare data -> yield! fromShort 10us; yield! DeclareData.pickle data
        | DeclareOk -> yield! fromShort 11us
        | Delete data -> yield! fromShort 20us; yield! DeleteData.pickle data
        | DeleteOk -> yield! fromShort 21us
        | Bind data -> yield! fromShort 30us; yield! BindData.pickle data
        | BindOk -> yield! fromShort 31us
        | Unbind data -> yield! fromShort 40us; yield! UnbindData.pickle data
        | UnbindOk -> yield! fromShort 51us
    |]

let (|Exchange|_|) = Exchange.parse