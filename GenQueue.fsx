module Queue
#if INTERACTIVE
#load "Amqp.fsx"
#endif
open Amqp


let classId = 50us


type DeclareData = {
    Reserved1: Short
    Queue: ShortStr
    Passive: Bit
    Durable: Bit
    Exclusive: Bit
    AutoDelete: Bit
    NoWait: Bit
    Arguments: Table
} with
    static member id = 10us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, queue = readShortStr payload off
        let bit, off, passive = readBit payload off bit
        let bit, off, durable = readBit payload off bit
        let bit, off, exclusive = readBit payload off bit
        let bit, off, autoDelete = readBit payload off bit
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        let off, arguments = readTable payload off
        {
            Reserved1 = reserved1
            Queue = queue
            Passive = passive
            Durable = durable
            Exclusive = exclusive
            AutoDelete = autoDelete
            NoWait = noWait
            Arguments = arguments
        }
    static member pickle (x: DeclareData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Queue
            let bits = [ x.Passive; x.Durable; x.Exclusive; x.AutoDelete; x.NoWait ]
            yield! writeBits bits
            yield! writeTable x.Arguments
        |]


type DeclareOkData = {
    Queue: ShortStr
    MessageCount: Long
    ConsumerCount: Long
} with
    static member id = 11us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, queue = readShortStr payload off
        let off, messageCount = readLong payload off
        let off, consumerCount = readLong payload off
        {
            Queue = queue
            MessageCount = messageCount
            ConsumerCount = consumerCount
        }
    static member pickle (x: DeclareOkData) =
        [|
            yield! writeShortStr x.Queue
            yield! writeLong x.MessageCount
            yield! writeLong x.ConsumerCount
        |]


type BindData = {
    Reserved1: Short
    Queue: ShortStr
    Exchange: ShortStr
    RoutingKey: ShortStr
    NoWait: Bit
    Arguments: Table
} with
    static member id = 20us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, queue = readShortStr payload off
        let off, exchange = readShortStr payload off
        let off, routingKey = readShortStr payload off
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        let off, arguments = readTable payload off
        {
            Reserved1 = reserved1
            Queue = queue
            Exchange = exchange
            RoutingKey = routingKey
            NoWait = noWait
            Arguments = arguments
        }
    static member pickle (x: BindData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Queue
            yield! writeShortStr x.Exchange
            yield! writeShortStr x.RoutingKey
            let bits = [ x.NoWait ]
            yield! writeBits bits
            yield! writeTable x.Arguments
        |]




type UnbindData = {
    Reserved1: Short
    Queue: ShortStr
    Exchange: ShortStr
    RoutingKey: ShortStr
    Arguments: Table
} with
    static member id = 50us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, queue = readShortStr payload off
        let off, exchange = readShortStr payload off
        let off, routingKey = readShortStr payload off
        let off, arguments = readTable payload off
        {
            Reserved1 = reserved1
            Queue = queue
            Exchange = exchange
            RoutingKey = routingKey
            Arguments = arguments
        }
    static member pickle (x: UnbindData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Queue
            yield! writeShortStr x.Exchange
            yield! writeShortStr x.RoutingKey
            yield! writeTable x.Arguments
        |]




type PurgeData = {
    Reserved1: Short
    Queue: ShortStr
    NoWait: Bit
} with
    static member id = 30us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, queue = readShortStr payload off
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        {
            Reserved1 = reserved1
            Queue = queue
            NoWait = noWait
        }
    static member pickle (x: PurgeData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Queue
            let bits = [ x.NoWait ]
            yield! writeBits bits
        |]


type PurgeOkData = {
    MessageCount: Long
} with
    static member id = 31us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, messageCount = readLong payload off
        {
            MessageCount = messageCount
        }
    static member pickle (x: PurgeOkData) =
        [|
            yield! writeLong x.MessageCount
        |]


type DeleteData = {
    Reserved1: Short
    Queue: ShortStr
    IfUnused: Bit
    IfEmpty: Bit
    NoWait: Bit
} with
    static member id = 40us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, queue = readShortStr payload off
        let bit, off, ifUnused = readBit payload off bit
        let bit, off, ifEmpty = readBit payload off bit
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        {
            Reserved1 = reserved1
            Queue = queue
            IfUnused = ifUnused
            IfEmpty = ifEmpty
            NoWait = noWait
        }
    static member pickle (x: DeleteData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Queue
            let bits = [ x.IfUnused; x.IfEmpty; x.NoWait ]
            yield! writeBits bits
        |]


type DeleteOkData = {
    MessageCount: Long
} with
    static member id = 41us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, messageCount = readLong payload off
        {
            MessageCount = messageCount
        }
    static member pickle (x: DeleteOkData) =
        [|
            yield! writeLong x.MessageCount
        |]


type Queue =
    | Declare of DeclareData
    | DeclareOk of DeclareOkData
    | Bind of BindData
    | BindOk
    | Unbind of UnbindData
    | UnbindOk
    | Purge of PurgeData
    | PurgeOk of PurgeOkData
    | Delete of DeleteData
    | DeleteOk of DeleteOkData
with
    static member parse (payload: byte []) =
        match toShort payload 0 with
        | 50us ->
            match toShort payload 2 with
            | 10us -> DeclareData.parse payload |> Declare
            | 11us -> DeclareOkData.parse payload |> DeclareOk
            | 20us -> BindData.parse payload |> Bind
            | 21us -> BindOk
            | 50us -> UnbindData.parse payload |> Unbind
            | 51us -> UnbindOk
            | 30us -> PurgeData.parse payload |> Purge
            | 31us -> PurgeOkData.parse payload |> PurgeOk
            | 40us -> DeleteData.parse payload |> Delete
            | 41us -> DeleteOkData.parse payload |> DeleteOk
            | x -> failwith (sprintf "%A not implemented" x)
            |> Some
        | _ -> None
    static member pickle (x: Queue) = [|
        yield! fromShort 50us
        match x with
        | Declare data -> yield! fromShort 10us; yield! DeclareData.pickle data
        | DeclareOk data -> yield! fromShort 11us; yield! DeclareOkData.pickle data
        | Bind data -> yield! fromShort 20us; yield! BindData.pickle data
        | BindOk -> yield! fromShort 21us
        | Unbind data -> yield! fromShort 50us; yield! UnbindData.pickle data
        | UnbindOk -> yield! fromShort 51us
        | Purge data -> yield! fromShort 30us; yield! PurgeData.pickle data
        | PurgeOk data -> yield! fromShort 31us; yield! PurgeOkData.pickle data
        | Delete data -> yield! fromShort 40us; yield! DeleteData.pickle data
        | DeleteOk data -> yield! fromShort 41us; yield! DeleteOkData.pickle data
    |]

let (|Queue|_|) = Queue.parse