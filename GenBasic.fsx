module Basic
#if INTERACTIVE
#load "Amqp.fsx"
#endif
open Amqp


let classId = 60us


type BasicPropsData = {
    ContentType: ShortStr
    ContentEncoding: ShortStr
    Headers: Table
    DeliveryMode: Octet
    Priority: Octet
    CorrelationId: ShortStr
    ReplyTo: ShortStr
    Expiration: ShortStr
    MessageId: ShortStr
    Timestamp: Timestamp
    Type: ShortStr
    UserId: ShortStr
    AppId: ShortStr
    ClusterId: ShortStr
} with
    static member id = 60us
    static member parse (payload: byte []) =
        let off = 12
        let bit = 0
        let off, flags = readShort payload off
        let withDef index f payload off def = if isSet flags index then f payload off else off, def
        let off, contentType = withDef 0 readShortStr payload off ""
        let off, contentEncoding = withDef 1 readShortStr payload off ""
        let off, headers = withDef 2 readTable payload off Map.empty
        let off, deliveryMode = withDef 3 readOctet payload off 0uy
        let off, priority = withDef 4 readOctet payload off 0uy
        let off, correlationId = withDef 5 readShortStr payload off ""
        let off, replyTo = withDef 6 readShortStr payload off ""
        let off, expiration = withDef 7 readShortStr payload off ""
        let off, messageId = withDef 8 readShortStr payload off ""
        let off, timestamp = withDef 9 readTimestamp payload off 0uL
        let off, type' = withDef 10 readShortStr payload off ""
        let off, userId = withDef 11 readShortStr payload off ""
        let off, appId = withDef 12 readShortStr payload off ""
        let off, clusterId = withDef 13 readShortStr payload off ""
        {
            ContentType = contentType
            ContentEncoding = contentEncoding
            Headers = headers
            DeliveryMode = deliveryMode
            Priority = priority
            CorrelationId = correlationId
            ReplyTo = replyTo
            Expiration = expiration
            MessageId = messageId
            Timestamp = timestamp
            Type = type'
            UserId = userId
            AppId = appId
            ClusterId = clusterId
        }
    static member pickle (x: BasicPropsData) =
        [|
            yield! writeShort (System.UInt16.MaxValue <<< 2)
            yield! writeShortStr x.ContentType
            yield! writeShortStr x.ContentEncoding
            yield! writeTable x.Headers
            yield! writeOctet x.DeliveryMode
            yield! writeOctet x.Priority
            yield! writeShortStr x.CorrelationId
            yield! writeShortStr x.ReplyTo
            yield! writeShortStr x.Expiration
            yield! writeShortStr x.MessageId
            yield! writeTimestamp x.Timestamp
            yield! writeShortStr x.Type
            yield! writeShortStr x.UserId
            yield! writeShortStr x.AppId
            yield! writeShortStr x.ClusterId
        |]

type BasicContentHeader =
    { BodySize: LongLong
      Props: BasicPropsData }
    static member parse (payload: byte[]) =
        match readShort payload 0 with
        | _, 60us ->
            let off, size = readLongLong payload 4
            let props = BasicPropsData.parse payload
            { BodySize = size
              Props = props }
            |> Some
        | _ -> None
    static member pickle (x: BasicContentHeader) =
        [|
            yield! writeShort 60us
            yield! writeShort 0us
            yield! BasicPropsData.pickle x.Props
        |]

type QosData = {
    PrefetchSize: Long
    PrefetchCount: Short
    Global: Bit
} with
    static member id = 10us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, prefetchSize = readLong payload off
        let off, prefetchCount = readShort payload off
        let bit, off, global' = readBit payload off bit
        let bit = 0
        {
            PrefetchSize = prefetchSize
            PrefetchCount = prefetchCount
            Global = global'
        }
    static member pickle (x: QosData) =
        [|
            yield! writeLong x.PrefetchSize
            yield! writeShort x.PrefetchCount
            let bits = [ x.Global ]
            yield! writeBits bits
        |]




type ConsumeData = {
    Reserved1: Short
    Queue: ShortStr
    ConsumerTag: ShortStr
    NoLocal: Bit
    NoAck: Bit
    Exclusive: Bit
    NoWait: Bit
    Arguments: Table
} with
    static member id = 20us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, queue = readShortStr payload off
        let off, consumerTag = readShortStr payload off
        let bit, off, noLocal = readBit payload off bit
        let bit, off, noAck = readBit payload off bit
        let bit, off, exclusive = readBit payload off bit
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        let off, arguments = readTable payload off
        {
            Reserved1 = reserved1
            Queue = queue
            ConsumerTag = consumerTag
            NoLocal = noLocal
            NoAck = noAck
            Exclusive = exclusive
            NoWait = noWait
            Arguments = arguments
        }
    static member pickle (x: ConsumeData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Queue
            yield! writeShortStr x.ConsumerTag
            let bits = [ x.NoLocal; x.NoAck; x.Exclusive; x.NoWait ]
            yield! writeBits bits
            yield! writeTable x.Arguments
        |]


type ConsumeOkData = {
    ConsumerTag: ShortStr
} with
    static member id = 21us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, consumerTag = readShortStr payload off
        {
            ConsumerTag = consumerTag
        }
    static member pickle (x: ConsumeOkData) =
        [|
            yield! writeShortStr x.ConsumerTag
        |]


type CancelData = {
    ConsumerTag: ShortStr
    NoWait: Bit
} with
    static member id = 30us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, consumerTag = readShortStr payload off
        let bit, off, noWait = readBit payload off bit
        let bit = 0
        {
            ConsumerTag = consumerTag
            NoWait = noWait
        }
    static member pickle (x: CancelData) =
        [|
            yield! writeShortStr x.ConsumerTag
            let bits = [ x.NoWait ]
            yield! writeBits bits
        |]


type CancelOkData = {
    ConsumerTag: ShortStr
} with
    static member id = 31us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, consumerTag = readShortStr payload off
        {
            ConsumerTag = consumerTag
        }
    static member pickle (x: CancelOkData) =
        [|
            yield! writeShortStr x.ConsumerTag
        |]


type PublishData = {
    Reserved1: Short
    Exchange: ShortStr
    RoutingKey: ShortStr
    Mandatory: Bit
    Immediate: Bit
} with
    static member id = 40us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, exchange = readShortStr payload off
        let off, routingKey = readShortStr payload off
        let bit, off, mandatory = readBit payload off bit
        let bit, off, immediate = readBit payload off bit
        let bit = 0
        {
            Reserved1 = reserved1
            Exchange = exchange
            RoutingKey = routingKey
            Mandatory = mandatory
            Immediate = immediate
        }
    static member pickle (x: PublishData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Exchange
            yield! writeShortStr x.RoutingKey
            let bits = [ x.Mandatory; x.Immediate ]
            yield! writeBits bits
        |]


type ReturnData = {
    ReplyCode: Short
    ReplyText: ShortStr
    Exchange: ShortStr
    RoutingKey: ShortStr
} with
    static member id = 50us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, replyCode = readShort payload off
        let off, replyText = readShortStr payload off
        let off, exchange = readShortStr payload off
        let off, routingKey = readShortStr payload off
        {
            ReplyCode = replyCode
            ReplyText = replyText
            Exchange = exchange
            RoutingKey = routingKey
        }
    static member pickle (x: ReturnData) =
        [|
            yield! writeShort x.ReplyCode
            yield! writeShortStr x.ReplyText
            yield! writeShortStr x.Exchange
            yield! writeShortStr x.RoutingKey
        |]


type DeliverData = {
    ConsumerTag: ShortStr
    DeliveryTag: LongLong
    Redelivered: Bit
    Exchange: ShortStr
    RoutingKey: ShortStr
} with
    static member id = 60us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, consumerTag = readShortStr payload off
        let off, deliveryTag = readLongLong payload off
        let bit, off, redelivered = readBit payload off bit
        let bit = 0
        let off, exchange = readShortStr payload off
        let off, routingKey = readShortStr payload off
        {
            ConsumerTag = consumerTag
            DeliveryTag = deliveryTag
            Redelivered = redelivered
            Exchange = exchange
            RoutingKey = routingKey
        }
    static member pickle (x: DeliverData) =
        [|
            yield! writeShortStr x.ConsumerTag
            yield! writeLongLong x.DeliveryTag
            let bits = [ x.Redelivered ]
            yield! writeBits bits
            yield! writeShortStr x.Exchange
            yield! writeShortStr x.RoutingKey
        |]


type GetData = {
    Reserved1: Short
    Queue: ShortStr
    NoAck: Bit
} with
    static member id = 70us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShort payload off
        let off, queue = readShortStr payload off
        let bit, off, noAck = readBit payload off bit
        let bit = 0
        {
            Reserved1 = reserved1
            Queue = queue
            NoAck = noAck
        }
    static member pickle (x: GetData) =
        [|
            yield! writeShort x.Reserved1
            yield! writeShortStr x.Queue
            let bits = [ x.NoAck ]
            yield! writeBits bits
        |]


type GetOkData = {
    DeliveryTag: LongLong
    Redelivered: Bit
    Exchange: ShortStr
    RoutingKey: ShortStr
    MessageCount: Long
} with
    static member id = 71us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, deliveryTag = readLongLong payload off
        let bit, off, redelivered = readBit payload off bit
        let bit = 0
        let off, exchange = readShortStr payload off
        let off, routingKey = readShortStr payload off
        let off, messageCount = readLong payload off
        {
            DeliveryTag = deliveryTag
            Redelivered = redelivered
            Exchange = exchange
            RoutingKey = routingKey
            MessageCount = messageCount
        }
    static member pickle (x: GetOkData) =
        [|
            yield! writeLongLong x.DeliveryTag
            let bits = [ x.Redelivered ]
            yield! writeBits bits
            yield! writeShortStr x.Exchange
            yield! writeShortStr x.RoutingKey
            yield! writeLong x.MessageCount
        |]


type GetEmptyData = {
    Reserved1: ShortStr
} with
    static member id = 72us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShortStr payload off
        {
            Reserved1 = reserved1
        }
    static member pickle (x: GetEmptyData) =
        [|
            yield! writeShortStr x.Reserved1
        |]


type AckData = {
    DeliveryTag: LongLong
    Multiple: Bit
} with
    static member id = 80us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, deliveryTag = readLongLong payload off
        let bit, off, multiple = readBit payload off bit
        let bit = 0
        {
            DeliveryTag = deliveryTag
            Multiple = multiple
        }
    static member pickle (x: AckData) =
        [|
            yield! writeLongLong x.DeliveryTag
            let bits = [ x.Multiple ]
            yield! writeBits bits
        |]


type RejectData = {
    DeliveryTag: LongLong
    Requeue: Bit
} with
    static member id = 90us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, deliveryTag = readLongLong payload off
        let bit, off, requeue = readBit payload off bit
        let bit = 0
        {
            DeliveryTag = deliveryTag
            Requeue = requeue
        }
    static member pickle (x: RejectData) =
        [|
            yield! writeLongLong x.DeliveryTag
            let bits = [ x.Requeue ]
            yield! writeBits bits
        |]


type RecoverAsyncData = {
    Requeue: Bit
} with
    static member id = 100us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let bit, off, requeue = readBit payload off bit
        let bit = 0
        {
            Requeue = requeue
        }
    static member pickle (x: RecoverAsyncData) =
        [|
            let bits = [ x.Requeue ]
            yield! writeBits bits
        |]


type RecoverData = {
    Requeue: Bit
} with
    static member id = 110us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let bit, off, requeue = readBit payload off bit
        let bit = 0
        {
            Requeue = requeue
        }
    static member pickle (x: RecoverData) =
        [|
            let bits = [ x.Requeue ]
            yield! writeBits bits
        |]




type NackData = {
    DeliveryTag: LongLong
    Multiple: Bit
    Requeue: Bit
} with
    static member id = 120us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, deliveryTag = readLongLong payload off
        let bit, off, multiple = readBit payload off bit
        let bit, off, requeue = readBit payload off bit
        let bit = 0
        {
            DeliveryTag = deliveryTag
            Multiple = multiple
            Requeue = requeue
        }
    static member pickle (x: NackData) =
        [|
            yield! writeLongLong x.DeliveryTag
            let bits = [ x.Multiple; x.Requeue ]
            yield! writeBits bits
        |]


type Basic =
    | Qos of QosData
    | QosOk
    | Consume of ConsumeData
    | ConsumeOk of ConsumeOkData
    | Cancel of CancelData
    | CancelOk of CancelOkData
    | Publish of PublishData
    | Return of ReturnData
    | Deliver of DeliverData
    | Get of GetData
    | GetOk of GetOkData
    | GetEmpty of GetEmptyData
    | Ack of AckData
    | Reject of RejectData
    | RecoverAsync of RecoverAsyncData
    | Recover of RecoverData
    | RecoverOk
    | Nack of NackData
with
    static member parse (payload: byte []) =
        match toShort payload 0 with
        | 60us ->
            match toShort payload 2 with
            | 10us -> QosData.parse payload |> Qos
            | 11us -> QosOk
            | 20us -> ConsumeData.parse payload |> Consume
            | 21us -> ConsumeOkData.parse payload |> ConsumeOk
            | 30us -> CancelData.parse payload |> Cancel
            | 31us -> CancelOkData.parse payload |> CancelOk
            | 40us -> PublishData.parse payload |> Publish
            | 50us -> ReturnData.parse payload |> Return
            | 60us -> DeliverData.parse payload |> Deliver
            | 70us -> GetData.parse payload |> Get
            | 71us -> GetOkData.parse payload |> GetOk
            | 72us -> GetEmptyData.parse payload |> GetEmpty
            | 80us -> AckData.parse payload |> Ack
            | 90us -> RejectData.parse payload |> Reject
            | 100us -> RecoverAsyncData.parse payload |> RecoverAsync
            | 110us -> RecoverData.parse payload |> Recover
            | 111us -> RecoverOk
            | 120us -> NackData.parse payload |> Nack
            | x -> failwith (sprintf "%A not implemented" x)
            |> Some
        | _ -> None
    static member pickle (x: Basic) = [|
        yield! fromShort 60us
        match x with
        | Qos data -> yield! fromShort 10us; yield! QosData.pickle data
        | QosOk -> yield! fromShort 11us
        | Consume data -> yield! fromShort 20us; yield! ConsumeData.pickle data
        | ConsumeOk data -> yield! fromShort 21us; yield! ConsumeOkData.pickle data
        | Cancel data -> yield! fromShort 30us; yield! CancelData.pickle data
        | CancelOk data -> yield! fromShort 31us; yield! CancelOkData.pickle data
        | Publish data -> yield! fromShort 40us; yield! PublishData.pickle data
        | Return data -> yield! fromShort 50us; yield! ReturnData.pickle data
        | Deliver data -> yield! fromShort 60us; yield! DeliverData.pickle data
        | Get data -> yield! fromShort 70us; yield! GetData.pickle data
        | GetOk data -> yield! fromShort 71us; yield! GetOkData.pickle data
        | GetEmpty data -> yield! fromShort 72us; yield! GetEmptyData.pickle data
        | Ack data -> yield! fromShort 80us; yield! AckData.pickle data
        | Reject data -> yield! fromShort 90us; yield! RejectData.pickle data
        | RecoverAsync data -> yield! fromShort 100us; yield! RecoverAsyncData.pickle data
        | Recover data -> yield! fromShort 110us; yield! RecoverData.pickle data
        | RecoverOk -> yield! fromShort 111us
        | Nack data -> yield! fromShort 120us; yield! NackData.pickle data
    |]

let (|Basic|_|) = Basic.parse
