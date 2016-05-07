module Connection
open Amqp


let classId = 10us


type StartData = {
    VersionMajor: Octet
    VersionMinor: Octet
    ServerProperties: Table
    Mechanisms: LongStr
    Locales: LongStr
} with
    static member id = 10us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, versionMajor = readOctet payload off
        let off, versionMinor = readOctet payload off
        let off, serverProperties = readTable payload off
        let off, mechanisms = readLongStr payload off
        let off, locales = readLongStr payload off
        {
            VersionMajor = versionMajor
            VersionMinor = versionMinor
            ServerProperties = serverProperties
            Mechanisms = mechanisms
            Locales = locales
        }
    static member pickle (x: StartData) =
        [|
            yield! writeOctet x.VersionMajor
            yield! writeOctet x.VersionMinor
            yield! writeTable x.ServerProperties
            yield! writeLongStr x.Mechanisms
            yield! writeLongStr x.Locales
        |]


type StartOkData = {
    ClientProperties: Table
    Mechanism: ShortStr
    Response: LongStr
    Locale: ShortStr
} with
    static member id = 11us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, clientProperties = readTable payload off
        let off, mechanism = readShortStr payload off
        let off, response = readLongStr payload off
        let off, locale = readShortStr payload off
        {
            ClientProperties = clientProperties
            Mechanism = mechanism
            Response = response
            Locale = locale
        }
    static member pickle (x: StartOkData) =
        [|
            yield! writeTable x.ClientProperties
            yield! writeShortStr x.Mechanism
            yield! writeLongStr x.Response
            yield! writeShortStr x.Locale
        |]


type SecureData = {
    Challenge: LongStr
} with
    static member id = 20us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, challenge = readLongStr payload off
        {
            Challenge = challenge
        }
    static member pickle (x: SecureData) =
        [|
            yield! writeLongStr x.Challenge
        |]


type SecureOkData = {
    Response: LongStr
} with
    static member id = 21us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, response = readLongStr payload off
        {
            Response = response
        }
    static member pickle (x: SecureOkData) =
        [|
            yield! writeLongStr x.Response
        |]


type TuneData = {
    ChannelMax: Short
    FrameMax: Long
    Heartbeat: Short
} with
    static member id = 30us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, channelMax = readShort payload off
        let off, frameMax = readLong payload off
        let off, heartbeat = readShort payload off
        {
            ChannelMax = channelMax
            FrameMax = frameMax
            Heartbeat = heartbeat
        }
    static member pickle (x: TuneData) =
        [|
            yield! writeShort x.ChannelMax
            yield! writeLong x.FrameMax
            yield! writeShort x.Heartbeat
        |]


type TuneOkData = {
    ChannelMax: Short
    FrameMax: Long
    Heartbeat: Short
} with
    static member id = 31us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, channelMax = readShort payload off
        let off, frameMax = readLong payload off
        let off, heartbeat = readShort payload off
        {
            ChannelMax = channelMax
            FrameMax = frameMax
            Heartbeat = heartbeat
        }
    static member pickle (x: TuneOkData) =
        [|
            yield! writeShort x.ChannelMax
            yield! writeLong x.FrameMax
            yield! writeShort x.Heartbeat
        |]


type OpenData = {
    VirtualHost: ShortStr
    Reserved1: ShortStr
    Reserved2: Bit
} with
    static member id = 40us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, virtualHost = readShortStr payload off
        let off, reserved1 = readShortStr payload off
        let bit, off, reserved2 = readBit payload off bit
        let bit = 0
        {
            VirtualHost = virtualHost
            Reserved1 = reserved1
            Reserved2 = reserved2
        }
    static member pickle (x: OpenData) =
        [|
            yield! writeShortStr x.VirtualHost
            yield! writeShortStr x.Reserved1
            let bits = [ x.Reserved2 ]
            yield! writeBits bits
        |]


type OpenOkData = {
    Reserved1: ShortStr
} with
    static member id = 41us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reserved1 = readShortStr payload off
        {
            Reserved1 = reserved1
        }
    static member pickle (x: OpenOkData) =
        [|
            yield! writeShortStr x.Reserved1
        |]


type CloseData = {
    ReplyCode: Short
    ReplyText: ShortStr
    ClassId: Short
    MethodId: Short
} with
    static member id = 50us
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




type BlockedData = {
    Reason: ShortStr
} with
    static member id = 60us
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let off, reason = readShortStr payload off
        {
            Reason = reason
        }
    static member pickle (x: BlockedData) =
        [|
            yield! writeShortStr x.Reason
        |]




type Connection =
    | Start of StartData
    | StartOk of StartOkData
    | Secure of SecureData
    | SecureOk of SecureOkData
    | Tune of TuneData
    | TuneOk of TuneOkData
    | Open of OpenData
    | OpenOk of OpenOkData
    | Close of CloseData
    | CloseOk
    | Blocked of BlockedData
    | Unblocked
with
    static member parse (payload: byte []) =
        match toShort payload 0 with
        | 10us ->
            match toShort payload 2 with
            | 10us -> StartData.parse payload |> Start
            | 11us -> StartOkData.parse payload |> StartOk
            | 20us -> SecureData.parse payload |> Secure
            | 21us -> SecureOkData.parse payload |> SecureOk
            | 30us -> TuneData.parse payload |> Tune
            | 31us -> TuneOkData.parse payload |> TuneOk
            | 40us -> OpenData.parse payload |> Open
            | 41us -> OpenOkData.parse payload |> OpenOk
            | 50us -> CloseData.parse payload |> Close
            | 51us -> CloseOk
            | 60us -> BlockedData.parse payload |> Blocked
            | 61us -> Unblocked
            | x -> failwith (sprintf "%A not implemented" x)
            |> Some
        | _ -> None
    static member pickle (x: Connection) = [|
        yield! fromShort 10us
        match x with
        | Start data -> yield! fromShort 10us; yield! StartData.pickle data
        | StartOk data -> yield! fromShort 11us; yield! StartOkData.pickle data
        | Secure data -> yield! fromShort 20us; yield! SecureData.pickle data
        | SecureOk data -> yield! fromShort 21us; yield! SecureOkData.pickle data
        | Tune data -> yield! fromShort 30us; yield! TuneData.pickle data
        | TuneOk data -> yield! fromShort 31us; yield! TuneOkData.pickle data
        | Open data -> yield! fromShort 40us; yield! OpenData.pickle data
        | OpenOk data -> yield! fromShort 41us; yield! OpenOkData.pickle data
        | Close data -> yield! fromShort 50us; yield! CloseData.pickle data
        | CloseOk -> yield! fromShort 51us
        | Blocked data -> yield! fromShort 60us; yield! BlockedData.pickle data
        | Unblocked -> yield! fromShort 61us
    |]

let (|Connection|_|) = Connection.parse