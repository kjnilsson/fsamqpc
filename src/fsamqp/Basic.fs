module BasicExt
open Amqp

let classId = 60us

type BasicPropsData = Map<string, Field>

let parse (payload: byte []) : BasicPropsData =
    let off = 12
    let bit = 0
    let off, flags = readShort payload off
    let table: Table = Map.empty
    let tableAdd index map key f off = 
        if isSet flags index then 
            let off, x = f off
            off, Map.add key x map  
        else off, map
    let off, table = tableAdd 0 table "content-type" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 1 table "content-encoding" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 2 table "headers" (readTable payload >> mapSnd TableField) off
    let off, table = tableAdd 3 table "delivery-mode" (readOctet payload >> mapSnd OctetField) off
    let off, table = tableAdd 4 table "priority" (readOctet payload >> mapSnd OctetField) off
    let off, table = tableAdd 5 table "correlation-id" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 6 table "reply-to" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 7 table "expiration" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 8 table "message-id" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 9 table "timestamp" (readTimestamp payload >> mapSnd TimestampField) off
    let off, table = tableAdd 10 table "type" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 11 table "user-id" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 12 table "app-id" (readShortStr payload >> mapSnd ShortStrField) off
    let off, table = tableAdd 13 table "cluster-id" (readShortStr payload >> mapSnd ShortStrField) off
    table

let rec writeTableFieldValue f = [|
    match f with
    | BooleanField f -> yield writeBoolean f 
    | ShortShortField f -> yield byte f 
    | OctetField f -> yield f
    | ShortIntField f -> yield! writeShortInt f 
    | ShortField f -> yield! writeShort f
    | LongIntField f -> yield! writeLongInt f
    | LongField f -> yield! writeLong f 
    | LongLongIntField f -> yield! writeLongLongInt f
    | LongLongField f -> yield! writeLongLong f
    | FloatField f -> yield! writeFloat f
    | DoubleField f -> yield! writeDouble f
    | DecimalField f -> yield! f //TODO
    | ShortStrField f -> yield! writeShortStr f 
    | LongStrField f -> yield! writeLongStr f 
    | TimestampField f -> yield! writeTimestamp f 
    | TableField f -> yield! writeTable f
    | NoField -> yield byte 'V'
    |]


let pickle (props: BasicPropsData) =
    let fields = [
        "content-type"
        "content-encoding"
        "headers"
        "delivery-mode" 
        "priority"
        "correlation-id"
        "reply-to" 
        "expiration" 
        "message-id" 
        "timestamp" 
        "type" 
        "user-id" 
        "app-id" 
        "cluster-id"  
    ]
    let _, flags, data =
        fields 
        |> List.fold (fun (i, s, data) v -> 
            match Map.tryFind v props with
            | Some x -> i+1, setBit i s, writeTableFieldValue x :: data
            | None -> i+1, s, data) (0, 0us, [])
    [|
        yield! writeShort flags
        yield! List.fold (++) Array.empty (data |> List.rev)
    |]

type BasicContentHeader =
    { BodySize: LongLong
      Props: BasicPropsData }
    static member parse (payload: byte[]) =
        match readShort payload 0 with
        | _, 60us ->
            let off, size = readLongLong payload 4
            let props = parse payload
            { BodySize = size
              Props = props }
            |> Some
        | _ -> None
    static member pickle (x: BasicContentHeader) =
        [|
            yield! writeShort 60us
            yield! writeShort 0us
            yield! writeLongLong x.BodySize
            yield! pickle x.Props
        |]

let (|BasicContentHeader|_|) = BasicContentHeader.parse

let test () =
    let p = ["content-type", ShortStrField "json"] |> Map.ofList 
    let payload = Array.zeroCreate 12 ++ pickle p
    let off = 12
    let bit = 0
    let off, flags = readShort payload off
    let withDef index f payload off def = 
        if isSet flags index then f payload off else off, def
    let off, contentType = withDef 0 readShortStr payload off ""
    let off, contentEncoding = withDef 1 readShortStr payload off ""
    let off, headers = withDef 2 readTable payload off Map.empty
    payload |> parse

