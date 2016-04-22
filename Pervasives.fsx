[<AutoOpen>]
module Pervasives
open System
module Option =
    let protect f =
        try f() |> Some with _ -> None

type DateTime with
    static member fromUnix (t: uint64) =
        DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds (float t)
    static member toUnix (t: DateTime) : uint64 =
        let origin = new DateTime(1970, 1, 1, 0, 0, 0, 0)
        let diff = t - origin
        uint64 diff.TotalSeconds

let (</>) a b = System.IO.Path.Combine(a,  b)

let mapSnd f (a, b) = a, f b
let mapFst f (a, b) = f a, b

let (++) = Array.append

let (|Item|_|) = Map.tryFind

let (|AsUTF8|_|) (data : byte[]) =
    try System.Text.Encoding.UTF8.GetString(data) |> Some
    with _ -> None

let (|SplitBy|) (del: string) (s: string) =
    s.Split([|del|], StringSplitOptions.RemoveEmptyEntries)
    |> Array.toList

let asLongLong (data: byte[]) offset =
    let data = data.[offset .. offset + 7]
    BitConverter.ToUInt64(Array.rev data, 0)//network byte order
let asLongLongInt (data: byte[]) offset =
    let data = data.[offset .. offset + 7]
    BitConverter.ToInt64(Array.rev data, 0)//network byte order
let asLong (data: byte[]) offset =
    let data = data.[offset .. offset + 3]
    BitConverter.ToUInt32(Array.rev data, 0)//network byte order
let toLongInt (data: byte[]) offset =
    let data = data.[offset .. offset + 3]
    BitConverter.ToInt32(Array.rev data, 0)//network byte order
let toFloat (data: byte[]) offset =
    let data = data.[offset .. offset + 3]
    BitConverter.ToSingle(Array.rev data, 0)//network byte order
let toDouble (data: byte[]) offset =
    let data = data.[offset .. offset + 7]
    BitConverter.ToDouble(Array.rev data, 0)//network byte order
let toShort (data: byte[]) offset =
    let data = data.[offset .. offset + 1]
    BitConverter.ToUInt16(Array.rev data, 0)
let toShortInt (data: byte[]) offset =
    let data = data.[offset .. offset + 1]
    BitConverter.ToInt16(Array.rev data, 0)

let fromShort (v: UInt16) =
    BitConverter.GetBytes v |> Array.rev
let fromLong (v: UInt32) =
    BitConverter.GetBytes v |> Array.rev
let fromLongLong (v: UInt64) =
    BitConverter.GetBytes v |> Array.rev

let (|LongLong|_|) (data: byte[]) =
    Option.protect (fun() -> asLongLong data 0)

let (|Long|_|) (data: byte[]) =
    Option.protect (fun() -> asLong data 0)

let (|Short|_|) (data: byte[]) =
    Option.protect (fun() -> toShort data 0)
