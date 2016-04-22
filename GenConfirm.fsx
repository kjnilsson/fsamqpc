module Confirm
#if INTERACTIVE
#load "Amqp.fsx"
#endif
open Amqp


type SelectData = {
    Nowait: Bit
} with
    static member parse (payload: byte []) =
        let off = 4
        let bit = 0
        let bit, off, nowait = readBit payload off bit
        let bit = 0
        {
            Nowait = nowait
        }
    static member pickle (x: SelectData) =
        [|
            let bits = [ x.Nowait ]
            yield! writeBits bits
        |]




type Confirm =
    | Select of SelectData
    | SelectOk
with
    static member parse (payload: byte []) =
        match toShort payload 0, toShort payload 2 with
        | 85us, 10us -> SelectData.parse payload |> Select
        | 85us, 11us -> SelectOk
        | x -> failwith (sprintf "%A not implemented" x)
    static member pickle (x: Confirm) = [|
        yield! fromShort 85us
        match x with
        | Select data -> yield! fromShort 10us; yield! SelectData.pickle data
        | SelectOk -> yield! fromShort 11us
    |]