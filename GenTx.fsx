module Tx
#if INTERACTIVE
#load "Amqp.fsx"
#endif
open Amqp














type Tx =
    | Select
    | SelectOk
    | Commit
    | CommitOk
    | Rollback
    | RollbackOk
with
    static member parse (payload: byte []) =
        match toShort payload 0, toShort payload 2 with
        | 90us, 10us -> Select
        | 90us, 11us -> SelectOk
        | 90us, 20us -> Commit
        | 90us, 21us -> CommitOk
        | 90us, 30us -> Rollback
        | 90us, 31us -> RollbackOk
        | x -> failwith (sprintf "%A not implemented" x)
    static member pickle (x: Tx) = [|
        yield! fromShort 90us
        match x with
        | Select -> yield! fromShort 10us
        | SelectOk -> yield! fromShort 11us
        | Commit -> yield! fromShort 20us
        | CommitOk -> yield! fromShort 21us
        | Rollback -> yield! fromShort 30us
        | RollbackOk -> yield! fromShort 31us
    |]