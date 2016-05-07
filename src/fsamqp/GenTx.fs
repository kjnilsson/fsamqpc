module Tx
open Amqp


let classId = 90us














type Tx =
    | Select
    | SelectOk
    | Commit
    | CommitOk
    | Rollback
    | RollbackOk
with
    static member parse (payload: byte []) =
        match toShort payload 0 with
        | 90us ->
            match toShort payload 2 with
            | 10us -> Select
            | 11us -> SelectOk
            | 20us -> Commit
            | 21us -> CommitOk
            | 30us -> Rollback
            | 31us -> RollbackOk
            | x -> failwith (sprintf "%A not implemented" x)
            |> Some
        | _ -> None
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

let (|Tx|_|) = Tx.parse