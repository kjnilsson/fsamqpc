
#if INTERACTIVE
#load "Pervasives.fsx"
#endif
//gen
module Casing =
    open System.Text
    let (|ToUpper|) = System.Char.ToUpper
    let rec private upper (sb: StringBuilder) chars =
        match chars with
        | [] -> sb.ToString()
        | ToUpper c :: rest -> 
            c |> sb.Append |> ignore
            next sb rest
    and next sb chars =
        match chars with
        | [] -> sb.ToString()
        | '-' :: rest -> upper sb rest
        | c :: rest ->
            sb.Append c |> ignore
            next sb rest
    let pascal (s: string) =
        let sb = new StringBuilder()
        s.ToCharArray()
        |> Array.toList
        |> upper sb
    let camel (s: string) =
        let sb = new StringBuilder()
        s.ToCharArray()
        |> Array.toList
        |> next sb
        
Casing.pascal "start-ok"
Casing.camel "start-ok"

type GenType =
    | Bit
    | Octet
    | Short
    | Long
    | LongLong
    | ShortStr
    | LongStr
    | Timestamp
    | Table
    static member parse =
        function
        | "bit" -> Bit
        | "octet" -> Octet
        | "short" -> Short
        | "long" -> Long
        | "longlong" -> LongLong
        | "shortstr" -> ShortStr
        | "longstr"-> LongStr
        | "timestamp" -> Timestamp
        | "peer-properties" -> Table
        | "table" -> Table
        | x -> failwith (sprintf "invalid gentype %s" x)

type GenMethod =
    { Name: string
      Index: int
      Fields: (string * GenType) list }
type GenClass =
    { Name: string
      Index: int
      Properties: (string * GenType) list
      Methods: GenMethod list }

#r "System.Xml.Linq"
open System.Xml.Linq
open System.IO
let xn n = XName.Get n
let xml = File.ReadAllText (__SOURCE_DIRECTORY__ </> "amqp0-9-1.stripped.xml")  |> XElement.Parse

let domains = 
    xml.Elements (xn "domain") 
    |> Seq.map (fun e ->
        let name = e.Attribute(xn "name")
        let t = e.Attribute(xn "type")
        name.Value, t.Value)
    |> Map.ofSeq
        

let parseXml (xml: XElement) =
    let getFields (e: XElement) =
        match e.Elements (xn "field") with
        | null -> []
        | fields ->
            fields
            |> Seq.toList
            |> List.choose (fun e ->
                match e.Attribute (xn "reserved") with
                | null ->
                    let name = (e.Attribute (xn "name")).Value
                    let domain = 
                        match e.Attribute (xn "domain") with
                        | null -> e.Attribute (xn "type") 
                        | d -> d
                    let domain = Map.find domain.Value domains 
                    Some (name, GenType.parse domain)
                | _ -> None)
    xml.Descendants (xn "class")
    |> Seq.map (fun c ->
        let name = c.Attribute(xn "name")
        let index = c.Attribute(xn "index")
        let fields = getFields c
        let methods = 
            c.Elements(xn "method") |> Seq.toList
            |> List.map (fun e ->
                let name = e.Attribute (xn "name")
                let index = e.Attribute (xn "index")
                let fields = getFields e
                { Name = name.Value
                  Index = System.Int32.Parse index.Value
                  Fields = fields })
        { Name = name.Value
          Index = System.Int32.Parse index.Value
          Properties = fields
          Methods = methods })

let fsharpSafe =
    function
    | "type" | "global" as x ->
        sprintf "%s'" x
    | x -> x

let camel x =
    Casing.camel x |> fsharpSafe

let genMethodParse name (fields: (string * GenType) list) =
    let typeName = Casing.pascal name
    [   yield sprintf "    static member parse (payload: byte []) ="
        match fields with
        | [] ->
            yield sprintf "        %s" typeName
        | _ ->
            yield "        let off = 0"
            yield "        let bit = 0"
            let rec bits agg rem off b =
                match rem with
                | (n, Bit) :: rem ->
                    let agg = sprintf "        let bit, off, %s = readBit payload off bit" (camel n) :: agg
                    bits agg rem off (b+1)
                | _ -> 
                    let agg = "        let bit = 0" :: agg
                    read agg rem off
            and read agg rem off =
                match rem with
                | [] -> List.rev agg
                | (n, Bit) :: _ -> 
                    bits agg rem off 0 
                | (n, t) :: rem -> 
                    let agg = sprintf "        let off, %s = read%A payload off" (camel n) t :: agg
                    read agg rem off
            yield! read [] fields 0
            yield "        {"
            for n, t in fields do
                yield sprintf "            %s = %s" (Casing.pascal n) (camel n)
            yield "        }"
            ]

let rec gather agg rem =
    match rem, agg with
    | [], _ -> agg
    | (_, Bit) :: _, agg ->
        bits agg rem
    | h :: rem, c :: aggRem ->
        gather ((h :: c) :: aggRem) rem
    | h :: rem, [] ->
        gather ((h :: []) :: []) rem
and bits agg rem =
    match rem, agg with
    | [], _ -> agg
    | (n, Bit) :: rem, c :: aggRem ->
        bits (((n, Bit) :: c) :: aggRem) rem
    | (n, Bit) :: rem, [] ->
        bits (((n, Bit) :: []) :: []) rem
    | _ ->
        gather ([] :: agg) rem
        
let genMethodPickle name (fields: (string * GenType) list) =
    let typeName = Casing.pascal name
    [   yield sprintf "    static member pickle (x: %s) =" typeName
        match fields with
        | [] ->
            yield sprintf "        %s" typeName
        | _ ->
            yield "        [|"
            for g in gather [] fields do
                match g.[0] with
                | _, Bit -> //do bits
                    let text = g |> List.map (fst >> Casing.pascal) |> (fun s -> System.String.Join("; ", s))
                    yield (sprintf "            let bits = [ %s ]" text)
                    yield "            yield! writeBits bits"
                | _ -> //do others
                    for (n, t) in g do
                        yield sprintf "            yield! write%A x.%s" t (Casing.pascal n)
            yield "        |]"
            ]
let genMethod name fields =
    let typeName = Casing.pascal name
    match fields with
    | [] ->
        [ sprintf "type %s = %s" typeName typeName ]
    | _ ->
        [ yield sprintf "type %s = {" typeName
          for n, t in fields do
            yield sprintf "    %s: %A" (Casing.pascal n) t 
          yield "} with"
          yield! genMethodParse name fields
          yield! genMethodPickle name fields ]

let genClass (m: GenClass) =
    [ yield sprintf "module %s" (Casing.pascal m.Name)
      yield "#if INTERACTIVE"
      yield "#load \"Amqp.fsx\""
      yield "#endif"
      yield "open Amqp"
      yield "\r\n"
      match m.Properties with
      | [] -> ()
      | props ->
        yield! genMethod (sprintf "%sProps" (camel m.Name)) props
        yield "\r\n"
      for m in m.Methods do
        yield! genMethod m.Name m.Fields
        yield "\r\n" ]
    |> fun x -> System.String.Join("\r\n", x)

parseXml xml
|> Seq.toList
|> List.iter (fun c -> 
    let text = genClass c
    System.IO.File.WriteAllText("Gen" + Casing.pascal c.Name + ".fsx", text))
    

