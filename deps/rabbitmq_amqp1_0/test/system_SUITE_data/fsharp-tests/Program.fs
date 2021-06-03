// Learn more about F# at http://fsharp.org

open System
open System.Threading
open Amqp
open Amqp.Sasl
open Amqp.Framing
open Amqp.Types

let sleep (i: int) = System.Threading.Thread.Sleep i

[<AutoOpen>]
module RabbitClient =
    open RabbitMQ.Client

    let consume (m: IModel) queue autoAck f =
            let consumer =
                { new DefaultBasicConsumer(m) with
                    member x.HandleBasicDeliver(consumerTag,
                                                deliveryTag,
                                                redelivered,
                                                exchange,
                                                routingKey,
                                                props,
                                                body) =
                                                f deliveryTag props body }
            m.ContinuationTimeout <- (TimeSpan.FromSeconds 115.)
            (* m.BasicQos(0u, 100us, false) |> ignore *)
            let consumerTag = m.BasicConsume(queue, autoAck, consumer)
            { new System.IDisposable with
                member __.Dispose () =
                    m.BasicCancel(consumerTag) }

[<AutoOpen>]
module AmqpClient =
    type AmqpConnection =
        { Conn : Connection
          Session: Session }
        interface IDisposable with
            member x.Dispose() =
                try
                    x.Conn.Close()
                    x.Session.Close()
                with _ -> ()

    let connect uri =
        let c = Address uri |> Connection
        let s = Session c
        { Conn = c; Session = s }

    let connectWithOpen uri opn =
        let c = Connection(Address uri, null, opn, null)
        let s = Session c
        { Conn = c; Session = s }

    let senderReceiver ac name address =
        let s = SenderLink(ac.Session, name + "-sender" , address)
        let r = ReceiverLink(ac.Session, name + "-receiver", address)
        r.SetCredit(100, true)
        s, r

    let receive (receiver: ReceiverLink) =
        let rtd = receiver.Receive()
        receiver.Accept rtd
        rtd

    let amqpSequence xs =
        let l = Amqp.Types.List();
        xs |> List.iter (l.Add >> ignore)
        AmqpSequence(List = l)

[<AutoOpen>]
module Test =
    let assertEqual a b =
        if a <> b then
            failwith (sprintf "Expected: %A\r\nGot: %A" a b)

    let assertTrue b =
        if not b then
            failwith (sprintf "Expected True got False!")

    let sampleTypes =
        ["hi" :> obj
         "" :> obj
         "hi"B :> obj
         ""B :> obj
         Array.create 1000 50uy :> obj
         true :> obj
         0y :> obj
         0uy :> obj
         Byte.MaxValue :> obj
         0s :> obj
         Int16.MaxValue :> obj
         0 :> obj
         Int32.MaxValue :> obj
         0L :> obj
         Int64.MaxValue :> obj
         0us :> obj
         UInt16.MaxValue :> obj
         0u :> obj
         UInt32.MaxValue :> obj
         0ul :> obj
         UInt64.MaxValue :> obj
         null :> obj
         "\uFFF9" :> obj
         Amqp.Types.Symbol("Symbol") :> obj
         DateTime.Parse("2008-11-01T19:35:00.0000000Z").ToUniversalTime() :> obj
         Guid("f275ea5e-0c57-4ad7-b11a-b20c563d3b71") :> obj
        ]

    let testOutcome uri (attach: Attach) (cond: string) =
        use ac = connect uri
        let trySet (mre: AutoResetEvent) =
            try mre.Set() |> ignore with _ -> ()

        use mre = new System.Threading.AutoResetEvent(false)
        let mutable errorName = null
        ac.Session.add_Closed (
            new ClosedCallback (fun o err -> errorName <- string err.Condition; trySet mre))

        let attached = new OnAttached (
                        fun l attach -> errorName <- null; trySet mre)

        let receiver = ReceiverLink(ac.Session, "test-receiver", attach, attached)
        mre.WaitOne(1000) |> ignore
        if cond = null then
            receiver.Close()
        assertEqual cond errorName

    let roundtrip uri =
        use c = connect uri
        let sender, receiver = senderReceiver c "test" "roundtrip-q"
        for body in sampleTypes do
            let corr = "correlation"
            new Message(body,
                        Header = Header(Ttl = 500u),
                        Properties = new Properties(CorrelationId = corr))
            |> sender.Send
            let rtd = receive receiver
            assertEqual body rtd.Body
            assertTrue (rtd.Header.Ttl <= 500u)
            assertEqual rtd.Properties.CorrelationId  corr
            ()

    open RabbitMQ.Client

    let roundtrip_to_amqp_091 uri =
        use c = connect uri
        let q = "roundtrip-091-q"
        let corr = "corrlation"
        let sender = SenderLink(c.Session, q + "-sender" , q)
        new Message("hi"B, Header = Header(),
                    Properties = new Properties(CorrelationId = corr))
        |> sender.Send
        System.Threading.Thread.Sleep 500

        let cf = ConnectionFactory()
        cf.Uri <- Uri uri
        use c = cf.CreateConnection()
        use m = c.CreateModel()
        use h = new AutoResetEvent(false)
        let mutable id : string = null
        let con = consume m q false (fun deliveryTag props body ->
                                        printfn "got %A" props.CorrelationId
                                        id <- props.CorrelationId
                                        h.Set() |> ignore
                                        m.BasicAck(deliveryTag, false) |> ignore)

        h.WaitOne() |> ignore
        assertEqual id corr
        ()

    let defaultOutcome uri =
        for (defOut, cond, defObj) in
            ["amqp:accepted:list", null, Accepted() :> Outcome
             "amqp:rejected:list", null, Rejected() :> Outcome
             "amqp:released:list", null, Released() :> Outcome] do

            let source = new Source(Address = "default_outcome_q",
                                    DefaultOutcome = defObj)
            let attach = new Attach (Source = source,
                                     Target = Target())

            testOutcome uri attach cond

    let outcomes uri =
        for (outcome, cond) in
            ["amqp:accepted:list", null
             "amqp:rejected:list", null
             "amqp:released:list", null
             "amqp:modified:list", null
             "amqp:madeup:list", "amqp:not-implemented"] do

            let source = new Source(Address = "outcomes_q",
                                    Outcomes = [| Symbol outcome |])
            let attach = new Attach (Source = source,
                                     Target = Target())

            testOutcome uri attach cond


    let fragmentation uri =
        for frameSize, size in
            [512u, 512
             512u, 600
             512u, 1024
             1024u, 1024] do
            let addr = Address uri
            let opn = Open(ContainerId = Guid.NewGuid().ToString(),
                           HostName = addr.Host, ChannelMax = 256us,
                           MaxFrameSize = frameSize)
            use c = connectWithOpen uri opn
            let sender, receiver = senderReceiver c "test" "framentation-q"
            let m = new Message(String.replicate size "a")
            sender.Send m
            let m' = receive receiver
            assertEqual (m.Body) (m'.Body)

    let messageAnnotations uri =
        use c = connect uri
        let sender, receiver = senderReceiver c "test" "annotations-q"
        let ann = MessageAnnotations()
        let k1 = Symbol "key1"
        let k2 = Symbol "key2"
        ann.[Symbol "key1"] <- "value1"
        ann.[Symbol "key2"] <- "value2"
        let m = new Message("testing annotations", MessageAnnotations = ann)
        sender.Send m
        let m' = receive receiver

        assertEqual m.Body m'.Body
        assertEqual (m.MessageAnnotations.Descriptor) (m'.MessageAnnotations.Descriptor)
        assertEqual 2 (m'.MessageAnnotations.Map.Count)
        assertTrue (m.MessageAnnotations.[k1] = m'.MessageAnnotations.[k1])
        assertTrue (m.MessageAnnotations.[k2] = m'.MessageAnnotations.[k2])

    let footer uri =
        use c = connect uri
        let sender, receiver = senderReceiver c "test" "footer-q"
        let footer = Footer()
        let k1 = Symbol "key1"
        let k2 = Symbol "key2"
        footer.[Symbol "key1"] <- "value1"
        footer.[Symbol "key2"] <- "value2"
        let m = new Message("testing annotations", Footer = footer)
        sender.Send m
        let m' = receive receiver

        assertEqual m.Body m'.Body
        assertEqual (m.Footer.Descriptor) (m'.Footer.Descriptor)
        assertEqual 2 (m'.Footer.Map.Count)
        assertTrue (m.Footer.[k1] = m'.Footer.[k1])
        assertTrue (m.Footer.[k2] = m'.Footer.[k2])

    let datatypes uri =
        use c = connect uri
        let sender, receiver = senderReceiver c "test" "datatypes-q"
        let aSeq = amqpSequence sampleTypes
        (new Message(aSeq)) |> sender.Send
        let rtd = receive receiver
        let amqpSeq = rtd.Body :?> AmqpSequence
        for a in amqpSeq.List do
            List.exists ((=) a) sampleTypes |> assertTrue

    let reject uri =
        use c = connect uri
        let sender, receiver = senderReceiver c "test" "reject-q"
        new Message "testing reject" |> sender.Send
        let m = receiver.Receive()
        receiver.Reject(m)
        assertEqual null (receiver.Receive(TimeSpan.FromMilliseconds 100.))

    let redelivery uri =
        use c = connect uri
        let sender, receiver = senderReceiver c "test" "redelivery-q"
        new Message "testing redelivery" |> sender.Send
        let m = receiver.Receive()
        assertTrue (m.Header.FirstAcquirer)
        receiver.Close()
        c.Session.Close()
        let session = Session(c.Conn)
        let receiver = ReceiverLink(session, "test-receiver", "redelivery-q")

        let m' = receive receiver
        assertEqual (m.Body :?> string) (m'.Body :?> string)
        assertTrue (not m'.Header.FirstAcquirer)
        assertEqual null (receiver.Receive(TimeSpan.FromMilliseconds 100.))
        session.Close()

    let routing uri =
        for target, source, routingKey, succeed in
            ["/queue/test", "test", "", true
             "test", "/queue/test", "", true
             "test", "test", "", true

             "/topic/a.b.c.d",      "/topic/#.c.*",              "",        true
             "/exchange/amq.topic", "/topic/#.c.*",              "a.b.c.d", true
             "/topic/w.x.y.z",      "/exchange/amq.topic/#.y.*", "",        true
             "/exchange/amq.topic", "/exchange/amq.topic/#.y.*", "w.x.y.z", true

             "/exchange/amq.fanout",  "/exchange/amq.fanout/",  "",  true
             "/exchange/amq.direct",  "/exchange/amq.direct/",  "",  true
             "/exchange/amq.direct",  "/exchange/amq.direct/a", "a", true

             (* FIXME: The following three tests rely on the queue "test"
              * created by previous tests in this function. *)
             "/queue/test",     "/amq/queue/test", "", true
             "/amq/queue/test", "/queue/test",     "", true
             "/amq/queue/test", "/amq/queue/test", "", true

             (* The following tests verify that a queue created out-of-band
              * in AMQP is reachable from the AMQP 1.0 world. Queues are created
              * from the common_test suite. *)
             "/amq/queue/transient_q", "/amq/queue/transient_q", "", true
             "/amq/queue/durable_q",   "/amq/queue/durable_q",   "", true
             "/amq/queue/quorum_q",   "/amq/queue/quorum_q",   "", true
             "/amq/queue/stream_q",   "/amq/queue/stream_q",   "", true
             "/amq/queue/autodel_q",   "/amq/queue/autodel_q",   "", true] do

            let rnd = Random()
            use c = connect uri
            let sender = SenderLink(c.Session, "test-sender", target)
            let receiver = ReceiverLink(c.Session, "test-receiver", source)
            receiver.SetCredit(100, true)
            use m = new Message(rnd.Next(10000), Properties = Properties(Subject = routingKey))
            sender.Send m
            (* printfn "%s %s %s %A" target source routingKey succeed *)

            if succeed then
                let m' = receiver.Receive(TimeSpan.FromMilliseconds 3000.)
                receiver.Accept m'
                assertTrue (m' <> null)
                assertEqual (m.Body :?> int) (m'.Body :?> int)
            else
                use m' = receiver.Receive(TimeSpan.FromMilliseconds 100.)
                assertEqual null m'



    let invalidRoutes uri =

        for dest, cond in
            ["/exchange/missing", "amqp:not-found"
             "/fruit/orange", "amqp:invalid-field"] do
            use ac = connect uri
            let trySet (mre: AutoResetEvent) =
                try mre.Set() |> ignore with _ -> ()

            let mutable errorName = null
            use mre = new System.Threading.AutoResetEvent(false)
            ac.Session.add_Closed (
                new ClosedCallback (fun _ err -> errorName <- err.Condition; trySet mre))

            let attached = new OnAttached (fun _ _ -> trySet mre)

            let sender = new SenderLink(ac.Session, "test-sender",
                                        Target(Address = dest), attached);
            mre.WaitOne() |> ignore

            try
                let receiver = ReceiverLink(ac.Session, "test-receiver", dest)
                receiver.Close()
            with
            | :? Amqp.AmqpException as ae ->
                assertEqual (ae.Error.Condition) (Symbol cond)
            | _ -> failwith "invalid expection thrown"

    let authFailure uri =
        try
            let u = Uri uri
            let uri = sprintf "amqp://blah:blah@%s:%i" u.Host u.Port
            let c = Connection(Address uri)
            failwith "expected exception not received"
        with
        | :? Amqp.AmqpException ->
            ()

    let accessFailureSend uri =
        try
            let u = Uri uri
            let uri = sprintf "amqp://access_failure:boo@%s:%i" u.Host u.Port
            use ac = connect uri
            let dest = "/amq/queue/test"
            ac.Session.add_Closed (
                new ClosedCallback (fun _ err -> printfn "session err %A" err.Condition
                ))
            let sender = new SenderLink(ac.Session, "test-sender", dest)
            sender.Send(new Message "hi", TimeSpan.FromSeconds 15.)


            failwith "expected exception not received"
        with
        | :? Amqp.AmqpException as ex ->
            printfn "Exception %A" ex
            ()

    let accessFailure uri =
        try
            let u = Uri uri
            let uri = sprintf "amqp://access_failure:boo@%s:%i" u.Host u.Port
            use ac = connect uri
            let dest = "/amq/queue/test"
            let receiver = ReceiverLink(ac.Session, "test-receiver", dest)
            receiver.Receive()
            receiver.Close()
            failwith "expected exception not received"
        with
        | :? Amqp.AmqpException as ex ->
            printfn "Exception %A" ex
            ()

    let accessFailureNotAllowed uri =
        try
            let u = Uri uri
            let uri = sprintf "amqp://access_failure_not_allowed:boo@%s:%i" u.Host u.Port
            use ac = connect uri
            let dest = "/amq/queue/test"
            let receiver = ReceiverLink(ac.Session, "test-receiver", dest)
            receiver.Receive()
            receiver.Close()
            failwith "expected exception not received"
        with
        | :? Amqp.AmqpException as ex ->
            printfn "Exception %A" ex
            ()
        | :? System.ObjectDisposedException as ex ->
            printfn "Exception %A" ex
            ()

let (|AsLower|) (s: string) =
    match s with
    | null -> null
    | _ -> s.ToLowerInvariant()

[<EntryPoint>]
let main argv =
    match List.ofArray argv with
    | [AsLower "auth_failure"; uri] ->
        authFailure uri
        0
    | [AsLower "access_failure"; uri] ->
        accessFailure uri
        0
    | [AsLower "access_failure_not_allowed"; uri] ->
        accessFailureNotAllowed uri
        0
    | [AsLower "access_failure_send"; uri] ->
        accessFailureSend uri
        0
    | [AsLower "roundtrip"; uri] ->
        roundtrip uri
        0
    | [AsLower "roundtrip_to_amqp_091"; uri] ->
        roundtrip_to_amqp_091  uri
        0
    | [AsLower "data_types"; uri] ->
        datatypes uri
        0
    | [AsLower "default_outcome"; uri] ->
        defaultOutcome uri
        0
    | [AsLower "outcomes"; uri] ->
        outcomes uri
        0
    | [AsLower "fragmentation"; uri] ->
        fragmentation uri
        0
    | [AsLower "message_annotations"; uri] ->
        messageAnnotations uri
        0
    | [AsLower "footer"; uri] ->
        footer uri
        0
    | [AsLower "reject"; uri] ->
        reject uri
        0
    | [AsLower "redelivery"; uri] ->
        redelivery uri
        0
    | [AsLower "routing"; uri] ->
        routing uri
        0
    | [AsLower "invalid_routes"; uri] ->
        invalidRoutes uri
        0
    | _ ->
        printfn "test %A not found. usage: <test> <uri>" argv
        1
