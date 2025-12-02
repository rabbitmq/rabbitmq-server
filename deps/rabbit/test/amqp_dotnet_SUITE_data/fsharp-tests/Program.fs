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

    let connectAnon uri =
        let c = Connection(Address uri, SaslProfile.Anonymous, null, null)
        let s = Session c
        { Conn = c; Session = s }

    let connectAnonWithOpen uri opn =
        let c = Connection(Address uri, SaslProfile.Anonymous, opn, null)
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

    let assertNotNull a =
        if a = null then
            failwith (sprintf "Null not expected")

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

    let testOutcome (uri:string) (attach:Attach) (expectedCond:string) =
        use ac = connectAnon uri
        let mutable errorName : string = null
        use attachedEv = new AutoResetEvent(false)
        use closedEv   = new AutoResetEvent(false)
        let onAttached = new OnAttached (fun _ _ -> attachedEv.Set() |> ignore)
        let receiver = new ReceiverLink(ac.Session, "test-receiver", attach, onAttached)
        receiver.add_Closed(
            new ClosedCallback (fun _ err ->
                if not (isNull err) then errorName <- string err.Condition
                closedEv.Set() |> ignore))

        if not (attachedEv.WaitOne(9000)) then failwith "Expected broker to reply with attach frame"
        if isNull expectedCond then
            if closedEv.WaitOne(5) then failwith "Unexpected link refusal"
            receiver.Close()
        else
            if not (closedEv.WaitOne(9000)) then failwith "Expected link refusal, but link didn't close."
            assertEqual expectedCond errorName

    let no_routes_is_released uri =
        // tests that a message sent to an exchange that resolves no routes for the
        // binding key returns the Released outcome, rather than Accepted
        use ac = connectAnon uri
        let address = "/exchanges/no_routes_is_released"
        let sender = SenderLink(ac.Session, "released-sender", address)
        let trySet (mre: AutoResetEvent) =
            try mre.Set() |> ignore with _ -> ()

        let mutable outcome = null
        use mre = new System.Threading.AutoResetEvent(false)

        let msg = new Message("hi"B,
                              Header = Header(Ttl = 500u),
                              Properties = new Properties(Subject = "no_routes"))
        let cb = new OutcomeCallback (fun l m o x -> outcome <- o; trySet mre)
        sender.Send(msg, cb, null)
        mre.WaitOne(1000) |> ignore

        match outcome with
        | :? Released ->
            ()
        | _ ->
            failwith (sprintf "Expected: Released\r\nGot: %A" outcome)

        ()

    let roundtrip uri =
        use c = connectAnon uri
        let sender, receiver = senderReceiver c "test" "/queues/roundtrip"
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

    let streams uri =
        use c = connectAnon uri
        let name = "streams-test"
        let address = "/queues/streams"
        let sender = SenderLink(c.Session, name + "-sender" , address)
        //for body in sampleTypes do
        let body = "hi"B :> obj

        let corr = "correlation"
        new Message(body,
                    Properties = new Properties(CorrelationId = corr))
        |> sender.Send
        //TODO wait for settlement
        let specs = [box("first");
                     box("last");
                     box("10m");
                     box(0)]
        for spec in specs do
            printfn "testing streams spec %A" spec
            let filterSet = Map()
            let describedValue = DescribedValue(Symbol "rabbitmq:stream-offset-spec", spec)
            filterSet.Add(Symbol "my offset", describedValue)
            let source = Source(Address = address,
                                FilterSet = filterSet)
            let attach = Attach(Source = source)
            let attached = new OnAttached (fun _ _ -> ())
            let receiver = ReceiverLink(c.Session, Guid.NewGuid().ToString(), attach, attached)
            receiver.SetCredit(100, true)
            let rtd = receiver.Receive()
            assertNotNull rtd
            assertEqual 3 rtd.MessageAnnotations.Map.Count
            assertTrue (rtd.MessageAnnotations.Map.ContainsKey(Symbol "x-stream-offset"))
            assertTrue (rtd.MessageAnnotations.Map.ContainsKey(Symbol "x-exchange"))
            assertTrue (rtd.MessageAnnotations.Map.ContainsKey(Symbol "x-routing-key"))

            assertEqual body rtd.Body
            assertEqual rtd.Properties.CorrelationId  corr
            receiver.Close()
        ()

    open RabbitMQ.Client

    let roundtrip_to_amqp_091 uri =
        use c = connectAnon uri
        let q = "roundtrip_to_amqp_091"
        let target = "/queues/roundtrip_to_amqp_091"
        let corr = "correlation"
        let sender = SenderLink(c.Session, q + "-sender" , target)

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

    let default_outcome uri =
        for (defOut, cond, defObj) in
            ["amqp:accepted:list", null, Accepted() :> Outcome
             "amqp:rejected:list", null, Rejected() :> Outcome
             "amqp:released:list", null, Released() :> Outcome] do

            let source = new Source(Address = "/queues/default_outcome",
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

            let source = new Source(Address = "/queues/outcomes",
                                    Outcomes = [| Symbol outcome |])
            let attach = new Attach (Source = source,
                                     Target = Target())

            testOutcome uri attach cond


    let fragmentation uri =
        for frameSize, size in
            [1024u, 990
             1024u, 1000
             1024u, 1010
             1024u, 1020
             1024u, 1024
             1024u, 1100
             1024u, 2048
             2048u, 2048] do
            let addr = Address uri
            let opn = Open(ContainerId = Guid.NewGuid().ToString(),
                           HostName = addr.Host, ChannelMax = 256us,
                           MaxFrameSize = frameSize)
            use c = connectAnonWithOpen uri opn
            let sender, receiver = senderReceiver c "test" "/queues/fragmentation"
            let m = new Message(String.replicate size "a")
            sender.Send m
            let m' = receive receiver
            assertEqual (m.Body) (m'.Body)

    let message_annotations uri =
        use c = connectAnon uri
        let sender, receiver = senderReceiver c "test" "/queues/message_annotations"
        let ann = MessageAnnotations()
        let k1 = Symbol "x-key1"
        let k2 = Symbol "x-key2"
        ann.[Symbol "x-key1"] <- "value1"
        ann.[Symbol "x-key2"] <- "value2"
        let m = new Message("testing annotations", MessageAnnotations = ann)
        sender.Send m
        let m' = receive receiver

        assertEqual m.Body m'.Body
        assertEqual (m.MessageAnnotations.Descriptor) (m'.MessageAnnotations.Descriptor)
        // our 2 custom annotations + x-exchange + x-routing-key = 4
        assertEqual 4 (m'.MessageAnnotations.Map.Count)
        assertTrue (m.MessageAnnotations.[k1] = m'.MessageAnnotations.[k1])
        assertTrue (m.MessageAnnotations.[k2] = m'.MessageAnnotations.[k2])

    let footer uri =
        use c = connectAnon uri
        let sender, receiver = senderReceiver c "test" "/queues/footer"
        let footer = Footer()
        let k1 = Symbol "key1"
        let k2 = Symbol "key2"
        footer.[Symbol "key1"] <- "value1"
        footer.[Symbol "key2"] <- "value2"
        let m = new Message("testing footer", Footer = footer)
        sender.Send m
        let m' = receive receiver

        assertEqual m.Body m'.Body
        assertEqual (m.Footer.Descriptor) (m'.Footer.Descriptor)
        assertEqual 2 (m'.Footer.Map.Count)
        assertTrue (m.Footer.[k1] = m'.Footer.[k1])
        assertTrue (m.Footer.[k2] = m'.Footer.[k2])

    let data_types uri =
        use c = connectAnon uri
        let sender, receiver = senderReceiver c "test" "/queues/data_types"
        let aSeq = amqpSequence sampleTypes
        (new Message(aSeq)) |> sender.Send
        let rtd = receive receiver
        let amqpSeq = rtd.Body :?> AmqpSequence
        for a in amqpSeq.List do
            List.exists ((=) a) sampleTypes |> assertTrue

    let reject uri =
        use c = connectAnon uri
        let sender, receiver = senderReceiver c "test" "/queues/reject"
        new Message "testing reject" |> sender.Send
        let m = receiver.Receive()
        receiver.Reject(m)
        assertEqual null (receiver.Receive(TimeSpan.FromMilliseconds 100.))

    let redelivery uri =
        use c = connectAnon uri
        let sender, receiver = senderReceiver c "test" "/queues/redelivery"
        new Message "testing redelivery" |> sender.Send
        let m = receiver.Receive()
        assertTrue (m.Header.FirstAcquirer)
        c.Session.Close()

        let session = Session(c.Conn)
        let receiver = ReceiverLink(session, "test-receiver", "/queues/redelivery")

        let m' = receive receiver
        assertEqual (m.Body :?> string) (m'.Body :?> string)
        assertTrue (not m'.Header.FirstAcquirer)
        assertEqual 1u (m'.Header.DeliveryCount)
        assertEqual null (receiver.Receive(TimeSpan.FromMilliseconds 100.))
        session.Close()

    let released uri =
        use c = connectAnon uri
        let sender, receiver = senderReceiver c "test" "/queues/released"
        new Message "testing released" |> sender.Send
        let m = receiver.Receive()
        assertTrue (m.Header.FirstAcquirer)
        receiver.SetCredit(0, false)
        receiver.Release m

        let m' = receive receiver
        assertEqual (m.Body :?> string) (m'.Body :?> string)
        assertTrue (not m'.Header.FirstAcquirer)
        assertEqual 0u (m'.Header.DeliveryCount)
        assertEqual null (receiver.Receive(TimeSpan.FromMilliseconds 100.))
        c.Session.Close()

    let routing uri =
        for target, source, toProp in
            [
             "/queues/test", "/queues/test", ""
             "/exchanges/amq.fanout",  "/queues/fanout_q",  ""
             "/exchanges/amq.direct/direct_q",  "/queues/direct_q",  ""
             null,  "/queues/direct_q",  "/exchanges/amq.direct/direct_q"
             "/queues/transient_q", "/queues/transient_q", ""
             "/queues/durable_q",   "/queues/durable_q",   ""
             "/queues/quorum_q",   "/queues/quorum_q",   ""
             "/queues/stream_q",   "/queues/stream_q",   ""
             "/queues/autodel_q",   "/queues/autodel_q",   ""] do

            let rnd = Random()
            use c = connectAnon uri
            let sender = SenderLink(c.Session, "test-sender", target)
            let receiver = ReceiverLink(c.Session, "test-receiver", source)
            receiver.SetCredit(100, true)
            use m = new Message(rnd.Next(10000),
                Properties = Properties(To = toProp))
            sender.Send m
            (* printfn "%s %s %s %A" target source routingKey *)

            let m' = receiver.Receive(TimeSpan.FromMilliseconds 3000.)
            receiver.Accept m'
            assertTrue (m' <> null)
            assertEqual (m.Body :?> int) (m'.Body :?> int)

    // Test attaching a sender to a non-existent exchange
    // Expect: Link refused with amqp:not-found error
    let attachSenderToMissingExchange uri =
        use ac = connectAnon uri
        use attachEvent = new System.Threading.AutoResetEvent(false)
        use detachEvent = new System.Threading.AutoResetEvent(false)
        let mutable linkError : Error = null

        let attached = new OnAttached (fun link attach ->
            if attach.Target = null then
                attachEvent.Set() |> ignore
            else
                failwith "Expected null target in attach response"
        )

        let sender = new SenderLink(ac.Session, "test-sender",
                                    Target(Address = "/exchanges/missing"), attached)

        sender.add_Closed(new ClosedCallback(fun _ err ->
            linkError <- err
            detachEvent.Set() |> ignore))

        assertTrue (attachEvent.WaitOne(9000))
        assertTrue (detachEvent.WaitOne(9000))

        assertNotNull linkError
        assertEqual (Symbol "amqp:not-found") linkError.Condition
        assertTrue (not ac.Session.IsClosed)

    // Test attaching a sender to an invalid address format
    // Expect: Link refused with amqp:invalid-field error
    let attachSenderToInvalidAddress uri =
        use ac = connectAnon uri
        use attachEvent = new System.Threading.AutoResetEvent(false)
        use detachEvent = new System.Threading.AutoResetEvent(false)
        let mutable linkError : Error = null

        let attached = new OnAttached (fun link attach ->
            if attach.Target = null then
                attachEvent.Set() |> ignore
            else
                failwith "Expected null target in attach response"
        )

        let sender = new SenderLink(ac.Session, "test-sender",
                                    Target(Address = "/fruit/orange"), attached)

        sender.add_Closed(new ClosedCallback(fun _ err ->
            linkError <- err
            detachEvent.Set() |> ignore))

        assertTrue (attachEvent.WaitOne(9000))
        assertTrue (detachEvent.WaitOne(9000))

        assertNotNull linkError
        assertEqual (Symbol "amqp:invalid-field") linkError.Condition
        assertTrue (not ac.Session.IsClosed)

    // Test attaching a receiver to a non-existent queue
    // Expect: Link refused with amqp:not-found error
    let attachReceiverFromMissingQueue uri =
        use ac = connectAnon uri
        use attachEvent = new System.Threading.AutoResetEvent(false)
        use detachEvent = new System.Threading.AutoResetEvent(false)
        let mutable linkError : Error = null

        let attached = new OnAttached (fun link attach ->
            if attach.Source = null then
                attachEvent.Set() |> ignore
            else
                failwith "Expected null source in attach response"
        )

        let receiver = new ReceiverLink(ac.Session, "test-receiver",
                                        Source(Address = "/queues/missing"), attached)

        receiver.add_Closed(new ClosedCallback(fun _ err ->
            linkError <- err
            detachEvent.Set() |> ignore))

        assertTrue (attachEvent.WaitOne(9000))
        assertTrue (detachEvent.WaitOne(9000))

        assertNotNull linkError
        assertEqual (Symbol "amqp:not-found") linkError.Condition
        assertTrue (not ac.Session.IsClosed)

    // Test attaching a receiver from an invalid address format
    // Expect: Link refused with amqp:invalid-field error
    let attachReceiverFromInvalidAddress uri =
        use ac = connectAnon uri
        use attachEvent = new System.Threading.AutoResetEvent(false)
        use detachEvent = new System.Threading.AutoResetEvent(false)
        let mutable linkError : Error = null

        let attached = new OnAttached (fun link attach ->
            if attach.Source = null then
                attachEvent.Set() |> ignore
            else
                failwith "Expected null source in attach response"
        )

        let receiver = new ReceiverLink(ac.Session, "test-receiver",
                                        Source(Address = "/fruit/orange"), attached)

        receiver.add_Closed(new ClosedCallback(fun _ err ->
            linkError <- err
            detachEvent.Set() |> ignore))

        assertTrue (attachEvent.WaitOne(9000))
        assertTrue (detachEvent.WaitOne(9000))

        assertNotNull linkError
        assertEqual (Symbol "amqp:invalid-field") linkError.Condition
        assertTrue (not ac.Session.IsClosed)

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
            let target = "/queues/test"
            ac.Session.add_Closed (
                new ClosedCallback (fun _ err -> printfn "session err %A" err.Condition
                ))
            let sender = new SenderLink(ac.Session, "test-sender", target)
            sender.Send(new Message "hi", TimeSpan.FromSeconds 15.)
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
            let src = "/queues/test"
            let receiver = ReceiverLink(ac.Session, "test-receiver", src)
            receiver.Close()
            failwith "expected exception not received"
        with
        | :? Amqp.AmqpException as ex ->
            printfn "Exception %A" ex
            ()

    // Test sending a message without a body section.
    // Although supported by this lib and Azure Service Bus, we expect RabbitMQ
    // to return a descriptive error since a body is mandatory according to the AMQP spec.
    let messageWithoutBody uri =
        use ac = connectAnon uri
        let sender = SenderLink(ac.Session, "sender", "/exchange/amq.fanout")

        use detachEvent = new AutoResetEvent(false)
        let mutable linkError : Error = null

        sender.add_Closed(new ClosedCallback(fun _ err ->
            linkError <- err
            detachEvent.Set() |> ignore))

        // Create a message with Properties but no body section
        let message = new Message(Properties = new Properties())

        try
            sender.Send(message, TimeSpan.FromSeconds 5.)
            failwith "Expected exception not received"
        with
        | :? Amqp.AmqpException as ex ->
            printfn "Got expected exception: %A" ex

        assertTrue (detachEvent.WaitOne(9000))
        assertNotNull linkError
        assertEqual (Symbol "amqp:decode-error") linkError.Condition
        assertNotNull linkError.Description
        assertTrue (linkError.Description.Contains("message has no body"))
        assertTrue (not ac.Session.IsClosed)

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
        data_types uri
        0
    | [AsLower "default_outcome"; uri] ->
        default_outcome uri
        0
    | [AsLower "outcomes"; uri] ->
        outcomes uri
        0
    | [AsLower "fragmentation"; uri] ->
        fragmentation uri
        0
    | [AsLower "message_annotations"; uri] ->
        message_annotations uri
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
    | [AsLower "released"; uri] ->
        released uri
        0
    | [AsLower "routing"; uri] ->
        routing uri
        0
    | [AsLower "attach_sender_to_missing_exchange"; uri] ->
        attachSenderToMissingExchange uri
        0
    | [AsLower "attach_sender_to_invalid_address"; uri] ->
        attachSenderToInvalidAddress uri
        0
    | [AsLower "attach_receiver_from_missing_queue"; uri] ->
        attachReceiverFromMissingQueue uri
        0
    | [AsLower "attach_receiver_from_invalid_address"; uri] ->
        attachReceiverFromInvalidAddress uri
        0
    | [AsLower "streams"; uri] ->
        streams uri
        0
    | [AsLower "no_routes_is_released"; uri] ->
        no_routes_is_released uri
        0
    | [AsLower "message_without_body"; uri] ->
        messageWithoutBody uri
        0
    | _ ->
        printfn "test %A not found. usage: <test> <uri>" argv
        1
