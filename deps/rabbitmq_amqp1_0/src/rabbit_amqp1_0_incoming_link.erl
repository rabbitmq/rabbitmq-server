-module(rabbit_amqp1_0_incoming_link).

-export([attach/4, transfer/5]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_link_util, [protocol_error/3]).

%% Just make these constant for the time being.
-define(INCOMING_CREDIT, 65536).

-record(incoming_link, {name, exchange, routing_key,
                        delivery_count = 0,
                        credit_used = ?INCOMING_CREDIT div 2
                       }).

attach(#'v1_0.attach'{name = Name,
                      handle = Handle,
                      source = Source,
                      snd_settle_mode = SndSettleMode,
                      target = Target,
                      initial_delivery_count = {uint, InitTransfer}},
       BCh, DCh, NextPublishId) ->
    %% TODO associate link name with target
    case ensure_target(Target, #incoming_link{ name = Name }, DCh) of
        {ok, ServerTarget,
         IncomingLink = #incoming_link{ delivery_count = InitTransfer }} ->
            {_, _Outcomes} = rabbit_amqp1_0_link_util:outcomes(Source),
            %% Default is mixed!
            NextPublishId1 =
                case SndSettleMode of
                    ?V_1_0_SENDER_SETTLE_MODE_SETTLED ->
                        NextPublishId;
                    _ ->
                        %% TODO we need to deal with mixed settlement mode -
                        %% presumably by turning on confirms and then throwing
                        %% some away.
                        amqp_channel:register_confirm_handler(BCh, self()),
                        amqp_channel:call(BCh, #'confirm.select'{}),
                        erlang:max(1, NextPublishId)
                end,
            put({in, Handle}, IncomingLink),
            Flow = #'v1_0.flow'{ handle = Handle,
                                 link_credit = {uint, ?INCOMING_CREDIT},
                                 drain = false,
                                 echo = false },
            Attach = #'v1_0.attach'{
              name = Name,
              handle = Handle,
              source = Source,
              target = ServerTarget,
              initial_delivery_count = undefined, % must be, I am the recvr
              role = ?RECV_ROLE}, %% server is receiver
            {ok, [Attach, Flow], NextPublishId1};
        {error, Reason} ->
            rabbit_log:warning("AMQP 1.0 attach rejected ~p~n", [Reason]),
            %% TODO proper link estalishment protocol here?
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                               "Attach rejected: ~p", [Reason])
    end.

transfer(#'v1_0.transfer'{handle = Handle,
                          settled = Settled,
                          delivery_id = {uint, TxfrId}},
         AnnotatedMessage, BCh, NextPublishId, Unsettled) ->
    case get({in, Handle}) of
        #incoming_link{ exchange = X, routing_key = LinkRKey,
                        delivery_count = Count,
                        credit_used = CreditUsed } = Link ->
            NewCount = rabbit_misc:serial_add(Count, 1),
            {MsgRKey, Msg} = rabbit_amqp1_0_message:assemble(AnnotatedMessage),
            RKey = case LinkRKey of
                       undefined -> MsgRKey;
                       _         -> LinkRKey
                   end,
            NextPublishId1 = case NextPublishId of
                                 0 -> 0;
                                 _ -> NextPublishId + 1 % serial?
                             end,
            amqp_channel:call(BCh, #'basic.publish'{exchange    = X,
                                                    routing_key = RKey}, Msg),
            {SendFlow, CreditUsed1} = case CreditUsed - 1 of
                                          C when C =< 0 ->
                                              {true,  ?INCOMING_CREDIT div 2};
                                          D ->
                                              {false, D}
                                      end,
            NewLink = Link#incoming_link{ delivery_count = NewCount,
                                          credit_used = CreditUsed1 },
            put({in, Handle}, NewLink),
            Unsettled1 = case Settled of
                             true  -> Unsettled;
                             %% Be lenient -- this is a boolean and really ought
                             %% to have a value, but the spec doesn't currently
                             %% require it.
                             Symbol when
                                   Symbol =:= false orelse
                                   Symbol =:= undefined ->
                                 gb_trees:insert(NextPublishId,
                                                 TxfrId,
                                                 Unsettled)
                         end,
            Reply = case SendFlow of
                        true ->
                            ?DEBUG("sending flow for incoming ~p", [NewLink]),
                            [incoming_flow(NewLink, Handle)];
                        false ->
                            []
                    end,
            {ok, Reply, NextPublishId1, Unsettled1};
        undefined ->
            protocol_error(?V_1_0_AMQP_ERROR_ILLEGAL_STATE,
                           "Unknown link handle ~p", [Handle])
    end.

%% There are a few things that influence what source and target
%% definitions mean for our purposes.
%%
%% Addresses: we artificially segregate exchanges and queues, since
%% they have different namespaces. However, we allow both incoming and
%% outgoing links to exchanges: outgoing links from an exchange
%% involve an anonymous queue.
%%
%% For targets, addresses are
%% Address = "/exchange/" Name "/" RoutingKey
%%         | "/exchange/" Name
%%         | "/queue"
%%         | "/queue/" Name
%%
%% For sources, addresses are
%% Address = "/exchange/" Name "/" RoutingKey
%%         | "/queue/" Name
%%
%% We use the message property "Subject" as the equivalent of the
%% routing key.  In AMQP 0-9-1 terms, a target of /queue is equivalent
%% to the default exchange; that is, the message is routed to the
%% queue named by the subject.  A target of "/queue/Name" ignores the
%% subject.  The reason for both varieties is that a
%% dynamically-created queue must be fully addressable as a target,
%% while a service may wish to use /queue and route each message to
%% its reply-to queue name (as it is done in 0-9-1).
%%
%% A dynamic source or target only ever creates a queue, and the
%% address is returned in full; e.g., "/queue/amq.gen.123456".
%% However, that cannot be used as a reply-to, since a 0-9-1 client
%% will use it unaltered as the routing key naming the queue.
%% Therefore, we rewrite reply-to from 1.0 clients to be just the
%% queue name, and expect replying clients to use /queue and the
%% subject field.
%%
%% For a source queue, the distribution-mode is always move.  For a
%% source exchange, it is always copy. Anything else should be
%% refused.
%%
%% TODO default-outcome and outcomes, dynamic lifetimes

%% TODO this looks to have a lot in common with ensure_source
ensure_target(Target = #'v1_0.target'{address       = Address,
                                      dynamic       = Dynamic,
                                      %% TODO expiry_policy = ExpiryPolicy,
                                      timeout       = Timeout},
              Link = #incoming_link{}, DCh) ->
    case Dynamic of
        true ->
            case Address of
                undefined ->
                    {ok, QueueName} = rabbit_amqp1_0_link_util:create_queue(Timeout, DCh),
                    {ok,
                     Target#'v1_0.target'{address = {utf8, rabbit_amqp1_0_link_util:queue_address(QueueName)}},
                     Link#incoming_link{exchange = <<"">>,
                                        routing_key = QueueName}};
                _Else ->
                    {error, {both_dynamic_and_address_supplied,
                             Dynamic, Address}}
            end;
        _ ->
            case Address of
                {Enc, Destination}
                when Enc =:= utf8 orelse Enc =:= utf16 ->
                    case rabbit_amqp1_0_link_util:parse_destination(Destination, Enc) of
                        ["queue", Name] ->
                            case rabbit_amqp1_0_link_util:check_queue(Name, DCh) of
                                {ok, QueueName} ->
                                    {ok, Target,
                                     Link#incoming_link{exchange = <<"">>,
                                                        routing_key = QueueName}};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        ["queue"] ->
                            %% Rely on the Subject being set
                            {ok, Target, Link#incoming_link{exchange = <<"">>}};
                        ["exchange", Name] ->
                            case rabbit_amqp1_0_link_util:check_exchange(Name, DCh) of
                                {ok, ExchangeName} ->
                                    {ok, Target,
                                     Link#incoming_link{exchange = ExchangeName}};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        ["exchange", Name, RKey] ->
                            case rabbit_amqp1_0_link_util:check_exchange(Name, DCh) of
                                {ok, ExchangeName} ->
                                    {ok, Target,
                                     Link#incoming_link{exchange = ExchangeName,
                                                        routing_key = list_to_binary(RKey)}};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        _Otherwise ->
                            {error, {unknown_address, Address}}
                    end;
                _Else ->
                    {error, {unknown_address, Address}}
            end
    end.

incoming_flow(#incoming_link{ delivery_count = Count }, Handle) ->
    #'v1_0.flow'{handle         = Handle,
                 delivery_count = {uint, Count},
                 link_credit    = {uint, ?INCOMING_CREDIT}}.
