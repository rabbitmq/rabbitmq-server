-module(rabbit_amqp1_0_outgoing_link).

-export([attach/3, deliver/5, update_credit/2, flow/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_link_util, [protocol_error/3]).

-define(INIT_TXFR_COUNT, 0).

-record(outgoing_link, {queue,
                        delivery_count = 0,
                        no_ack,
                        default_outcome}).

%% TODO ensure_destination
attach(#'v1_0.attach'{name = Name,
                      handle = Handle,
                      source = Source,
                      rcv_settle_mode = RcvSettleMode}, BCh, DCh) ->
    {DefaultOutcome, Outcomes} = rabbit_amqp1_0_link_util:outcomes(Source),
    %% Default is first
    NoAck = RcvSettleMode =/= ?V_1_0_RECEIVER_SETTLE_MODE_SECOND,
    DOSym = rabbit_amqp1_0_framing:symbol_for(DefaultOutcome),
    case ensure_source(Source,
                       #outgoing_link{ delivery_count = ?INIT_TXFR_COUNT,
                                       no_ack = NoAck,
                                       default_outcome = DOSym}, DCh) of
        {ok, Source1,
         OutgoingLink = #outgoing_link{ queue = QueueName,
                                        delivery_count = Count }} ->
            CTag = handle_to_ctag(Handle),
            %% Zero the credit before we start consuming, so that we only
            %% use explicitly given credit.
            amqp_channel:cast(BCh, #'basic.credit'{consumer_tag = CTag,
                                                   credit       = 0,
                                                   count        = Count,
                                                   drain        = false}),
            case amqp_channel:subscribe(
                   BCh, #'basic.consume' { queue = QueueName,
                                           consumer_tag = CTag,
                                           no_ack = NoAck,
                                           %% TODO exclusive?
                                           exclusive = false}, self()) of
                #'basic.consume_ok'{} ->
                    %% FIXME we should avoid the race by getting the queue to send
                    %% attach back, but a.t.m. it would use the wrong codec.
                    put({out, Handle}, OutgoingLink),
                    {ok, [#'v1_0.attach'{
                       name = Name,
                       handle = Handle,
                       initial_delivery_count = {uint, ?INIT_TXFR_COUNT},
                       source = Source1#'v1_0.source'{
                                  default_outcome = DefaultOutcome
                                  %% TODO this breaks the Python client, when it
                                  %% tries to send us back a matching detach message
                                  %% it gets confused between described(true, [...])
                                  %% and [...]. We think we're correct here
                                  %% outcomes = Outcomes
                                 },
                       role = ?SEND_ROLE}]};
                Fail ->
                    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, "Consume failed: ~p", Fail)
            end;
        {error, _Reason} ->
            %% TODO Y U NO protocol_error?
            {ok, [#'v1_0.attach'{source = undefined}]}
    end.

deliver(#'basic.deliver'{consumer_tag = ConsumerTag,
                         delivery_tag = DeliveryTag,
                         routing_key  = RKey},
        Msg, WriterPid, BCh, Session) ->
    %% FIXME, don't ignore ack required, keep track of credit, um .. etc.
    Handle = ctag_to_handle(ConsumerTag),
    case get({out, Handle}) of
        Link = #outgoing_link{} ->
            {Link1, Session1} =
                transfer(WriterPid, Handle, Link, BCh, Session,
                         RKey, Msg, DeliveryTag),
            put({out, Handle}, Link1),
            {ok, rabbit_amqp1_0_session:incr_transfer_number(Session1)};
        undefined ->
            %% FIXME handle missing link -- why does the queue think it's there?
            rabbit_log:warning("Delivery to non-existent consumer ~p",
                               [ConsumerTag]),
            {ok, Session}
    end.

update_credit(#'basic.credit_state'{consumer_tag = CTag,
                                    credit       = LinkCredit,
                                    count        = Count,
                                    available    = Available0,
                                    drain        = Drain},
              WriterPid) ->
    Available = case Available0 of
                    -1  -> undefined;
                    Num -> {uint, Num}
                end,
    Handle = ctag_to_handle(CTag),
    %% The transfer count that is given by the queue should be at
    %% least that we have locally, since we will either have received
    %% all the deliveries and transfered them, or the queue will have
    %% advanced it due to drain. So we adopt the queue's idea of the
    %% count.
    %% FIXME account for it not being there any more
    Out = get({out, Handle}),
    F = #'v1_0.flow'{ handle      = Handle,
                      delivery_count = {uint, Count},
                      link_credit = {uint, LinkCredit},
                      available   = Available,
                      drain       = Drain },
    put({out, Handle}, Out#outgoing_link{ delivery_count = Count }),
    rabbit_amqp1_0_writer:send_command(WriterPid, F),
    ok.

flow(#outgoing_link{delivery_count = LocalCount},
     #'v1_0.flow'{handle         = Handle,
                  delivery_count = Count0,
                  link_credit    = {uint, RemoteCredit},
                  drain          = Drain}, BCh) ->
    RemoteCount = case Count0 of
                      undefined     -> LocalCount;
                      {uint, Count} -> Count
                  end,
    %% Rebase to our transfer-count
    Credit = RemoteCount + RemoteCredit - LocalCount,
    CTag = handle_to_ctag(Handle),
    #'basic.credit_ok'{available = Available} =
        %% FIXME calculate the credit based on the transfer count
        amqp_channel:call(BCh, #'basic.credit'{consumer_tag = CTag,
                                               credit       = Credit,
                                               count        = LocalCount,
                                               drain        = Drain}),
    case Available of
        -1 ->
            ok;
        %% We don't know - probably because this flow relates
        %% to a handle that does not yet exist
        %% TODO is this an error?
        _  ->
            {ok, [#'v1_0.flow'{
                    handle         = Handle,
                    delivery_count = {uint, LocalCount},
                    link_credit    = {uint, Credit},
                    available      = {uint, Available},
                    drain          = Drain}]}
    end.

%% TODO this looks to have a lot in common with ensure_target
ensure_source(Source = #'v1_0.source'{address       = Address,
                                      dynamic       = Dynamic,
                                      expiry_policy = ExpiryPolicy,
                                      timeout       = Timeout},
              Link = #outgoing_link{}, DCh) ->
    case Dynamic of
        true ->
            case Address of
                undefined ->
                    {ok, QueueName} = rabbit_amqp1_0_link_util:create_queue(Timeout, DCh),
                    {ok,
                     Source#'v1_0.source'{address = {utf8, rabbit_amqp1_0_link_util:queue_address(QueueName)}},
                     Link#outgoing_link{queue = QueueName}};
                _Else ->
                    {error, {both_dynamic_and_address_supplied,
                             Dynamic, Address}}
            end;
        _ ->
            %% TODO ugh. This will go away after the planned codec rewrite.
            Destination = case Address of
                              {_Enc, D} -> binary_to_list(D);
                              D         -> D
                          end,
            case rabbit_amqp1_0_link_util:parse_destination(Destination) of
                ["queue", Name] ->
                    case rabbit_amqp1_0_link_util:check_queue(Name, DCh) of
                        {ok, QueueName} ->
                            {ok, Source,
                             Link#outgoing_link{queue = QueueName}};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                ["exchange", Name, RK] ->
                    case rabbit_amqp1_0_link_util:check_exchange(Name, DCh) of
                        {ok, ExchangeName} ->
                            RoutingKey = list_to_binary(RK),
                            {ok, QueueName} =
                                rabbit_amqp1_0_link_util:create_bound_queue(
                                  ExchangeName, RoutingKey, DCh),
                            {ok, Source, Link#outgoing_link{queue = QueueName}};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                _Otherwise ->
                    {error, {unknown_address, Destination}}
            end
    end.

transfer(WriterPid, LinkHandle,
         Link = #outgoing_link{delivery_count = Count,
                               no_ack = NoAck,
                               default_outcome = DefaultOutcome},
         BCh, Session, RKey, Msg, DeliveryTag) ->
    MaySend = rabbit_amqp1_0_session:may_send(Session),
    if MaySend ->
            NewLink = Link#outgoing_link{delivery_count = Count + 1},
            DeliveryId = rabbit_amqp1_0_session:next_transfer_number(Session),
            T = #'v1_0.transfer'{handle = LinkHandle,
                                 delivery_tag = {binary, <<DeliveryTag:64>>},
                                 delivery_id = {uint, DeliveryId},
                                 %% The only one in AMQP 1-0
                                 message_format = {uint, 0},
                                 settled = NoAck,
                                 resume = false,
                                 more = false,
                                 aborted = false,
                                 %% TODO: actually batchable would be
                                 %% fine, but in any case it's only a
                                 %% hint
                                 batchable = false},
            rabbit_amqp1_0_writer:send_command(
              WriterPid,
              [T | rabbit_amqp1_0_message:annotated_message(RKey, Msg)]),
            {NewLink, case NoAck of
                          true  -> Session;
                          false -> rabbit_amqp1_0_session:record_delivery(
                                     DeliveryTag, DefaultOutcome, Session)
                      end};
       %% FIXME We can't knowingly exceed our credit.  On the other
       %% hand, we've been handed a message to deliver. This has
       %% probably happened because the receiver has suddenly reduced
       %% the credit or session window.
       NoAck ->
            {Link, Session};
       true ->
            amqp_channel:call(BCh, #'basic.reject'{requeue = true,
                                                   delivery_tag = DeliveryTag}),
            {Link, Session}
    end.

handle_to_ctag({uint, H}) ->
    <<"ctag-", H:32/integer>>.

ctag_to_handle(<<"ctag-", H:32/integer>>) ->
    {uint, H}.
