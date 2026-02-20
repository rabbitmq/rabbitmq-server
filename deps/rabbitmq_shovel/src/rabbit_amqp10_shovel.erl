%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_amqp10_shovel).

-behaviour(rabbit_shovel_behaviour).

-include_lib("rabbit/include/mc.hrl").
-include("rabbit_shovel.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-export([
         parse/2,
         parse_source/1,
         parse_dest/4,
         validate_src/1,
         validate_dest/1,
         validate_src_funs/2,
         validate_dest_funs/2,
         source_uri/1,
         dest_uri/1,
         source_protocol/1,
         dest_protocol/1,
         source_endpoint/1,
         dest_endpoint/1,
         connect_source/1,
         init_source/1,
         connect_dest/1,
         init_dest/1,
         handle_source/2,
         handle_dest/2,
         close_source/1,
         close_dest/1,
         ack/3,
         nack/3,
         status/1,
         forward/3,
         pending_count/1
        ]).

-rabbit_boot_step(
   {rabbit_amqp10_shovel_protocol,
    [{description, "AMQP10 shovel protocol"},
     {mfa,      {rabbit_registry, register,
                 [shovel_protocol, <<"amqp10">>, ?MODULE]}},
     {cleanup,  {rabbit_registry, unregister,
                 [shovel_protocol, <<"amqp10">>]}},
     {requires, rabbit_registry}]}).

-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_data_coercion, [to_binary/1]).
-import(rabbit_shovel_util, [validate_uri_fun/1,
                             deobfuscated_uris/2]).

-include_lib("kernel/include/logger.hrl").

-define(LINK_CREDIT_TIMEOUT, 20_000).
-define(AWAIT_SEND_MSG_TIMEOUT, 1_000).

-type state() :: rabbit_shovel_behaviour:state().
-type uri() :: rabbit_shovel_behaviour:uri().
-type tag() :: rabbit_shovel_behaviour:tag().
-type endpoint_config() :: rabbit_shovel_behaviour:source_config()
                           | rabbit_shovel_behaviour:dest_config().

-spec parse(binary(), {source | destination, proplists:proplist()}) ->
    endpoint_config().
parse(_Name, {destination, Conf}) ->
    Uris = pget(uris, Conf),
    #{module => ?MODULE,
      uris => Uris,
      unacked => #{},
      target_address => pget(target_address, Conf),
      properties => maps:from_list(pget(properties, Conf, [])),
      application_properties => maps:from_list(pget(application_properties, Conf, [])),
      delivery_annotations => maps:from_list(pget(delivery_annotations, Conf, [])),
      message_annotations => maps:from_list(pget(message_annotations, Conf, [])),
      add_forward_headers => pget(add_forward_headers, Conf, false),
      add_timestamp_header => pget(add_timestamp_header, Conf, false)
     };
parse(_Name, {source, Conf}) ->
    Uris = pget(uris, Conf),
    #{module => ?MODULE,
      uris => Uris,
      prefetch_count => pget(prefetch_count, Conf, 1000),
      delete_after => pget(delete_after, Conf, never),
      source_address => pget(source_address, Conf),
      consumer_args => pget(consumer_args, Conf, []),
      consumer_name => pget(consumer_name, Conf, <<>>)}.

parse_source(Def) ->
    Uris = deobfuscated_uris(<<"src-uri">>, Def),
    Address = pget(<<"src-address">>, Def),
    DeleteAfter = pget(<<"src-delete-after">>, Def, <<"never">>),
    PrefetchCount = pget(<<"src-prefetch-count">>, Def, 1000),
    Headers = [],
    SrcCName = pget(<<"src-consumer-name">>, Def, <<>>),
    {#{module => rabbit_amqp10_shovel,
       uris => Uris,
       source_address => Address,
       delete_after => opt_b2a(DeleteAfter),
       prefetch_count => PrefetchCount,
       consumer_args => [],
       consumer_name => SrcCName}, Headers}.

parse_dest({_VHost, _Name}, _ClusterName, Def, SourceHeaders) ->
    Uris = deobfuscated_uris(<<"dest-uri">>, Def),
    Address = pget(<<"dest-address">>, Def),
    Properties =
        rabbit_data_coercion:to_proplist(
            pget(<<"dest-properties">>, Def, [])),
    AppProperties =
        rabbit_data_coercion:to_proplist(
            pget(<<"dest-application-properties">>, Def, [])),
    MessageAnns =
        rabbit_data_coercion:to_proplist(
            pget(<<"dest-message-annotations">>, Def, [])),
    #{module => rabbit_amqp10_shovel,
      uris => Uris,
      target_address => Address,
      message_annotations => maps:from_list(MessageAnns),
      application_properties => maps:from_list(AppProperties ++ SourceHeaders),
      properties => maps:from_list(
                      lists:map(fun({K, V}) ->
                                        {rabbit_data_coercion:to_atom(K), V}
                                end, Properties)),
      add_timestamp_header => pget(<<"dest-add-timestamp-header">>, Def, false),
      add_forward_headers => pget(<<"dest-add-forward-headers">>, Def, false),
      unacked => #{}
     }.

validate_src(Def) ->
    [case {pget(<<"src-delete-after">>, Def, pget(<<"delete-after">>, Def)), pget(<<"ack-mode">>, Def)} of
         {N, <<"no-ack">>} when is_integer(N) ->
             {error, "Cannot specify 'no-ack' and numerical 'delete-after'", []};
         _ ->
             ok
     end].

validate_dest(_Def) ->
    [].

validate_src_funs(_Def, User) ->
    [
     {<<"src-uri">>, validate_uri_fun(User), mandatory},
     {<<"src-address">>, fun rabbit_parameter_validation:binary/2, mandatory},
     {<<"src-prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"src-consumer-name">>, fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-delete-after">>, fun validate_amqp10_delete_after/2, optional}
    ].

validate_dest_funs(_Def, User) ->
    [{<<"dest-uri">>, validate_uri_fun(User), mandatory},
     {<<"dest-address">>, fun rabbit_parameter_validation:binary/2, mandatory},
     {<<"dest-add-forward-headers">>, fun rabbit_parameter_validation:boolean/2, optional},
     {<<"dest-add-timestamp-header">>, fun rabbit_parameter_validation:boolean/2, optional},
     %% The bare message should be inmutable in the AMQP network.
     %% Before RabbitMQ 4.2, we allowed to set application properties, message
     %% annotations and any property. This is wrong.
     %% From 4.2, the only message modification allowed is the optional
     %% addition of forward headers and shovelled timestamp inside message
     %% annotations.
     %% To avoid breaking existing deployments, the following configuration
     %% keys are still accepted but will be ignored.
     {<<"dest-application-properties">>, fun validate_amqp10_map/2, optional},
     {<<"dest-message-annotations">>, fun validate_amqp10_map/2, optional},
     {<<"dest-properties">>, fun validate_amqp10_map/2, optional}
    ].

-spec connect_source(state()) -> state().
connect_source(State = #{name := Name,
                         ack_mode := AckMode,
                         source := #{uris := [Uri | _],
                                     source_address := Addr,
                                     consumer_name := CName} = Src}) ->
    SndSettleMode = case AckMode of
                        no_ack -> settled;
                        on_publish -> unsettled;
                        on_confirm -> unsettled
                    end,
    AttachFun = fun(S, L, A, SSM, D) ->
                        amqp10_client:attach_receiver_link(S, L, A, SSM, D, #{}, #{}, true)
                end,
    LinkNameOverride = case CName of
                           <<>> -> undefined;
                           _    -> CName
                       end,
    {Conn, Sess, LinkRef} = connect(Name, SndSettleMode, Uri, "receiver", Addr, Src,
                                    AttachFun, LinkNameOverride),
    State#{source => Src#{current => #{conn => Conn,
                                       session => Sess,
                                       link => LinkRef,
                                       uri => Uri}}}.

-spec connect_dest(state()) -> state().
connect_dest(State = #{name := Name,
                       ack_mode := AckMode,
                       dest := #{uris := [Uri | _],
                                 target_address := Addr} = Dst}) ->
    SndSettleMode = case AckMode of
                        no_ack -> settled;
                        on_publish -> settled;
                        on_confirm -> unsettled
                    end,
    AttachFun = fun amqp10_client:attach_sender_link_sync/5,
    {Conn, Sess, LinkRef} = connect(Name, SndSettleMode, Uri, "sender", Addr, Dst,
                                    AttachFun, undefined),
    %% wait for link credit here as if there are messages waiting we may try
    %% to forward before we've received credit
    State#{dest => Dst#{current => #{conn => Conn,
                                     session => Sess,
                                     link_state => attached,
                                     link => LinkRef,
                                     uri => Uri},
                        pending => []}}.

connect(Name, SndSettleMode, Uri, Postfix, Addr, Map, AttachFun, LinkNameOverride) ->
    {ok, Config0} = amqp10_client:parse_uri(Uri),
    %% As done for AMQP 0.9.1, exclude AMQP 1.0 shovel connections from maintenance mode
    %% to prevent crashes and errors being logged by the shovel plugin when a node gets drained.
    %% A better solution would be that the shovel plugin subscribes to event
    %% maintenance_connections_closed to gracefully transfer shovels over to other live nodes.
    Config = Config0#{properties => #{<<"ignore-maintenance">> => {boolean, true}}},
    {ok, Conn} = amqp10_client:open_connection(Config),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    link(Conn),
    LinkName = case LinkNameOverride of
                   undefined ->
                       LinkName0 = rabbit_shovel_util:gen_unique_name(Name, Postfix),
                       rabbit_data_coercion:to_binary(LinkName0);
                   _ ->
                       LinkNameOverride
               end,
    % needs to be sync, i.e. awaits the 'attach' event as
    % else we may try to use the link before it is ready
    Durability = maps:get(durability, Map, unsettled_state),
    %% Attach in raw mode
    {ok, LinkRef} = AttachFun(Sess, LinkName, Addr,
                              SndSettleMode,
                              Durability),
    {Conn, Sess, LinkRef}.

-spec init_source(state()) -> state().
init_source(State = #{source := #{current := #{link := Link},
                                  prefetch_count := Prefetch} = Src}) ->
    {Credit, RenewWhenBelow} = {Prefetch, max(1, round(Prefetch/10))},
    ok = amqp10_client:flow_link_credit(Link, Credit, RenewWhenBelow),
    Remaining = case Src of
                    #{delete_after := never} -> unlimited;
                    #{delete_after := Rem} -> Rem;
                    _ -> unlimited
                end,
    case Remaining of
        0 -> exit({shutdown, autodelete});
        _ -> ok
    end,
    State#{source => Src#{remaining => Remaining,
                          remaining_unacked => Remaining,
                          last_acked_tag => -1}}.

-spec init_dest(state()) -> state().
init_dest(#{name := Name,
            shovel_type := Type,
            dest := #{add_forward_headers := true} = Dst} = State) ->
    Props = #{<<"x-opt-shovelled-by">> => rabbit_nodes:cluster_name(),
              <<"x-opt-shovel-type">> => rabbit_data_coercion:to_binary(Type),
              <<"x-opt-shovel-name">> => rabbit_data_coercion:to_binary(Name)},
    State#{dest => Dst#{cached_forward_headers => Props}};
init_dest(State) ->
    State.

-spec source_uri(state()) -> uri().
source_uri(#{source := #{current := #{uri := Uri}}}) -> Uri.

-spec dest_uri(state()) -> uri().
dest_uri(#{dest := #{current := #{uri := Uri}}}) -> Uri.

source_protocol(_State) -> amqp10.
dest_protocol(_State) -> amqp10.

source_endpoint(#{shovel_type := static}) ->
    [];
source_endpoint(#{shovel_type := dynamic,
                  source := #{source_address := Addr}}) ->
    [{src_address, Addr}].

dest_endpoint(#{shovel_type := static}) ->
    [];
dest_endpoint(#{shovel_type := dynamic,
                dest := #{target_address := Addr}}) ->
    [{dest_address, Addr}].

-spec handle_source(Msg :: any(), state()) ->
    not_handled | state() | {stop, any()}.
handle_source({amqp10_msg, _LinkRef, RawMsg}, State) ->
    Tag = amqp10_raw_msg:delivery_tag(RawMsg),
    Payload = amqp10_raw_msg:payload(RawMsg),
    Msg = mc:init(mc_amqp, Payload, #{}),
    rabbit_shovel_behaviour:forward(Tag, Msg, State);
handle_source({amqp10_event, {connection, Conn, opened}},
              State = #{source := #{current := #{conn := Conn}}}) ->
    State;
handle_source({amqp10_event, {connection, Conn, {closed, Why}}},
              #{source := #{current := #{conn := Conn}},
                name := Name}) ->
    ?LOG_INFO("Shovel ~ts source connection closed. Reason: ~tp", [Name, Why]),
    {stop, {inbound_conn_closed, Why}};
handle_source({amqp10_event, {session, Sess, begun}},
              State = #{source := #{current := #{session := Sess}}}) ->
    State;
handle_source({amqp10_event, {session, Sess, {ended, Why}}},
              #{source := #{current := #{session := Sess}}}) ->
    {stop, {inbound_session_ended, Why}};
handle_source({amqp10_event, {link, Link, {detached, Why}}},
              #{source := #{current := #{link := Link}}}) ->
    {stop, {inbound_link_detached, Why}};
handle_source({amqp10_event, {link, Link, _Evt}},
              State= #{source := #{current := #{link := Link}}}) ->
    State;
handle_source({'EXIT', Conn, Reason},
              #{source := #{current := #{conn := Conn}}}) ->
    {stop, {outbound_conn_died, Reason}};

handle_source({'EXIT', _Pid, {shutdown, {server_initiated_close, _, Reason}}}, _State) ->
    {stop, {inbound_link_or_channel_closure, Reason}};

handle_source(_Msg, _State) ->
    not_handled.

-spec handle_dest(Msg :: any(), state()) -> not_handled | state().
handle_dest({amqp10_disposition, {Result, Tag}},
            State0 = #{ack_mode := on_confirm,
                       dest := #{unacked := Unacked} = Dst,
                       name := Name}) ->
    State1 = State0#{dest => Dst#{unacked => maps:remove(Tag, Unacked)}},
    {Decr, State} =
        case {Unacked, Result} of
            {#{Tag := IncomingTag}, accepted} ->
                {1, rabbit_shovel_behaviour:ack(IncomingTag, false, State1)};
            {#{Tag := IncomingTag}, rejected} ->
                {1, rabbit_shovel_behaviour:nack(IncomingTag, false, State1)};
            _ -> % not found - this should ideally not happen
                ?LOG_WARNING("Shovel ~ts amqp10 destination disposition tag not found: ~tp",
                                          [Name, Tag]),
                {0, State1}
        end,
    rabbit_shovel_behaviour:decr_remaining(Decr, State);
handle_dest({amqp10_event, {connection, Conn, opened}},
            State = #{dest := #{current := #{conn := Conn}}}) ->
    State;
handle_dest({amqp10_event, {connection, Conn, {closed, Why}}},
            #{name := Name,
              dest := #{current := #{conn := Conn}}}) ->
    ?LOG_INFO("Shovel ~ts destination connection closed. Reason: ~tp", [Name, Why]),
    {stop, {outbound_conn_died, Why}};
handle_dest({amqp10_event, {session, Sess, begun}},
            State = #{dest := #{current := #{session := Sess}}}) ->
    State;
handle_dest({amqp10_event, {session, Sess, {ended, Why}}},
            #{dest := #{current := #{session := Sess}}}) ->
    {stop, {outbound_conn_died, Why}};
handle_dest({amqp10_event, {link, Link, {detached, Why}}},
            #{dest := #{current := #{link := Link}}}) ->
    {stop, {outbound_link_detached, Why}};
handle_dest({amqp10_event, {link, Link, credited}},
            State0 = #{dest := #{current := #{link := Link} = Current,
                                 pending := Pend} = Dst}) ->

    %% we have credit so can begin to forward
    State = State0#{dest => Dst#{current => Current#{link_state => credited},
                                 pending => []}},
    lists:foldl(fun ({A, B}, S) ->
                        forward(A, B, S)
                end, State, lists:reverse(Pend));
handle_dest({amqp10_event, {link, Link, _Evt}},
            State= #{dest := #{current := #{link := Link}}}) ->
    State;
handle_dest({'EXIT', Conn, Reason},
            #{dest := #{current := #{conn := Conn}}}) ->
    {stop, {outbound_conn_died, Reason}};

handle_dest({'EXIT', _Pid, {shutdown, {server_initiated_close, _, Reason}}}, _State) ->
    {stop, {outbound_link_or_channel_closure, Reason}};

handle_dest(_Msg, _State) ->
    not_handled.

close_source(#{source := #{current := #{conn := Conn,
                                        session := Sess}}}) ->
    _ = amqp10_client:end_session(Sess),
    _ = amqp10_client:close_connection(Conn),
    ok;
close_source(_Config) -> ok.

close_dest(#{dest := #{current := #{conn := Conn,
                                    session := Sess}}}) ->
    _ = amqp10_client:end_session(Sess),
    _ = amqp10_client:close_connection(Conn),
    ok;
close_dest(_Config) -> ok.

-spec ack(Tag :: tag(), Multi :: boolean(), state()) -> state().
ack(Tag, true, State = #{source := #{current := #{link := LinkRef},
                                     last_acked_tag := LastTag} = Src}) ->
    First = LastTag + 1,
    ok = amqp10_client_session:disposition(LinkRef, First, Tag, true, accepted),
    State#{source => Src#{last_acked_tag => Tag}};
ack(Tag, false, State = #{source := #{current := #{link := LinkRef}} = Src}) ->
    ok = amqp10_client_session:disposition(LinkRef, Tag, Tag, true, accepted),
    State#{source => Src#{last_acked_tag => Tag}}.

-spec nack(Tag :: tag(), Multi :: boolean(), state()) -> state().
nack(Tag, false, State = #{source := #{current := #{link := LinkRef}} = Src}) ->
    % the tag is the same as the deliveryid
    ok = amqp10_client_session:disposition(LinkRef, Tag, Tag, true, rejected),
    State#{source => Src#{last_acked_tag => Tag}};
nack(Tag, true, State = #{source := #{current := #{link := LinkRef},
                                      last_acked_tag := LastTag} = Src}) ->
    First = LastTag + 1,
    ok = amqp10_client_session:disposition(LinkRef, First, Tag, true, rejected),
    State#{source => Src#{last_acked_tag => Tag}}.

status(#{dest := #{current := #{link_state := attached}}}) ->
    flow;
status(#{dest := #{current := #{link_state := credited}}}) ->
    running;
status(_) ->
    %% Destination not yet connected
    ignore.

pending_count(#{dest := Dest}) ->
    Pending = maps:get(pending, Dest, []),
    length(Pending).

-spec forward(Tag :: tag(), Mc :: mc:state(), state()) ->
    state() | {stop, any()}.
forward(_Tag, _Mc,
        #{source := #{remaining := 0}} = State) ->
    State;
forward(_Tag, _Mc,
        #{source := #{remaining_unacked := 0}} = State) ->
    State;
forward(Tag, Mc,
        #{dest := #{current := #{link_state := attached},
                    pending := Pend0} = Dst} = State) ->
    %% simply cache the forward oo
    Pend = [{Tag, Mc} | Pend0],
    State#{dest => Dst#{pending => Pend}};
forward(Tag, Msg0,
        #{dest := #{current := #{link := Link},
                    unacked := Unacked},
          ack_mode := AckMode} = State) ->
    OutTag = rabbit_data_coercion:to_binary(Tag),
    Msg1 = add_timestamp_header(State, add_forward_headers(State, Msg0)),
    Msg2 = mc:protocol_state(mc:convert(mc_amqp, Msg1)),
    Msg3 = amqp10_raw_msg:new(AckMode =/= on_confirm, Tag, iolist_to_binary(Msg2)),
    case send_msg(Link, Msg3) of
        ok ->
            #{dest := Dst1} = State1 = rabbit_shovel_behaviour:incr_forwarded(State),
            rabbit_shovel_behaviour:decr_remaining_unacked(
              case AckMode of
                  no_ack ->
                      rabbit_shovel_behaviour:decr_remaining(1, State1);
                  on_confirm ->
                      State1#{dest => Dst1#{unacked => Unacked#{OutTag => Tag}}};
                  on_publish ->
                      State2 = rabbit_shovel_behaviour:ack(Tag, false, State1),
                      rabbit_shovel_behaviour:decr_remaining(1, State2)
              end);
        Stop ->
            Stop
    end.

send_msg(Link, Msg) ->
    case amqp10_client:send_msg(Link, Msg) of
        ok ->
            ok;
        {error, insufficient_credit} ->
            receive {amqp10_event, {link, Link, credited}} ->
                    send_msg(Link, Msg)
            after ?LINK_CREDIT_TIMEOUT ->
                      {stop, credited_timeout}
            end;
        {error, remote_incoming_window_exceeded} ->
            %% We could be blocked because of an alarm
            timer:sleep(?AWAIT_SEND_MSG_TIMEOUT),
            send_msg(Link, Msg)
    end.

add_timestamp_header(#{dest := #{add_timestamp_header := true}}, Msg) ->
    mc:set_annotation(
      <<"x-opt-shovelled-timestamp">>, os:system_time(milli_seconds),
      Msg);
add_timestamp_header(_, Msg) -> Msg.

add_forward_headers(#{dest := #{cached_forward_headers := Anns}}, Msg) ->
    maps:fold(fun(K, V, Acc) ->
                      mc:set_annotation(K, V, Acc)
              end, Msg, Anns);
add_forward_headers(_, Msg) -> Msg.

validate_amqp10_delete_after(_Name, <<"never">>)          -> ok;
validate_amqp10_delete_after(_Name, N) when is_integer(N), N >= 0 -> ok;
validate_amqp10_delete_after(Name,  Term) ->
    {error, "~ts should be a number greater than or equal to 0 or \"never\", actually was "
     "~tp", [Name, Term]}.

validate_amqp10_map(Name, Terms0) ->
    Terms = rabbit_data_coercion:to_proplist(Terms0),
    Str = fun rabbit_parameter_validation:binary/2,
    Validation = [{K, Str, optional} || {K, _} <- Terms],
    rabbit_parameter_validation:proplist(Name, Validation, Terms).

opt_b2a(B) when is_binary(B) -> list_to_atom(binary_to_list(B));
opt_b2a(N)                   -> N.
