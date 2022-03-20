%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp10_shovel).

-behaviour(rabbit_shovel_behaviour).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-export([
         parse/2,
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
         forward/4
        ]).

-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_data_coercion, [to_binary/1]).

-define(INFO(Text, Args), rabbit_log_shovel:info(Text, Args)).
-define(LINK_CREDIT_TIMEOUT, 5000).

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
      consumer_args => pget(consumer_args, Conf, [])}.

-spec connect_source(state()) -> state().
connect_source(State = #{name := Name,
                         ack_mode := AckMode,
                         source := #{uris := [Uri | _],
                                     source_address := Addr} = Src}) ->
    AttachFun = fun amqp10_client:attach_receiver_link/5,
    {Conn, Sess, LinkRef} = connect(Name, AckMode, Uri, "receiver", Addr, Src,
                                    AttachFun),
    State#{source => Src#{current => #{conn => Conn,
                                       session => Sess,
                                       link => LinkRef,
                                       uri => Uri}}}.

-spec connect_dest(state()) -> state().
connect_dest(State = #{name := Name,
                       ack_mode := AckMode,
                       dest := #{uris := [Uri | _],
                                 target_address := Addr} = Dst}) ->
    AttachFun = fun amqp10_client:attach_sender_link_sync/5,
    {Conn, Sess, LinkRef} = connect(Name, AckMode, Uri, "sender", Addr, Dst,
                                    AttachFun),
    %% wait for link credit here as if there are messages waiting we may try
    %% to forward before we've received credit
    State#{dest => Dst#{current => #{conn => Conn,
                                     session => Sess,
                                     link_state => attached,
                                     pending => [],
                                     link => LinkRef,
                                     uri => Uri}}}.

connect(Name, AckMode, Uri, Postfix, Addr, Map, AttachFun) ->
    {ok, Config} = amqp10_client:parse_uri(Uri),
    {ok, Conn} = amqp10_client:open_connection(Config),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    link(Conn),
    LinkName = begin
                   LinkName0 = gen_unique_name(Name, Postfix),
                   rabbit_data_coercion:to_binary(LinkName0)
               end,
    % mixed settlement mode covers all the ack_modes
    SettlementMode = case AckMode of
                         no_ack -> settled;
                         _ -> unsettled
                     end,
    % needs to be sync, i.e. awaits the 'attach' event as
    % else we may try to use the link before it is ready
    Durability = maps:get(durability, Map, unsettled_state),
    {ok, LinkRef} = AttachFun(Sess, LinkName, Addr,
                              SettlementMode,
                              Durability),
    {Conn, Sess, LinkRef}.

-spec init_source(state()) -> state().
init_source(State = #{source := #{current := #{link := Link},
                                  prefetch_count := Prefetch} = Src}) ->
    {Credit, RenewAfter} = case Src of
                               #{delete_after := R} when is_integer(R) ->
                                   {R, never};
                               #{prefetch_count := Pre} ->
                                   {Pre, round(Prefetch/10)}
                           end,
    ok = amqp10_client:flow_link_credit(Link, Credit, RenewAfter),
    Remaining = case Src of
                    #{delete_after := never} -> unlimited;
                    #{delete_after := Rem} -> Rem;
                    _ -> unlimited
                end,
    State#{source => Src#{remaining => Remaining,
                          remaining_unacked => Remaining,
                          last_acked_tag => -1}}.

-spec init_dest(state()) -> state().
init_dest(#{name := Name,
            shovel_type := Type,
            dest := #{add_forward_headers := true} = Dst} = State) ->
    Props = #{<<"shovelled-by">> => rabbit_nodes:cluster_name(),
              <<"shovel-type">> => rabbit_data_coercion:to_binary(Type),
              <<"shovel-name">> => rabbit_data_coercion:to_binary(Name)},
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

-spec handle_source(Msg :: any(), state()) -> not_handled | state().
handle_source({amqp10_msg, _LinkRef, Msg}, State) ->
    Tag = amqp10_msg:delivery_id(Msg),
    Payload = amqp10_msg:body_bin(Msg),
    rabbit_shovel_behaviour:forward(Tag, #{}, Payload, State);
handle_source({amqp10_event, {connection, Conn, opened}},
              State = #{source := #{current := #{conn := Conn}}}) ->
    State;
handle_source({amqp10_event, {connection, Conn, {closed, Why}}},
              #{source := #{current := #{conn := Conn}},
                name := Name}) ->
    ?INFO("Shovel ~s source connection closed. Reason: ~p", [Name, Why]),
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
                rabbit_log_shovel:warning("Shovel ~s amqp10 destination disposition tag not found: ~p",
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
    ?INFO("Shovel ~s destination connection closed. Reason: ~p", [Name, Why]),
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
            State0 = #{dest := #{current := #{link := Link},
                                 pending := Pend} = Dst}) ->

    %% we have credit so can begin to forward
    State = State0#{dest => Dst#{link_state => credited,
                                 pending => []}},
    lists:foldl(fun ({A, B, C}, S) ->
                        forward(A, B, C, S)
                end, State, lists:reverse(Pend));
handle_dest({amqp10_event, {link, Link, _Evt}},
            State= #{dest := #{current := #{link := Link}}}) ->
    State;
handle_dest({'EXIT', Conn, Reason},
            #{dest := #{current := #{conn := Conn}}}) ->
    {stop, {outbound_conn_died, Reason}};
handle_dest(_Msg, _State) ->
    not_handled.

close_source(#{source := #{current := #{conn := Conn}}}) ->
    _ = amqp10_client:close_connection(Conn),
    ok;
close_source(_Config) -> ok.

close_dest(#{dest := #{current := #{conn := Conn}}}) ->
    _ = amqp10_client:close_connection(Conn),
    ok;
close_dest(_Config) -> ok.

-spec ack(Tag :: tag(), Multi :: boolean(), state()) -> state().
ack(Tag, true, State = #{source := #{current := #{session := Session},
                                     last_acked_tag := LastTag} = Src}) ->
    First = LastTag + 1,
    ok = amqp10_client_session:disposition(Session, receiver, First,
                                           Tag, true, accepted),
    State#{source => Src#{last_acked_tag => Tag}};
ack(Tag, false, State = #{source := #{current :=
                                      #{session := Session}} = Src}) ->
    ok = amqp10_client_session:disposition(Session, receiver, Tag,
                                           Tag, true, accepted),
    State#{source => Src#{last_acked_tag => Tag}}.

-spec nack(Tag :: tag(), Multi :: boolean(), state()) -> state().
nack(Tag, false, State = #{source :=
                           #{current := #{session := Session}} = Src}) ->
    % the tag is the same as the deliveryid
    ok = amqp10_client_session:disposition(Session, receiver, Tag,
                                           Tag, false, rejected),
    State#{source => Src#{last_nacked_tag => Tag}};
nack(Tag, true, State = #{source := #{current := #{session := Session},
                                     last_nacked_tag := LastTag} = Src}) ->
    First = LastTag + 1,
    ok = amqp10_client_session:disposition(Session, receiver, First,
                                           Tag, true, accepted),
    State#{source => Src#{last_nacked_tag => Tag}}.

-spec forward(Tag :: tag(), Props :: #{atom() => any()},
              Payload :: binary(), state()) -> state().
forward(_Tag, _Props, _Payload,
        #{source := #{remaining_unacked := 0}} = State) ->
    State;
forward(Tag, Props, Payload,
        #{dest := #{current := #{link_state := attached},
                    pending := Pend0} = Dst} = State) ->
    %% simply cache the forward oo
    Pend = [{Tag, Props, Payload} | Pend0],
    State#{dest => Dst#{pending => {Pend}}};
forward(Tag, Props, Payload,
        #{dest := #{current := #{link := Link},
                    unacked := Unacked} = Dst,
          ack_mode := AckMode} = State) ->
    OutTag = rabbit_data_coercion:to_binary(Tag),
    Msg0 = new_message(OutTag, Payload, State),
    Msg = add_timestamp_header(
            State, set_message_properties(
                     Props, add_forward_headers(State, Msg0))),
    ok = amqp10_client:send_msg(Link, Msg),
    rabbit_shovel_behaviour:decr_remaining_unacked(
      case AckMode of
          no_ack ->
              rabbit_shovel_behaviour:decr_remaining(1, State);
          on_confirm ->
              State#{dest => Dst#{unacked => Unacked#{OutTag => Tag}}};
          on_publish ->
              State1 = rabbit_shovel_behaviour:ack(Tag, false, State),
              rabbit_shovel_behaviour:decr_remaining(1, State1)
      end).

new_message(Tag, Payload, #{ack_mode := AckMode,
                            dest := #{properties := Props,
                                      application_properties := AppProps,
                                      message_annotations := MsgAnns}}) ->
    Msg0 = amqp10_msg:new(Tag, Payload, AckMode =/= on_confirm),
    Msg1 = amqp10_msg:set_properties(Props, Msg0),
    Msg = amqp10_msg:set_message_annotations(MsgAnns, Msg1),
    amqp10_msg:set_application_properties(AppProps, Msg).

add_timestamp_header(#{dest := #{add_timestamp_header := true}}, Msg) ->
    P =#{creation_time => os:system_time(milli_seconds)},
    amqp10_msg:set_properties(P, Msg);
add_timestamp_header(_, Msg) -> Msg.

add_forward_headers(#{dest := #{cached_forward_headers := Props}}, Msg) ->
      amqp10_msg:set_application_properties(Props, Msg);
add_forward_headers(_, Msg) -> Msg.

set_message_properties(Props, Msg) ->
    %% this is effectively special handling properties from amqp 0.9.1
    maps:fold(
      fun(content_type, Ct, M) ->
              amqp10_msg:set_properties(
                #{content_type => to_binary(Ct)}, M);
         (content_encoding, Ct, M) ->
              amqp10_msg:set_properties(
                #{content_encoding => to_binary(Ct)}, M);
         (delivery_mode, 2, M) ->
              amqp10_msg:set_headers(#{durable => true}, M);
         (correlation_id, Ct, M) ->
              amqp10_msg:set_properties(#{correlation_id => to_binary(Ct)}, M);
         (reply_to, Ct, M) ->
              amqp10_msg:set_properties(#{reply_to => to_binary(Ct)}, M);
         (message_id, Ct, M) ->
              amqp10_msg:set_properties(#{message_id => to_binary(Ct)}, M);
         (timestamp, Ct, M) ->
              amqp10_msg:set_properties(#{creation_time => Ct}, M);
         (user_id, Ct, M) ->
              amqp10_msg:set_properties(#{user_id => Ct}, M);
         (headers, Headers0, M) when is_list(Headers0) ->
              %% AMPQ 0.9.1 are added as applicatin properties
              %% TODO: filter headers to make safe
              Headers = lists:foldl(
                          fun ({K, _T, V}, Acc) ->
                                  case is_amqp10_compat(V) of
                                      true ->
                                          Acc#{to_binary(K) => V};
                                      false ->
                                          Acc
                                  end
                          end, #{}, Headers0),
              amqp10_msg:set_application_properties(Headers, M);
         (Key, Value, M) ->
              case is_amqp10_compat(Value) of
                  true ->
                      amqp10_msg:set_application_properties(
                        #{to_binary(Key) => Value}, M);
                  false ->
                      M
              end
      end, Msg, Props).

gen_unique_name(Pre0, Post0) ->
    Pre = to_binary(Pre0),
    Post = to_binary(Post0),
    Id = bin_to_hex(crypto:strong_rand_bytes(8)),
    <<Pre/binary, <<"_">>/binary, Id/binary, <<"_">>/binary, Post/binary>>.

bin_to_hex(Bin) ->
    <<<<if N >= 10 -> N -10 + $a;
           true  -> N + $0 end>>
      || <<N:4>> <= Bin>>.

is_amqp10_compat(T) ->
    is_binary(T) orelse
    is_number(T) orelse
    %% TODO: not all lists are compatible
    is_list(T) orelse
    is_boolean(T).
