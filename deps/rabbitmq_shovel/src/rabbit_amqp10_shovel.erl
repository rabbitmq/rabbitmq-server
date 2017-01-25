-module(rabbit_amqp10_shovel).

-behaviour(rabbit_shovel_protocol).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-export([
         parse/2,
         source_uri/1,
         dest_uri/1,
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

-define(INFO(Text, Args), error_logger:info_msg(Text, Args)).

-type state() :: rabbit_shovel_protocol:state().
-type uri() :: rabbit_shovel_protocol:uri().
-type tag() :: rabbit_shovel_protocol:tag().
-type endpoint_config() :: rabbit_shovel_protocol:endpoint_config().

-spec parse(binary(), {source | destination, proplists:proplist()}) ->
    endpoint_config().
parse(_Name, {destination, Conf}) ->
    Uris = pget(uris, Conf),
    #{module => ?MODULE,
      uris => Uris,
      unacked => #{},
      target_address => pget(target_address, Conf),
      delivery_annotations => maps:from_list(pget(delivery_annotations, Conf, [])),
      message_annotations => maps:from_list(pget(message_annotations, Conf, [])),
      properties => maps:from_list(pget(properties, Conf, [])),
      add_forward_headers => pget(add_forward_headers, Conf, false),
      add_timestamp_header => pget(add_timestamp_header, Conf, false)
     };
parse(_Name, {source, Conf}) ->
    Uris = pget(uris, Conf),
    #{module => ?MODULE,
      uris => Uris,
      prefetch_count => pget(prefetch_count, Conf, 1000),
      delete_after => pget(delete_after, Conf, never),
      source_address => pget(source_address, Conf)}.

-spec connect_source(state()) -> state().
connect_source(State = #{name := Name,
                         ack_mode := AckMode,
                         source := #{uris := [Uri | _],
                                     source_address := Addr} = Src}) ->
    {ok, Config} = amqp10_client:parse_uri(Uri),
    {ok, Conn} = amqp10_client:open_connection(Config),
    link(Conn),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    LinkName = list_to_binary(atom_to_list(Name) ++ "-receiver"),
    % mixed settlement mode covers all the ack_modes
    SettlementMode = case AckMode of
                         no_ack -> settled;
                         _ -> unsettled
                     end,
    {ok, LinkRef} = amqp10_client:attach_receiver_link(Sess, LinkName, Addr,
                                                       SettlementMode),
    State#{source => Src#{current => #{conn => Conn,
                                       session => Sess,
                                       link => LinkRef,
                                       uri => Uri}}}.

-spec connect_dest(state()) -> state().
connect_dest(State = #{name := Name,
                       ack_mode := AckMode,
                       dest := #{uris := [Uri | _],
                                 target_address := Addr} = Dst}) ->
    {ok, Config} = amqp10_client:parse_uri(Uri),
    {ok, Conn} = amqp10_client:open_connection(Config),
    link(Conn),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    LinkName = list_to_binary(atom_to_list(Name) ++ "-sender"),
    SettlementMode = case AckMode of
                         no_ack -> settled;
                         _ -> unsettled
                     end,
    {ok, LinkRef} = amqp10_client:attach_sender_link(Sess, LinkName, Addr,
                                                     SettlementMode),
    State#{dest => Dst#{current => #{conn => Conn,
                                     session => Sess,
                                     link => LinkRef,
                                     uri => Uri}}}.

-spec init_source(state()) -> state().
init_source(State = #{source := #{current := #{link := Link},
                                  prefetch_count := Prefetch}}) ->
    ?INFO("flowing credit", []),
    ok = amqp10_client:flow_link_credit(Link, Prefetch, round(Prefetch/10)),
    State#{remaining => unlimited,
           remaining_unacked => unlimited}. % TODO remaining?

-spec init_dest(state()) -> state().
init_dest(State) -> State.

-spec source_uri(state()) -> uri().
source_uri(#{source := #{current := #{uri := Uri}}}) -> Uri.

-spec dest_uri(state()) -> uri().
dest_uri(#{dest := #{current := #{uri := Uri}}}) -> Uri.

-spec handle_source(Msg :: any(), state()) -> not_handled | state().
handle_source({'EXIT', Conn, Reason},
              #{source := #{current := #{conn := Conn}}}) ->
    {stop, {outbound_conn_died, Reason}};

handle_source({amqp10_msg, LinkRef, Msg},
              State = #{dest := #{module := DstMod}}) ->
    ?INFO("handling msg ~p link_ref ~p~n", [Msg, LinkRef]),
    Tag = amqp10_msg:delivery_id(Msg),
    [Payload] = amqp10_msg:body(Msg),
    DstMod:forward(Tag, #{}, Payload, State);

handle_source(_Msg, _State) ->
    not_handled.

-spec handle_dest(Msg :: any(), state()) -> not_handled | state().
handle_dest({'EXIT', Conn, Reason},
            #{dest := #{current := #{conn := Conn}}}) ->
    {stop, {outbound_conn_died, Reason}};

handle_dest({amqp10_disposition, {Result, Tag}},
            State0 = #{ack_mode := on_confirm,
                      source := #{module := SrcMod},
                      dest := #{unacked := Unacked} = Dst}) ->
    State = State0#{dest => Dst#{unacked => maps:remove(Tag, Unacked)}},
    case {Unacked, Result} of
        {#{Tag := IncomingTag}, accepted} ->
            SrcMod:ack(IncomingTag, false, State);
        {#{Tag := IncomingTag}, rejected} ->
            SrcMod:nack(IncomingTag, false, State);
        _ -> % not found - this should ideally not happen
            error_logger:warning_msg("amqp10 destination disposition tag not"
                                     "found: ~p~n", [Tag]),
            State
    end;
handle_dest(_Msg, _State) ->
    not_handled.

close_source(#{source := #{current := #{conn := Conn}}}) ->
    _ = amqp10_client:close_connection(Conn),
    ok.

close_dest(#{dest := #{current := #{conn := Conn}}}) ->
    _ = amqp10_client:close_connection(Conn),
    ok.

-spec ack(Tag :: tag(), Multi :: boolean(), state()) -> state().
% TODO support multiple by keeping track of last ack
ack(Tag, false, State = #{source := #{current := #{session := Session}}}) ->
    % TODO: the tag should be the deliveryid
    ok = amqp10_client_session:disposition(Session, receiver, Tag,
                                           Tag, true, accepted),
    State.

-spec nack(Tag :: tag(), Multi :: boolean(), state()) -> state().
nack(Tag, false, State = #{source := #{current := #{session := Session}}}) ->
    % TODO: the tag should be the deliveryid
    ok = amqp10_client_session:disposition(Session, receiver, Tag,
                                           Tag, false, rejected),
    State.

-spec forward(Tag :: tag(), Props :: #{atom() => any()},
              Payload :: binary(), state()) -> state().
forward(Tag, Props, Payload,
        #{dest := #{current := #{link := Link},
                    unacked := Unacked} = Dst,
          source := #{module := SrcMod},
          ack_mode := AckMode} = State) ->
    OutTag = integer_to_binary(Tag),
    Msg0 = amqp10_msg:new(OutTag, Payload, AckMode =:= no_ack),
    Msg = add_timestamp_header(
            State, set_message_properties(
                     Props, add_forward_headers(State, Msg0))),
    error_logger:info_msg("forwarding ~p ", [Msg]),
    ok = amqp10_client:send_msg(Link, Msg),
    case AckMode of
        no_ack -> State;
        on_publish ->
            SrcMod:ack(Tag, false, State);
        on_confirm ->
              State#{dest => Dst#{unacked => Unacked#{OutTag => Tag}}}
    end.

add_timestamp_header(#{dest := #{add_timestamp_header := true}}, Msg) ->
    P =#{creation_time => os:system_time(millisecond)},
    amqp10_msg:set_properties(P, Msg);
add_timestamp_header(_, Msg) -> Msg.

add_forward_headers(#{name := Name,
                      dest := #{add_forward_headers := true}}, Msg) ->
      Props = #{<<"shovelled-by">> => rabbit_nodes:cluster_name(),
                <<"shovel-type">> => <<"static">>,
                <<"shovel-name">> => list_to_binary(atom_to_list(Name))},
      amqp10_msg:set_application_properties(Props, Msg);
add_forward_headers(_, Msg) -> Msg.

set_message_properties(Props, Msg) ->
    maps:fold(fun(delivery_mode, 2, M) ->
                      amqp10_msg:set_headers(#{durable => true}, M);
                 (content_type, Ct, M) ->
                      amqp10_msg:set_properties(
                        #{content_type => to_binary(Ct)}, M);
                 (_, _, M) -> M
              end, Msg, Props).

pget(K, PList) ->
    {K, V} = proplists:lookup(K, PList),
    V.

pget(K, PList, Default) ->
    proplists:get_value(K, PList, Default).

to_binary(X) when is_binary(X) -> X;
to_binary(X) when is_list(X) -> list_to_binary(X);
to_binary(X) when is_atom(X) -> list_to_binary(atom_to_list(X)).
