%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp091_shovel).

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

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

parse(_Name, {source, Source}) ->
    Prefetch = parse_parameter(prefetch_count, fun parse_non_negative_integer/1,
                               proplists:get_value(prefetch_count, Source,
                                                   ?DEFAULT_PREFETCH)),
    Queue = parse_parameter(queue, fun parse_binary/1,
                            proplists:get_value(queue, Source)),
    %% TODO parse
    CArgs = proplists:get_value(consumer_args, Source, []),
    #{module => ?MODULE,
      uris => proplists:get_value(uris, Source),
      resource_decl => decl_fun(Source),
      queue => Queue,
      delete_after => proplists:get_value(delete_after, Source, never),
      prefetch_count => Prefetch,
      consumer_args => CArgs};
parse(Name, {destination, Dest}) ->
    PubProp = proplists:get_value(publish_properties, Dest, []),
    PropsFun = try_make_parse_publish(publish_properties, PubProp),
    PubFields = proplists:get_value(publish_fields, Dest, []),
    PubFieldsFun = try_make_parse_publish(publish_fields, PubFields),
    AFH = proplists:get_value(add_forward_headers, Dest, false),
    ATH = proplists:get_value(add_timestamp_header, Dest, false),
    PropsFun1 = add_forward_headers_fun(Name, AFH, PropsFun),
    PropsFun2 = add_timestamp_header_fun(ATH, PropsFun1),
    #{module => ?MODULE,
      uris => proplists:get_value(uris, Dest),
      resource_decl  => decl_fun(Dest),
      props_fun => PropsFun2,
      fields_fun => PubFieldsFun,
      add_forward_headers => AFH,
      add_timestamp_header => ATH}.

connect_source(Conf = #{name := Name,
                        source := #{uris := Uris} = Src}) ->
    {Conn, Chan, Uri} = make_conn_and_chan(Uris, Name),
    Conf#{source => Src#{current => {Conn, Chan, Uri}}}.

init_source(Conf = #{ack_mode := AckMode,
                     source := #{queue := Queue,
                                 current := {Conn, Chan, _},
                                 prefetch_count := Prefetch,
                                 resource_decl := Decl,
                                 consumer_args := Args} = Src}) ->
    Decl(Conn, Chan),

    NoAck = AckMode =:= no_ack,
    case NoAck of
        false ->
            #'basic.qos_ok'{} =
            amqp_channel:call(Chan, #'basic.qos'{prefetch_count = Prefetch}),
            ok;
        true  -> ok
    end,
    Remaining = remaining(Chan, Conf),
    case Remaining of
        0 ->
            exit({shutdown, autodelete});
        _ -> ok
    end,
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Chan, #'basic.consume'{queue = Queue,
                                                      no_ack = NoAck,
                                                      arguments = Args}, self()),
    Conf#{source => Src#{remaining => Remaining,
                         remaining_unacked => Remaining}}.

connect_dest(Conf = #{name := Name, dest := #{uris := Uris} = Dst}) ->
    {Conn, Chan, URI} = make_conn_and_chan(Uris, Name),
    Conf#{dest => Dst#{current => {Conn, Chan, URI}}}.

init_dest(Conf = #{ack_mode := AckMode,
                   dest := #{current := {Conn, Chan, _},
                             resource_decl := Decl} = Dst}) ->

    Decl(Conn, Chan),

    case AckMode of
        on_confirm ->
            #'confirm.select_ok'{} =
                amqp_channel:call(Chan, #'confirm.select'{}),
            ok = amqp_channel:register_confirm_handler(Chan, self());
        _ ->
            ok
    end,
    Conf#{dest => Dst#{unacked => #{}}}.

ack(Tag, Multi, State = #{source := #{current := {_, Chan, _}}}) ->
    ok = amqp_channel:cast(Chan, #'basic.ack'{delivery_tag = Tag,
                                              multiple = Multi}),
    State.

nack(Tag, Multi, State = #{source := #{current := {_, Chan, _}}}) ->
     ok = amqp_channel:cast(Chan, #'basic.nack'{delivery_tag = Tag,
                                                multiple = Multi}),
     State.

source_uri(#{source := #{current := {_, _, Uri}}}) -> Uri.
dest_uri(#{dest := #{current := {_, _, Uri}}}) -> Uri.

source_protocol(_State) -> amqp091.
dest_protocol(_State) -> amqp091.

source_endpoint(#{source := #{queue := <<>>,
                              source_exchange := SrcX,
                              source_exchange_key := SrcXKey}}) ->
    [{src_exchange, SrcX},
     {src_exchange_key, SrcXKey}];
source_endpoint(#{source := #{queue := Queue}}) ->
    [{src_queue, Queue}];
source_endpoint(_Config) ->
    [].

dest_endpoint(#{shovel_type := static}) ->
    [];
dest_endpoint(#{dest := Dest}) ->
    Keys = [dest_exchange, dest_exchange_key, dest_queue],
    maps:to_list(maps:filter(fun(K, _) -> proplists:is_defined(K, Keys) end, Dest)).

forward(IncomingTag, Props, Payload,
        State0 = #{dest := #{props_fun := PropsFun,
                             current := {_, _, DstUri},
                             fields_fun := FieldsFun}}) ->
    SrcUri = rabbit_shovel_behaviour:source_uri(State0),
    % do publish
    Exchange = maps:get(exchange, Props, undefined),
    RoutingKey = maps:get(routing_key, Props, undefined),
    Method = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Method1 = FieldsFun(SrcUri, DstUri, Method),
    Msg1 = #amqp_msg{props = PropsFun(SrcUri, DstUri, props_from_map(Props)),
                     payload = Payload},
    publish(IncomingTag, Method1, Msg1, State0).

props_from_map(Map) ->
    #'P_basic'{content_type = maps:get(content_type, Map, undefined),
               content_encoding = maps:get(content_encoding, Map, undefined),
               headers = maps:get(headers, Map, undefined),
               delivery_mode = maps:get(delivery_mode, Map, undefined),
               priority = maps:get(priority, Map, undefined),
               correlation_id = maps:get(correlation_id, Map, undefined),
               reply_to = maps:get(reply_to, Map, undefined),
               expiration = maps:get(expiration, Map, undefined),
               message_id = maps:get(message_id, Map, undefined),
               timestamp = maps:get(timestamp, Map, undefined),
               type = maps:get(type, Map, undefined),
               user_id = maps:get(user_id, Map, undefined),
               app_id = maps:get(app_id, Map, undefined),
               cluster_id = maps:get(cluster_id, Map, undefined)}.

map_from_props(#'P_basic'{content_type = Content_type,
                          content_encoding = Content_encoding,
                          headers = Headers,
                          delivery_mode = Delivery_mode,
                          priority = Priority,
                          correlation_id = Correlation_id,
                          reply_to = Reply_to,
                          expiration = Expiration,
                          message_id = Message_id,
                          timestamp = Timestamp,
                          type = Type,
                          user_id = User_id,
                          app_id = App_id,
                          cluster_id = Cluster_id}) ->
    lists:foldl(fun({_K, undefined}, Acc) -> Acc;
                   ({K, V}, Acc) -> Acc#{K => V}
                end, #{}, [{content_type, Content_type},
                           {content_encoding, Content_encoding},
                           {headers, Headers},
                           {delivery_mode, Delivery_mode},
                           {priority, Priority},
                           {correlation_id, Correlation_id},
                           {reply_to, Reply_to},
                           {expiration, Expiration},
                           {message_id, Message_id},
                           {timestamp, Timestamp},
                           {type, Type},
                           {user_id, User_id},
                           {app_id, App_id},
                           {cluster_id, Cluster_id}
                          ]).

handle_source(#'basic.consume_ok'{}, State) ->
    State;
handle_source({#'basic.deliver'{delivery_tag = Tag,
                                exchange = Exchange,
                                routing_key = RoutingKey},
              #amqp_msg{props = Props0, payload = Payload}}, State) ->
    Props = (map_from_props(Props0))#{exchange => Exchange,
                                      routing_key => RoutingKey},
    % forward to destination
    rabbit_shovel_behaviour:forward(Tag, Props, Payload, State);

handle_source({'EXIT', Conn, Reason},
              #{source := #{current := {Conn, _, _}}}) ->
    {stop, {inbound_conn_died, Reason}};

handle_source(_Msg, _State) ->
    not_handled.

handle_dest(#'basic.ack'{delivery_tag = Seq, multiple = Multiple},
            State = #{ack_mode := on_confirm}) ->
    confirm_to_inbound(fun (Tag, Multi, StateX) ->
                               rabbit_shovel_behaviour:ack(Tag, Multi, StateX)
                       end, Seq, Multiple, State);

handle_dest(#'basic.nack'{delivery_tag = Seq, multiple = Multiple},
            State = #{ack_mode := on_confirm }) ->
    confirm_to_inbound(fun (Tag, Multi, StateX) ->
                               rabbit_shovel_behaviour:nack(Tag, Multi, StateX)
                       end, Seq, Multiple, State);

handle_dest(#'basic.cancel'{}, #{name := Name}) ->
    rabbit_log:warning("Shovel ~p received a 'basic.cancel' from the server", [Name]),
    {stop, {shutdown, restart}};

handle_dest({'EXIT', Conn, Reason}, #{dest := #{current := {Conn, _, _}}}) ->
    {stop, {outbound_conn_died, Reason}};

handle_dest(_Msg, _State) ->
    not_handled.

close_source(#{source := #{current := {Conn, _, _}}}) ->
    catch amqp_connection:close(Conn, ?MAX_CONNECTION_CLOSE_TIMEOUT),
    ok;
close_source(_) ->
    %% It never connected, connection doesn't exist
    ok.

close_dest(#{dest := #{current := {Conn, _, _}}}) ->
    catch amqp_connection:close(Conn, ?MAX_CONNECTION_CLOSE_TIMEOUT),
    ok;
close_dest(_) ->
    %% It never connected, connection doesn't exist
    ok.

confirm_to_inbound(ConfirmFun, Seq, Multiple,
                   State0 = #{dest := #{unacked := Unacked} = Dst}) ->
    #{Seq := InTag} = Unacked,
    State = ConfirmFun(InTag, Multiple, State0),
    {Unacked1, Removed} = remove_delivery_tags(Seq, Multiple, Unacked, 0),
    rabbit_shovel_behaviour:decr_remaining(Removed,
                                           State#{dest =>
                                                  Dst#{unacked => Unacked1}}).

publish(_Tag, _Method, _Msg, State = #{source := #{remaining_unacked := 0}}) ->
    %% We are in on-confirm mode, and are autodelete. We have
    %% published all the messages we need to; we just wait for acks to
    %% come back. So drop subsequent messages on the floor to be
    %% requeued later.
    State;

publish(IncomingTag, Method, Msg,
        State = #{ack_mode := AckMode,
                  dest := Dst}) ->
    #{unacked := Unacked,
      current := {_, OutboundChan, _}} = Dst,
    Seq = case AckMode of
              on_confirm ->
                  amqp_channel:next_publish_seqno(OutboundChan);
              _  -> undefined
          end,
    ok = amqp_channel:call(OutboundChan, Method, Msg),
    rabbit_shovel_behaviour:decr_remaining_unacked(
      case AckMode of
          no_ack ->
              rabbit_shovel_behaviour:decr_remaining(1, State);
          on_confirm ->
              State#{dest => Dst#{unacked => Unacked#{Seq => IncomingTag}}};
          on_publish ->
              State1 = rabbit_shovel_behaviour:ack(IncomingTag, false, State),
              rabbit_shovel_behaviour:decr_remaining(1, State1)
      end).

make_conn_and_chan([], {VHost, Name} = _ShovelName) ->
    rabbit_log:error(
          "Shovel '~s' in vhost '~s' has no more URIs to try for connection",
          [Name, VHost]),
    erlang:error(failed_to_connect_using_provided_uris);
make_conn_and_chan([], ShovelName) ->
    rabbit_log:error(
          "Shovel '~s' has no more URIs to try for connection",
          [ShovelName]),
    erlang:error(failed_to_connect_using_provided_uris);
make_conn_and_chan(URIs, ShovelName) ->
    try do_make_conn_and_chan(URIs, ShovelName) of
        Val -> Val
    catch throw:{error, Reason, URI} ->
        log_connection_failure(Reason, URI, ShovelName),
        make_conn_and_chan(lists:usort(URIs -- [URI]), ShovelName)
    end.

do_make_conn_and_chan(URIs, ShovelName) ->
    URI = lists:nth(rand:uniform(length(URIs)), URIs),
    {ok, AmqpParam} = amqp_uri:parse(URI),
    ConnName = get_connection_name(ShovelName),
    case amqp_connection:start(AmqpParam, ConnName) of
        {ok, Conn} ->
            link(Conn),
            {ok, Ch} = amqp_connection:open_channel(Conn),
            link(Ch),
            {Conn, Ch, list_to_binary(amqp_uri:remove_credentials(URI))};
        {error, not_allowed} ->
            throw({error, not_allowed, URI});
        {error, Reason} ->
            throw({error, Reason, URI})
    end.

log_connection_failure(Reason, URI, {VHost, Name} = _ShovelName) ->
    rabbit_log:error(
          "Shovel '~s' in vhost '~s' failed to connect (URI: ~s): ~s",
      [Name, VHost, amqp_uri:remove_credentials(URI), human_readable_connection_error(Reason)]);
log_connection_failure(Reason, URI, ShovelName) ->
    rabbit_log:error(
          "Shovel '~s' failed to connect (URI: ~s): ~s",
          [ShovelName, amqp_uri:remove_credentials(URI), human_readable_connection_error(Reason)]).

human_readable_connection_error({auth_failure, Msg}) ->
    Msg;
human_readable_connection_error(not_allowed) ->
    "access to target virtual host was refused";
human_readable_connection_error(unknown_host) ->
    "unknown host (failed to resolve hostname)";
human_readable_connection_error(econnrefused) ->
    "connection to target host was refused (ECONNREFUSED)";
human_readable_connection_error(econnreset) ->
    "connection to target host was reset by peer (ECONNRESET)";
human_readable_connection_error(etimedout) ->
    "connection to target host timed out (ETIMEDOUT)";
human_readable_connection_error(ehostunreach) ->
    "target host is unreachable (EHOSTUNREACH)";
human_readable_connection_error(nxdomain) ->
    "target hostname cannot be resolved (NXDOMAIN)";
human_readable_connection_error(eacces) ->
    "connection to target host failed with EACCES. "
    "This may be due to insufficient RabbitMQ process permissions or "
    "a reserved IP address used as destination";
human_readable_connection_error(Other) ->
    rabbit_misc:format("~p", [Other]).

get_connection_name(ShovelName) when is_atom(ShovelName) ->
    Prefix = <<"Shovel ">>,
    ShovelNameAsBinary = atom_to_binary(ShovelName, utf8),
    <<Prefix/binary, ShovelNameAsBinary/binary>>;
%% for dynamic shovels, name is a binary
get_connection_name(ShovelName) when is_binary(ShovelName) ->
    Prefix = <<"Shovel ">>,
    <<Prefix/binary, ShovelName/binary>>;
%% fallback
get_connection_name(_) ->
    <<"Shovel">>.

remove_delivery_tags(Seq, false, Unacked, 0) ->
    {maps:remove(Seq, Unacked), 1};
remove_delivery_tags(Seq, true, Unacked, Count) ->
    case maps:size(Unacked) of
        0  -> {Unacked, Count};
        _ ->
            maps:fold(fun(K, _V, {Acc, Cnt}) when K =< Seq ->
                              {maps:remove(K, Acc), Cnt + 1};
                         (_K, _V, Acc) -> Acc
                      end, {Unacked, 0}, Unacked)
    end.

remaining(_Ch, #{source := #{delete_after := never}}) ->
    unlimited;
remaining(Ch, #{source := #{delete_after := 'queue-length',
                            queue := Queue}}) ->
    #'queue.declare_ok'{message_count = N} =
        amqp_channel:call(Ch, #'queue.declare'{queue = Queue,
                                               passive = true}),
    N;
remaining(_Ch, #{source := #{delete_after := Count}}) ->
    Count.

%%% PARSING

try_make_parse_publish(Key, Fields) ->
    make_parse_publish(Key, Fields).

make_parse_publish(publish_fields, Fields) ->
    make_publish_fun(Fields, record_info(fields, 'basic.publish'));
make_parse_publish(publish_properties, Fields) ->
    make_publish_fun(Fields, record_info(fields, 'P_basic')).

make_publish_fun(Fields, ValidFields) when is_list(Fields) ->
    SuppliedFields = proplists:get_keys(Fields),
    case SuppliedFields -- ValidFields of
        [] ->
            FieldIndices = make_field_indices(ValidFields, Fields),
            fun (_SrcUri, _DestUri, Publish) ->
                    lists:foldl(fun ({Pos1, Value}, Pub) ->
                                        setelement(Pos1, Pub, Value)
                                end, Publish, FieldIndices)
            end;
        Unexpected ->
            fail({invalid_parameter_value, publish_properties,
                  {unexpected_fields, Unexpected, ValidFields}})
    end;
make_publish_fun(Fields, _) ->
    fail({invalid_parameter_value, publish_properties,
          {require_list, Fields}}).

make_field_indices(Valid, Fields) ->
    make_field_indices(Fields, field_map(Valid, 2), []).

make_field_indices([], _Idxs , Acc) ->
    lists:reverse(Acc);
make_field_indices([{Key, Value} | Rest], Idxs, Acc) ->
    make_field_indices(Rest, Idxs, [{dict:fetch(Key, Idxs), Value} | Acc]).

field_map(Fields, Idx0) ->
    {Dict, _IdxMax} =
        lists:foldl(fun (Field, {Dict1, Idx1}) ->
                            {dict:store(Field, Idx1, Dict1), Idx1 + 1}
                    end, {dict:new(), Idx0}, Fields),
    Dict.

-spec fail(term()) -> no_return().
fail(Reason) -> throw({error, Reason}).

add_forward_headers_fun(Name, true, PubProps) ->
    fun(SrcUri, DestUri, Props) ->
            rabbit_shovel_util:update_headers(
              [{<<"shovelled-by">>, rabbit_nodes:cluster_name()},
               {<<"shovel-type">>,  <<"static">>},
               {<<"shovel-name">>,  list_to_binary(atom_to_list(Name))}],
              [], SrcUri, DestUri, PubProps(SrcUri, DestUri, Props))
    end;
add_forward_headers_fun(_Name, false, PubProps) ->
    PubProps.

add_timestamp_header_fun(true, PubProps) ->
    fun(SrcUri, DestUri, Props) ->
        rabbit_shovel_util:add_timestamp_header(
            PubProps(SrcUri, DestUri, Props))
    end;
add_timestamp_header_fun(false, PubProps) -> PubProps.

parse_declaration({[], Acc}) ->
    Acc;
parse_declaration({[{Method, Props} | Rest], Acc}) when is_list(Props) ->
    FieldNames = try rabbit_framing_amqp_0_9_1:method_fieldnames(Method)
                 catch exit:Reason -> fail(Reason)
                 end,
    case proplists:get_keys(Props) -- FieldNames of
        []            -> ok;
        UnknownFields -> fail({unknown_fields, Method, UnknownFields})
    end,
    {Res, _Idx} = lists:foldl(
                    fun (K, {R, Idx}) ->
                            NewR = case proplists:get_value(K, Props) of
                                       undefined -> R;
                                       V         -> setelement(Idx, R, V)
                                   end,
                            {NewR, Idx + 1}
                    end, {rabbit_framing_amqp_0_9_1:method_record(Method), 2},
                    FieldNames),
    parse_declaration({Rest, [Res | Acc]});
parse_declaration({[{Method, Props} | _Rest], _Acc}) ->
    fail({expected_method_field_list, Method, Props});
parse_declaration({[Method | Rest], Acc}) ->
    parse_declaration({[{Method, []} | Rest], Acc}).

decl_fun(Endpoint) ->
    Decl = parse_declaration({proplists:get_value(declarations, Endpoint, []),
                              []}),
    fun (_Conn, Ch) ->
            [begin
                 amqp_channel:call(Ch, M)
             end || M <- lists:reverse(Decl)]
    end.

parse_parameter(Param, Fun, Value) ->
    try
        Fun(Value)
    catch
        _:{error, Err} ->
            fail({invalid_parameter_value, Param, Err})
    end.

parse_non_negative_integer(N) when is_integer(N) andalso N >= 0 ->
    N;
parse_non_negative_integer(N) ->
    fail({require_non_negative_integer, N}).

parse_binary(Binary) when is_binary(Binary) ->
    Binary;
parse_binary(NotABinary) ->
    fail({require_binary, NotABinary}).
