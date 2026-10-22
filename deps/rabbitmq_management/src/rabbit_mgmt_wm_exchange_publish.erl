%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_exchange_publish).

-export([init/2, resource_exists/2, allow_missing_post/2, is_authorized/2,
         allowed_methods/2,  content_types_provided/2, accept_content/2,
         content_types_accepted/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit/include/mc.hrl").

%% Kept below the Cowboy request idle timeout so that a response is
%% returned before the socket is closed.
-define(DEFAULT_PUBLISH_TIMEOUT, 30_000).

-define(IS_AMQP_LONG(V), (is_integer(V) andalso
                          V >= -9223372036854775808 andalso
                          V =< 9223372036854775807)).

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData0, Context) ->
    case rabbit_mgmt_wm_exchange:exchange(ReqData0) of
        not_found ->
            ReqData1 = rabbit_mgmt_util:set_resp_not_found(<<"exchange_not_found">>, ReqData0),
            {false, ReqData1, Context};
        _ ->
            {true, ReqData0, Context}
    end.

allow_missing_post(ReqData, Context) ->
    {false, ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

accept_content(ReqData, Context) ->
    rabbit_mgmt_util:post_respond(do_it(ReqData, Context)).

do_it(ReqData0, Context) ->
    VHost = rabbit_mgmt_util:vhost(ReqData0),
    X = rabbit_mgmt_util:id(exchange, ReqData0),
    rabbit_mgmt_util:with_decode(
      [routing_key, properties, payload, payload_encoding], ReqData0, Context,
      fun ([RoutingKey, Props, Payload0, Enc], Json, ReqData)
            when is_binary(Payload0), is_binary(RoutingKey) ->
              case maps:get(<<"protocol">>, Json, <<"amqp091">>) of
                  <<"amqp091">> ->
                      publish_amqp091(VHost, X, RoutingKey, Props, Payload0, Enc,
                                      ReqData, Context);
                  <<"amqp">> ->
                      publish_amqp(VHost, X, RoutingKey, Props, Payload0, Enc,
                                   Json, ReqData, Context);
                  Protocol ->
                      throw({error, {unsupported_protocol, Protocol}})
              end;
          ([_RoutingKey, _Props, Payload, _Enc], _, _ReqData) when is_binary(Payload) ->
              throw({error, routing_key_not_string});
          ([_RoutingKey, _Props, _Payload, _Enc], _, _ReqData) ->
              throw({error, payload_not_string})
      end).

publish_amqp091(VHost, X, RoutingKey, Props0, Payload0, Enc, ReqData, Context) ->
              rabbit_mgmt_util:with_channel(
                VHost, ReqData, Context,
                fun (Ch) ->
                        MRef = erlang:monitor(process, Ch),
                        amqp_channel:register_confirm_handler(Ch, self()),
                        amqp_channel:register_return_handler(Ch, self()),
                        amqp_channel:call(Ch, #'confirm.select'{}),
                        Props = rabbit_mgmt_format:to_basic_properties(Props0),
                        Payload = decode(Payload0, Enc),
                        amqp_channel:cast(Ch, #'basic.publish'{
                                            exchange    = X,
                                            routing_key = RoutingKey,
                                            mandatory   = true},
                                          #amqp_msg{props   = Props,
                                                    payload = Payload}),
                        Timeout = publish_timeout(),
                        receive
                            {#'basic.return'{}, _} ->
                                receive
                                    #'basic.ack'{} ->
                                        good(MRef, false, ReqData, Context);
                                    #'basic.nack'{} ->
                                        bad(rejected, MRef, ReqData, Context)
                                after Timeout ->
                                        bad(confirm_timeout, MRef, ReqData, Context)
                                end;
                            #'basic.ack'{} ->
                                good(MRef, true, ReqData, Context);
                            #'basic.nack'{} ->
                                bad(rejected, MRef, ReqData, Context);
                            {'DOWN', _, _, _, Err} ->
                                bad(Err, ReqData, Context)
                        after Timeout ->
                                bad(confirm_timeout, MRef, ReqData, Context)
                        end
                end).

publish_amqp(VHost, XNameBin, RoutingKey, Props, Payload0, Enc, Json,
             ReqData, Context = #context{user = User}) ->
    XName = rabbit_misc:r(VHost, exchange, XNameBin),
    try rabbit_access_control:check_resource_access(User, XName, write, #{}) of
        ok ->
            publish_amqp(XName, RoutingKey, Props, Payload0, Enc, Json,
                         ReqData, Context)
    catch
        exit:#amqp_error{explanation = Expl} ->
            rabbit_mgmt_util:not_authorised(Expl, ReqData, Context)
    end.

publish_amqp(XName = #resource{virtual_host = VHost, name = XNameBin},
             RoutingKey, Props, Payload0, Enc, Json, ReqData,
             Context = #context{user = User}) ->
    X = case rabbit_exchange:lookup(XName) of
            {ok, X0} -> X0;
            {error, not_found} -> throw({error, exchange_not_found})
        end,
    Payload = decode(Payload0, Enc),
    Mc0 = mc:init(mc_amqp, amqp_sections(Props, Json, Payload), #{}),
    case rabbit_access_control:check_user_id(Mc0, User) of
        ok -> ok;
        {refused, Reason, Args} ->
            throw({error, iolist_to_binary(io_lib:format(Reason, Args))})
    end,
    Mc1 = mc:set_annotation(?ANN_EXCHANGE, XNameBin, Mc0),
    Mc2 = mc:set_annotation(?ANN_ROUTING_KEYS, [RoutingKey], Mc1),
    IcptCtx = #{protocol => amqp10,
                vhost => VHost,
                username => User#user.username,
                connection_name => <<"HTTP API">>},
    Mc = rabbit_msg_interceptor:intercept_incoming(Mc2, IcptCtx),
    Qs = rabbit_amqqueue:prepend_extra_bcc(
           rabbit_db_queue:get_targets(
             rabbit_exchange:route(X, Mc, #{return_binding_keys => true}))),
    case Qs of
        [] ->
            rabbit_mgmt_util:reply([{routed, false}], ReqData, Context);
        _ ->
            Opts = #{correlation => make_ref()},
            case rabbit_queue_type:deliver(Qs, Mc, Opts, rabbit_queue_type:init()) of
                {ok, QStates, Actions} ->
                    Pending = sets:from_list([amqqueue:get_name(Q) || Q <- queues(Qs)],
                                             [{version, 2}]),
                    Deadline = erlang:monotonic_time(millisecond) + publish_timeout(),
                    await_confirms(Actions, Pending, QStates, Deadline, ReqData, Context);
                {error, Err} ->
                    bad(Err, ReqData, Context)
            end
    end.

queues(Qs) ->
    [case Q of
         {Q1, _RouteInfos} -> Q1;
         Q1 -> Q1
     end || Q <- Qs].

await_confirms(Actions, Pending0, QStates, Deadline, ReqData, Context) ->
    case confirm_actions(Actions, Pending0) of
        rejected ->
            bad(rejected, ReqData, Context);
        Pending ->
            case sets:is_empty(Pending) of
                true ->
                    rabbit_mgmt_util:reply([{routed, true}], ReqData, Context);
                false ->
                    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
                    receive
                        {'$gen_cast', {queue_event, QRef, Evt}} ->
                            case rabbit_queue_type:handle_event(QRef, Evt, QStates) of
                                {ok, QStates1, Actions1} ->
                                    await_confirms(Actions1, Pending, QStates1,
                                                   Deadline, ReqData, Context);
                                _ ->
                                    bad(rejected, ReqData, Context)
                            end
                    after Timeout ->
                            bad(confirm_timeout, ReqData, Context)
                    end
            end
    end.

confirm_actions([], Pending) ->
    Pending;
confirm_actions([{settled, QName, _} | Rest], Pending) ->
    confirm_actions(Rest, sets:del_element(QName, Pending));
confirm_actions([{rejected, _, _, _} | _], _Pending) ->
    rejected;
confirm_actions([_ | Rest], Pending) ->
    confirm_actions(Rest, Pending).

amqp_sections(Props, Json, Payload) ->
    Header = amqp_header(maps:get(<<"header">>, Json, #{})),
    Anns = amqp_map(message_annotations,
                    maps:get(<<"message_annotations">>, Json, #{})),
    Properties = amqp_properties(Props),
    AppProps = amqp_map(application_properties,
                        maps:get(<<"application_properties">>, Json, #{})),
    Sections = [S || S <- [Header, Anns, Properties, AppProps], S =/= undefined],
    iolist_to_binary(
      [amqp10_framing:encode_bin(S) || S <- Sections] ++
      [amqp10_framing:encode_bin(#'v1_0.data'{content = Payload})]).

amqp_header(H) when map_size(H) =:= 0 ->
    undefined;
amqp_header(H) when is_map(H) ->
    reject_unknown_keys(header, [<<"durable">>, <<"priority">>, <<"ttl">>], H),
    #'v1_0.header'{
       durable = case maps:get(<<"durable">>, H, undefined) of
                     undefined -> undefined;
                     D when is_boolean(D) -> D;
                     _ -> throw({error, {invalid_amqp_header, durable}})
                 end,
       priority = case maps:get(<<"priority">>, H, undefined) of
                      undefined -> undefined;
                      P when is_integer(P), P >= 0, P =< 255 -> {ubyte, P};
                      _ -> throw({error, {invalid_amqp_header, priority}})
                  end,
       ttl = case maps:get(<<"ttl">>, H, undefined) of
                 undefined -> undefined;
                 T when is_integer(T), T >= 0, T =< 4294967295 -> {uint, T};
                 _ -> throw({error, {invalid_amqp_header, ttl}})
             end};
amqp_header(_) ->
    throw({error, {invalid_amqp_section, header}}).

amqp_properties(P) when map_size(P) =:= 0 ->
    undefined;
amqp_properties(P) when is_map(P) ->
    reject_unknown_keys(
      properties,
      [<<"message_id">>, <<"user_id">>, <<"to">>, <<"subject">>, <<"reply_to">>,
       <<"correlation_id">>, <<"content_type">>, <<"content_encoding">>,
       <<"absolute_expiry_time">>, <<"creation_time">>, <<"group_id">>,
       <<"group_sequence">>, <<"reply_to_group_id">>], P),
    Utf8 = fun(K) -> amqp_prop(K, P, fun(V) when is_binary(V) -> {utf8, V} end) end,
    Symbol = fun(K) -> amqp_prop(K, P, fun(V) when is_binary(V) -> {symbol, V} end) end,
    Time = fun(K) -> amqp_prop(K, P, fun(V) when ?IS_AMQP_LONG(V), V >= 0 -> {timestamp, V} end) end,
    #'v1_0.properties'{
       message_id = Utf8(<<"message_id">>),
       user_id = amqp_prop(<<"user_id">>, P, fun(V) when is_binary(V) -> {binary, V} end),
       to = Utf8(<<"to">>),
       subject = Utf8(<<"subject">>),
       reply_to = Utf8(<<"reply_to">>),
       correlation_id = Utf8(<<"correlation_id">>),
       content_type = Symbol(<<"content_type">>),
       content_encoding = Symbol(<<"content_encoding">>),
       absolute_expiry_time = Time(<<"absolute_expiry_time">>),
       creation_time = Time(<<"creation_time">>),
       group_id = Utf8(<<"group_id">>),
       group_sequence = amqp_prop(<<"group_sequence">>, P,
                                  fun(V) when is_integer(V), V >= 0,
                                              V =< 4294967295 -> {uint, V} end),
       reply_to_group_id = Utf8(<<"reply_to_group_id">>)};
amqp_properties(_) ->
    throw({error, {invalid_amqp_section, properties}}).

reject_unknown_keys(Section, Known, Map) ->
    case maps:keys(Map) -- Known of
        [] -> ok;
        [Key | _] -> throw({error, {unknown_amqp_property, Section, Key}})
    end.

amqp_prop(Key, Props, Fun) ->
    case maps:get(Key, Props, undefined) of
        undefined -> undefined;
        <<>> -> undefined;
        V ->
            try Fun(V)
            catch error:function_clause ->
                      throw({error, {invalid_amqp_property, Key}})
            end
    end.

amqp_map(_Section, M) when map_size(M) =:= 0 ->
    undefined;
amqp_map(Section, M) when is_map(M) ->
    Content = [{amqp_key(Section, K), amqp_value(V)} || {K, V} <- maps:to_list(M)],
    case Section of
        message_annotations -> #'v1_0.message_annotations'{content = Content};
        application_properties -> #'v1_0.application_properties'{content = Content}
    end;
amqp_map(Section, _) ->
    throw({error, {invalid_amqp_section, Section}}).

amqp_key(message_annotations, <<"x-", _/binary>> = K) -> {symbol, K};
amqp_key(message_annotations, K) ->
    throw({error, {invalid_message_annotation, K}});
amqp_key(application_properties, K) when is_binary(K) -> {utf8, K}.

amqp_value(V) when is_binary(V) -> {utf8, V};
amqp_value(V) when is_boolean(V) -> V;
amqp_value(V) when ?IS_AMQP_LONG(V) -> {long, V};
amqp_value(V) when is_integer(V) -> throw({error, amqp_integer_out_of_range});
amqp_value(V) when is_float(V) -> {double, V};
amqp_value(null) -> null;
amqp_value(_) -> throw({error, unsupported_amqp_value}).

good(MRef, Routed, ReqData, Context) ->
    erlang:demonitor(MRef),
    rabbit_mgmt_util:reply([{routed, Routed}], ReqData, Context).

bad(Reason, MRef, ReqData, Context) ->
    erlang:demonitor(MRef),
    bad(Reason, ReqData, Context).

bad({shutdown, {connection_closing,
                {server_initiated_close, Code, Reason}}}, ReqData, Context) ->
    rabbit_mgmt_util:bad_request_exception(Code, Reason, ReqData, Context);

bad({shutdown, {server_initiated_close, Code, Reason}}, ReqData, Context) ->
    rabbit_mgmt_util:bad_request_exception(Code, Reason, ReqData, Context);
bad(rejected, ReqData, Context) ->
    Msg = "Unable to publish message. Check queue limits.",
    rabbit_mgmt_util:bad_request_exception(rejected, Msg, ReqData, Context);
bad({{coordinator_unavailable, _}, _}, ReqData, Context) ->
    Msg = "Unable to publish message. Coordinator unavailable.",
    rabbit_mgmt_util:bad_request_exception(rejected, Msg, ReqData, Context);
bad(confirm_timeout, ReqData, Context) ->
    Msg = "Timed out waiting for a publisher confirm. The message may still "
          "have been enqueued. Check the health of the queues bound to this exchange.",
    rabbit_mgmt_util:bad_request_exception(confirm_timeout, Msg, ReqData, Context).

publish_timeout() ->
    application:get_env(rabbitmq_management, publish_timeout,
                        ?DEFAULT_PUBLISH_TIMEOUT).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

decode(Payload, <<"string">>) -> Payload;
decode(Payload, <<"base64">>) -> rabbit_mgmt_util:b64decode_or_throw(Payload);
decode(_Payload, Enc)         -> throw({error, {unsupported_encoding, Enc}}).
