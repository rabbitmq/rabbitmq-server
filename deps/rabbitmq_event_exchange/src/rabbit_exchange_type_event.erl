%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_exchange_type_event).

-behaviour(gen_event).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit/include/mc.hrl").
-include("rabbit_event_exchange.hrl").

-export([register/0, unregister/0]).
-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).
-export([info/1, info/2]).

-export([fmt_proplist/1]). %% testing

-define(APP_NAME, rabbitmq_event_exchange).

-record(state, {protocol :: amqp_0_9_1 | amqp_1_0,
                vhost :: rabbit_types:vhost(),
                has_any_bindings :: boolean()
               }).

-rabbit_boot_step({rabbit_event_exchange,
                   [{description, "event exchange"},
                    {mfa,         {?MODULE, register, []}},
                    {cleanup,     {?MODULE, unregister, []}},
                    {requires,    recovery},
                    {enables,     routing_ready}]}).

%%----------------------------------------------------------------------------

info(_X) -> [].

info(_X, _) -> [].

register() ->
    case rabbit_exchange:declare(exchange(), topic, true, false, true, [],
                                 ?INTERNAL_USER) of
        {ok, _Exchange} ->
            gen_event:add_handler(rabbit_event, ?MODULE, []);
        {error, timeout} = Err ->
            Err
    end.

unregister() ->
    case rabbit_exchange:ensure_deleted(exchange(), false, ?INTERNAL_USER) of
        ok ->
            gen_event:delete_handler(rabbit_event, ?MODULE, []),
            ok;
        {error, _} = Err ->
            Err
    end.

exchange() ->
    exchange(get_vhost()).

exchange(VHost) ->
    _ = ensure_vhost_exists(VHost),
   rabbit_misc:r(VHost, exchange, ?EXCH_NAME).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, Protocol} = application:get_env(?APP_NAME, protocol),
    VHost = get_vhost(),
    X = rabbit_misc:r(VHost, exchange, ?EXCH_NAME),
    HasBindings = case rabbit_binding:list_for_source(X) of
                      [] -> false;
                      _ -> true
                  end,
    {ok, #state{protocol = Protocol,
                vhost = VHost,
                has_any_bindings = HasBindings}}.

handle_call(_Request, State) -> {ok, not_understood, State}.

handle_event(#event{type = Type,
                    props = Props,
                    reference = none,
                    timestamp = Timestamp},
             #state{protocol = Protocol,
                    vhost = VHost,
                    has_any_bindings = true} = State) ->
    case key(Type) of
        ignore ->
            {ok, State};
        Key ->
            XName = exchange(VHost),
            Mc = mc_init(Protocol, XName, Key, Props, Timestamp),
            _ = rabbit_queue_type:publish_at_most_once(XName, Mc),
            {ok, State}
    end;
handle_event(_Event, State) ->
    {ok, State}.

handle_info({event_exchange, added_first_binding}, State) ->
    {ok, State#state{has_any_bindings = true}};
handle_info({event_exchange, removed_last_binding}, State) ->
    {ok, State#state{has_any_bindings = false}};
handle_info(_Info, State) -> {ok, State}.

terminate(_Arg, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%----------------------------------------------------------------------------

ensure_vhost_exists(VHost) ->
    rabbit_vhost:add(VHost, ?INTERNAL_USER).

%% pattern matching is way more efficient that the string operations,
%% let's use all the keys we're aware of to speed up the handler.
%% Any unknown or new one will be processed as before (see last function clause).
key(queue_deleted) ->
    <<"queue.deleted">>;
key(queue_created) ->
    <<"queue.created">>;
key(exchange_created) ->
    <<"exchange.created">>;
key(exchange_deleted) ->
    <<"exchange.deleted">>;
key(binding_created) ->
    <<"binding.created">>;
key(connection_created) ->
    <<"connection.created">>;
key(connection_closed) ->
    <<"connection.closed">>;
key(channel_created) ->
    <<"channel.created">>;
key(channel_closed) ->
    <<"channel.closed">>;
key(consumer_created) ->
    <<"consumer.created">>;
key(consumer_deleted) ->
    <<"consumer.deleted">>;
key(queue_stats) ->
    ignore;
key(connection_stats) ->
    ignore;
key(policy_set) ->
    <<"policy.set">>;
key(policy_cleared) ->
    <<"policy.cleared">>;
key(queue_policy_updated) ->
    <<"queue.policy.updated">>;
key(queue_policy_cleared) ->
    <<"queue.policy.cleared">>;
key(parameter_set) ->
    <<"parameter.set">>;
key(parameter_cleared) ->
    <<"parameter.cleared">>;
key(vhost_created) ->
    <<"vhost.created">>;
key(vhost_deleted) ->
    <<"vhost.deleted">>;
key(vhost_limits_set) ->
    <<"vhost.limits.set">>;
key(vhost_limits_cleared) ->
    <<"vhost.limits.cleared">>;
key(user_authentication_success) ->
    <<"user.authentication.success">>;
key(user_authentication_failure) ->
    <<"user.authentication.failure">>;
key(user_created) ->
    <<"user.created">>;
key(user_deleted) ->
    <<"user.deleted">>;
key(user_password_changed) ->
    <<"user.password.changed">>;
key(user_password_cleared) ->
    <<"user.password.cleared">>;
key(user_tags_set) ->
    <<"user.tags.set">>;
key(permission_created) ->
    <<"permission.created">>;
key(permission_deleted) ->
    <<"permission.deleted">>;
key(topic_permission_created) ->
    <<"topic.permission.created">>;
key(topic_permission_deleted) ->
    <<"topic.permission.deleted">>;
key(alarm_set) ->
    <<"alarm.set">>;
key(alarm_cleared) ->
    <<"alarm.cleared">>;
key(shovel_worker_status) ->
    <<"shovel.worker.status">>;
key(shovel_worker_removed) ->
    <<"shovel.worker.removed">>;
key(federation_link_status) ->
    <<"federation.link.status">>;
key(federation_link_removed) ->
    <<"federation.link.removed">>;
key(S) ->
    case string:tokens(atom_to_list(S), "_") of
        [_, "stats"] -> ignore;
        Tokens       -> list_to_binary(string:join(Tokens, "."))
    end.

get_vhost() ->
    case application:get_env(?APP_NAME, vhost) of
        undefined ->
            {ok, V} = application:get_env(rabbit, default_vhost),
            V;
        {ok, V} ->
            V
    end.

mc_init(amqp_1_0, #resource{name = XNameBin}, Key, Props, Timestamp) ->
    Sections = [#'v1_0.message_annotations'{content = props_to_message_annotations(Props)},
                #'v1_0.properties'{creation_time = {timestamp, Timestamp}},
                #'v1_0.data'{content = <<>>}],
    Payload = iolist_to_binary([amqp10_framing:encode_bin(S) || S <- Sections]),
    Anns = #{?ANN_EXCHANGE => XNameBin,
             ?ANN_ROUTING_KEYS => [Key]},
    mc:init(mc_amqp, Payload, Anns);
mc_init(amqp_0_9_1, XName, Key, Props0, TimestampMillis) ->
    Props = [{<<"timestamp_in_ms">>, TimestampMillis} | Props0],
    Headers = fmt_proplist(Props),
    TimestampSecs = erlang:convert_time_unit(TimestampMillis, millisecond, second),
    PBasic = #'P_basic'{delivery_mode = 2,
                        headers = Headers,
                        timestamp = TimestampSecs},
    Content = rabbit_basic:build_content(PBasic, <<>>),
    {ok, Mc} = mc_amqpl:message(XName, Key, Content),
    Mc.

props_to_message_annotations(Props) ->
    KVList = lists:foldl(
               fun({K, #resource{virtual_host = Vhost, name = Name}}, Acc) ->
                       Ann0 = {to_message_annotation_key(K), {utf8, Name}},
                       Ann1 = {{symbol, <<"x-opt-vhost">>}, {utf8, Vhost}},
                       [Ann0, Ann1 | Acc];
                  ({K, V}, Acc) ->
                       Ann = {to_message_annotation_key(K),
                              to_message_annotation_val(V)},
                       [Ann | Acc]
               end, [], Props),
    lists:reverse(KVList).

to_message_annotation_key(Key) ->
    Key1 = to_binary(Key),
    Pattern = try persistent_term:get(cp_underscore)
              catch error:badarg ->
                        Cp = binary:compile_pattern(<<"_">>),
                        ok = persistent_term:put(cp_underscore, Cp),
                        Cp
              end,
    Key2 = binary:replace(Key1, Pattern, <<"-">>, [global]),
    Key3 = case Key2 of
               <<"x-", _/binary>> ->
                   Key2;
               _ ->
                   <<"x-opt-", Key2/binary>>
           end,
    {symbol, Key3}.

to_message_annotation_val(V)
  when is_boolean(V) ->
    {boolean, V};
to_message_annotation_val(V)
  when is_atom(V) ->
    {utf8, atom_to_binary(V, utf8)};
to_message_annotation_val(V)
  when is_binary(V) ->
    case mc_util:is_utf8_no_null_limited(V) of
        true ->
            {utf8, V};
        false ->
            {binary, V}
    end;
to_message_annotation_val(V)
  when is_integer(V) ->
    {long, V};
to_message_annotation_val(V)
  when is_number(V) ->
    %% AMQP double and Erlang float are both 64-bit.
    {double, V};
to_message_annotation_val(V)
  when is_pid(V) ->
    {utf8, to_pid(V)};
to_message_annotation_val([{Key, _} | _] = Proplist)
  when is_atom(Key) orelse
       is_binary(Key) ->
    {map, lists:map(fun({K, V}) ->
                            {{utf8, to_binary(K)},
                             to_message_annotation_val(V)}
                    end, Proplist)};
to_message_annotation_val([{Key, Type, _Value} | _] = Table)
  when is_binary(Key) andalso
       is_atom(Type) ->
    %% Looks like an AMQP 0.9.1 table
    mc_amqpl:from_091(table, Table);
to_message_annotation_val(V)
  when is_list(V) ->
    {list, [to_message_annotation_val(Val) || Val <- V]};
to_message_annotation_val(V) ->
    {utf8, fmt_other(V)}.

fmt_proplist(Props) ->
    lists:foldl(fun({K, V}, Acc) ->
                        case fmt(to_binary(K), V) of
                            L when is_list(L) -> lists:append(L, Acc);
                            T -> [T | Acc]
                        end
                end, [], Props).

fmt(K, #resource{virtual_host = VHost, 
                 name         = Name}) -> [{K,           longstr, Name},
                                           {<<"vhost">>, longstr, VHost}];
fmt(K, true)                 -> {K, bool, true};
fmt(K, false)                -> {K, bool, false};
fmt(K, V) when is_atom(V)    -> {K, longstr, atom_to_binary(V, utf8)};
fmt(K, V) when is_integer(V) -> {K, long, V};
fmt(K, V) when is_number(V)  -> {K, float, V};
fmt(K, V) when is_binary(V)  -> {K, longstr, V};
fmt(K, [{_, _}|_] = Vs)      -> {K, table, fmt_proplist(Vs)};
fmt(K, Vs) when is_list(Vs)  -> {K, array, [fmt(V) || V <- Vs]};
fmt(K, V) when is_pid(V)     -> {K, longstr, to_pid(V)};
fmt(K, V)                    -> {K, longstr, fmt_other(V)}.

%% Exactly the same as fmt/2, duplicated only for performance issues
fmt(true)                 -> {bool, true};
fmt(false)                -> {bool, false};
fmt(V) when is_atom(V)    -> {longstr, atom_to_binary(V, utf8)};
fmt(V) when is_integer(V) -> {long, V};
fmt(V) when is_number(V)  -> {float, V};
fmt(V) when is_binary(V)  -> {longstr, V};
fmt([{_, _}|_] = Vs)      -> {table, fmt_proplist(Vs)};
fmt(Vs) when is_list(Vs)  -> {array, [fmt(V) || V <- Vs]};
fmt(V) when is_pid(V)     -> {longstr, to_pid(V)};
fmt(V)                    -> {longstr, fmt_other(V)}.

fmt_other(V) ->
    list_to_binary(rabbit_misc:format("~1000000000p", [V])).

to_binary(Val) when is_atom(Val) ->
    atom_to_binary(Val);
to_binary(Val) when is_binary(Val) ->
    Val.

to_pid(Val) ->
    list_to_binary(rabbit_misc:pid_to_string(Val)).
