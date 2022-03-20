%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_event).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("rabbit_event_exchange.hrl").

-export([register/0, unregister/0]).
-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).
-export([info/1, info/2]).

-export([fmt_proplist/1]). %% testing

-record(state, {vhost,
                has_any_bindings
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
    rabbit_exchange:declare(exchange(), topic, true, false, true, [],
                            ?INTERNAL_USER),
    gen_event:add_handler(rabbit_event, ?MODULE, []).

unregister() ->
    rabbit_exchange:delete(exchange(), false, ?INTERNAL_USER),
    gen_event:delete_handler(rabbit_event, ?MODULE, []).

exchange() ->
    exchange(get_vhost()).

exchange(VHost) ->
    _ = ensure_vhost_exists(VHost),
    rabbit_misc:r(VHost, exchange, ?EXCH_NAME).

%%----------------------------------------------------------------------------

init([]) ->
    VHost = get_vhost(),
    X = rabbit_misc:r(VHost, exchange, ?EXCH_NAME),
    HasBindings = case rabbit_binding:list_for_source(X) of
                     [] -> false;
                     _ -> true
                 end,
    {ok, #state{vhost = VHost,
                has_any_bindings = HasBindings}}.

handle_call(_Request, State) -> {ok, not_understood, State}.

handle_event(_, #state{has_any_bindings = false} = State) ->
    {ok, State};
handle_event(#event{type      = Type,
                    props     = Props,
                    timestamp = TS,
                    reference = none}, #state{vhost = VHost} = State) ->
    case key(Type) of
        ignore -> ok;
        Key    ->
                  Props2 = [{<<"timestamp_in_ms">>, TS} | Props],
                  PBasic = #'P_basic'{delivery_mode = 2,
                                      headers = fmt_proplist(Props2),
                                      %% 0-9-1 says the timestamp is a
                                      %% "64 bit POSIX
                                      %% timestamp". That's second
                                      %% resolution, not millisecond.
                                      timestamp = erlang:convert_time_unit(
                                                    TS, milli_seconds, seconds)},
            Msg = rabbit_basic:message(exchange(VHost), Key, PBasic, <<>>),
                  rabbit_basic:publish(
                    rabbit_basic:delivery(false, false, Msg, undefined))
    end,
    {ok, State};
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
    case rabbit_vhost:exists(VHost) of
        false -> rabbit_vhost:add(VHost, ?INTERNAL_USER);
        _     -> ok
    end.

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

fmt_proplist(Props) ->
    lists:foldl(fun({K, V}, Acc) ->
                        case fmt(a2b(K), V) of
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
fmt(K, V) when is_pid(V)     -> {K, longstr,
                                 list_to_binary(rabbit_misc:pid_to_string(V))};
fmt(K, V)                    -> {K, longstr,
                                 list_to_binary(
                                   rabbit_misc:format("~1000000000p", [V]))}.

%% Exactly the same as fmt/2, duplicated only for performance issues
fmt(true)                 -> {bool, true};
fmt(false)                -> {bool, false};
fmt(V) when is_atom(V)    -> {longstr, atom_to_binary(V, utf8)};
fmt(V) when is_integer(V) -> {long, V};
fmt(V) when is_number(V)  -> {float, V};
fmt(V) when is_binary(V)  -> {longstr, V};
fmt([{_, _}|_] = Vs)      -> {table, fmt_proplist(Vs)};
fmt(Vs) when is_list(Vs)  -> {array, [fmt(V) || V <- Vs]};
fmt(V) when is_pid(V)     -> {longstr,
                              list_to_binary(rabbit_misc:pid_to_string(V))};
fmt(V)                    -> {longstr,
                              list_to_binary(
                                rabbit_misc:format("~1000000000p", [V]))}.

a2b(A) when is_atom(A)   -> atom_to_binary(A, utf8);
a2b(B) when is_binary(B) -> B.

get_vhost() ->
    case application:get_env(rabbitmq_event_exchange, vhost) of
        undefined ->
            {ok, V} = application:get_env(rabbit, default_vhost),
            V;
        {ok, V} ->
            V
    end.
