%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_event_consumer).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([register/4]).
-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pid, ref, monitor, pattern}).

%%----------------------------------------------------------------------------

register(Pid, Ref, Duration, Pattern) ->
    case gen_event:add_handler(rabbit_event, ?MODULE, [Pid, Ref, Duration, Pattern]) of
        ok ->
            {ok, Ref};
        Error ->
            Error
    end.

%%----------------------------------------------------------------------------

init([Pid, Ref, Duration, Pattern]) ->
    MRef = erlang:monitor(process, Pid),
    case Duration of
        infinity -> infinity;
        _        -> erlang:send_after(Duration * 1000, self(), rabbit_event_consumer_timeout)
    end,
    {ok, #state{pid = Pid, ref = Ref, monitor = MRef, pattern = Pattern}}.

handle_call(_Request, State) -> {ok, not_understood, State}.

handle_event(#event{type      = Type,
                    props     = Props,
                    timestamp = TS,
                    reference = none}, #state{pid = Pid,
                                              ref = Ref,
                                              pattern = Pattern} = State) ->
    case key(Type) of
        ignore -> ok;
        Key    -> case re:run(Key, Pattern, [{capture, none}]) of
                      match ->
                          Data = [{'event', Key}] ++
                              fmt_proplist([{'timestamp_in_ms', TS} | Props]),
                          Pid ! {Ref, Data, confinue};
                      _ ->
                          ok
                  end
    end,
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_info({'DOWN', MRef, _, _, _}, #state{monitor = MRef}) ->
    remove_handler;
handle_info(rabbit_event_consumer_timeout, #state{pid = Pid, ref = Ref}) ->
    Pid ! {Ref, <<>>, finished},
    remove_handler;
handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, #state{monitor = MRef}) ->
    erlang:demonitor(MRef),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%----------------------------------------------------------------------------

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
                        case fmt(K, V) of
                            L when is_list(L) -> lists:append(L, Acc);
                            T -> [T | Acc]
                        end
                end, [], Props).

fmt(K, #resource{virtual_host = VHost, 
                 name         = Name}) -> [{K,           Name},
                                           {'vhost', VHost}];
fmt(K, true)                 -> {K, true};
fmt(K, false)                -> {K, false};
fmt(K, V) when is_atom(V)    -> {K, atom_to_binary(V, utf8)};
fmt(K, V) when is_integer(V) -> {K, V};
fmt(K, V) when is_number(V)  -> {K, V};
fmt(K, V) when is_binary(V)  -> {K, V};
fmt(K, [{_, _}|_] = Vs)      -> {K, fmt_proplist(Vs)};
fmt(K, Vs) when is_list(Vs)  -> {K,  [fmt(V) || V <- Vs]};
fmt(K, V) when is_pid(V)     -> {K, list_to_binary(rabbit_misc:pid_to_string(V))};
fmt(K, V)                    -> {K,
                                 list_to_binary(
                                   rabbit_misc:format("~1000000000p", [V]))}.

%% Exactly the same as fmt/2, duplicated only for performance issues
fmt(true)                 -> true;
fmt(false)                -> false;
fmt(V) when is_atom(V)    -> atom_to_binary(V, utf8);
fmt(V) when is_integer(V) -> V;
fmt(V) when is_number(V)  -> V;
fmt(V) when is_binary(V)  -> V;
fmt([{_, _}|_] = Vs)      -> fmt_proplist(Vs);
fmt(Vs) when is_list(Vs)  -> [fmt(V) || V <- Vs];
fmt(V) when is_pid(V)     -> list_to_binary(rabbit_misc:pid_to_string(V));
fmt(V)                    -> list_to_binary(
                                rabbit_misc:format("~1000000000p", [V])).
