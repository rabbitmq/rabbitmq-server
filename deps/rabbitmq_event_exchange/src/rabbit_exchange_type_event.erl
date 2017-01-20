%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_event).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-export([register/0, unregister/0]).
-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).
-export([info/1, info/2]).

-export([fmt_proplist/1]). %% testing

-define(EXCH_NAME, <<"amq.rabbitmq.event">>).

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
    gen_event:delete_handler(rabbit_event, ?MODULE, []).

exchange() ->
    VHost = ensure_vhost_exists(),
    rabbit_misc:r(VHost, exchange, ?EXCH_NAME).

%%----------------------------------------------------------------------------

init([]) -> {ok, []}.

handle_call(_Request, State) -> {ok, not_understood, State}.

handle_event(#event{type      = Type,
                    props     = Props,
                    timestamp = TS,
                    reference = none}, State) ->
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
                  Msg = rabbit_basic:message(exchange(), Key, PBasic, <<>>),
                  rabbit_basic:publish(
                    rabbit_basic:delivery(false, false, Msg, undefined))
    end,
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_info(_Info, State) -> {ok, State}.

terminate(_Arg, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%----------------------------------------------------------------------------

ensure_vhost_exists() ->
    VHost = case application:get_env(rabbitmq_event_exchange, vhost) of
                undefined ->
                    {ok, V} = application:get_env(rabbit, default_vhost),
                    V;
                {ok, V} ->
                    V
            end,
    case rabbit_vhost:exists(VHost) of
        false -> rabbit_vhost:add(VHost, ?INTERNAL_USER);
        _     -> ok
    end,
    VHost.

key(S) ->
    case string:tokens(atom_to_list(S), "_") of
        [_, "stats"] -> ignore;
        Tokens       -> list_to_binary(string:join(Tokens, "."))
    end.

fmt_proplist(Props) ->
    lists:append([fmt(a2b(K), V) || {K, V} <- Props]).

fmt(K, #resource{virtual_host = VHost, 
                 name         = Name}) -> [{K,           longstr, Name},
                                           {<<"vhost">>, longstr, VHost}];
fmt(K, V) -> {T, Enc} = fmt(V),
             [{K, T, Enc}].

fmt(true)                 -> {bool, true};
fmt(false)                -> {bool, false};
fmt(V) when is_atom(V)    -> {longstr, a2b(V)};
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

a2b(A) when is_atom(A)   -> list_to_binary(atom_to_list(A));
a2b(B) when is_binary(B) -> B.
