%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_dispatch_registry).

-behaviour(gen_server).

-export([start_link/0]).
-export([add/5, remove/1, set_fallback/2, lookup/2, list_all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([list/1]).

-import(rabbit_misc, [pget/2]).

-define(ETS, rabbitmq_web_dispatch).

%% This gen_server is merely to serialise modifications to the dispatch
%% table for listeners.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add(Name, Listener, Selector, Handler, Link) ->
    gen_server:call(?MODULE, {add, Name, Listener, Selector, Handler, Link},
                    infinity).

remove(Name) ->
    gen_server:call(?MODULE, {remove, Name}, infinity).

%% @todo This needs to be dispatch instead of a fun too.
%% But I'm not sure what code is using this.
set_fallback(Listener, FallbackHandler) ->
    gen_server:call(?MODULE, {set_fallback, Listener, FallbackHandler},
                    infinity).

lookup(Listener, Req) ->
    case lookup_dispatch(Listener) of
        {ok, {Selectors, Fallback}} ->
            case catch match_request(Selectors, Req) of
                {'EXIT', Reason} -> {error, {lookup_failure, Reason}};
                not_found        -> {ok, Fallback};
                Dispatch         -> {ok, Dispatch}
            end;
        Err ->
            Err
    end.

%% This is called in a somewhat obfuscated manner in
%% rabbit_mgmt_external_stats:rabbit_web_dispatch_registry_list_all()
list_all() ->
    gen_server:call(?MODULE, list_all, infinity).

%% Callback Methods

init([]) ->
    ?ETS = ets:new(?ETS, [named_table, public]),
    {ok, undefined}.

handle_call({add, Name, Listener, Selector, Handler, Link = {_, Desc}}, _From,
            undefined) ->
    Continue = case rabbit_web_dispatch_sup:ensure_listener(Listener) of
                   new      -> set_dispatch(
                                 Listener, [],
                                 listing_fallback_handler(Listener)),
                               listener_started(Listener),
                               true;
                   existing -> true;
                   ignore   -> false
               end,
    case Continue of
        true  -> case lookup_dispatch(Listener) of
                     {ok, {Selectors, Fallback}} ->
                         Selector2 = lists:keystore(
                                       Name, 1, Selectors,
                                       {Name, Selector, Handler, Link}),
                         set_dispatch(Listener, Selector2, Fallback);
                     {error, {different, Desc2, Listener2}} ->
                         exit({incompatible_listeners,
                               {Desc, Listener}, {Desc2, Listener2}})
                 end;
        false -> ok
    end,
    {reply, ok, undefined};

handle_call({remove, Name}, _From,
            undefined) ->
    case listener_by_name(Name) of
        {error, not_found} ->
            _ = rabbit_log:warning("HTTP listener registry could not find context ~p",
                               [Name]),
            {reply, ok, undefined};
        {ok, Listener} ->
            {ok, {Selectors, Fallback}} = lookup_dispatch(Listener),
            Selectors1 = lists:keydelete(Name, 1, Selectors),
            set_dispatch(Listener, Selectors1, Fallback),
            case Selectors1 of
                [] -> rabbit_web_dispatch_sup:stop_listener(Listener),
                      listener_stopped(Listener);
                _  -> ok
            end,
            {reply, ok, undefined}
    end;

handle_call({set_fallback, Listener, FallbackHandler}, _From,
            undefined) ->
    {ok, {Selectors, _OldFallback}} = lookup_dispatch(Listener),
    set_dispatch(Listener, Selectors, FallbackHandler),
    {reply, ok, undefined};

handle_call(list_all, _From, undefined) ->
    {reply, list(), undefined};

handle_call(Req, _From, State) ->
    _ = rabbit_log:error("Unexpected call to ~p: ~p~n", [?MODULE, Req]),
    {stop, unknown_request, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    true = ets:delete(?ETS),
    ok.

code_change(_, State, _) ->
    {ok, State}.

%%---------------------------------------------------------------------------

%% Internal Methods

listener_started(Listener) ->
    [rabbit_networking:tcp_listener_started(Protocol, Listener, IPAddress, Port)
     || {Protocol, IPAddress, Port} <- listener_info(Listener)],
    ok.

listener_stopped(Listener) ->
    [rabbit_networking:tcp_listener_stopped(Protocol, Listener, IPAddress, Port)
     || {Protocol, IPAddress, Port} <- listener_info(Listener)],
    ok.

listener_info(Listener) ->
    Protocol = case pget(protocol, Listener) of
                   undefined ->
                       case pget(ssl, Listener) of
                           true -> https;
                           _    -> http
                       end;
                   P ->
                       P
               end,
    Port = pget(port, Listener),
    [{IPAddress, _Port, _Family} | _]
        = rabbit_networking:tcp_listener_addresses(Port),
    [{Protocol, IPAddress, Port}].

lookup_dispatch(Lsnr) ->
    case ets:lookup(?ETS, pget(port, Lsnr)) of
        [{_, Lsnr, S, F}]   -> {ok, {S, F}};
        [{_, Lsnr2, S, _F}] -> {error, {different, first_desc(S), Lsnr2}};
        []                  -> {error, {no_record_for_listener, Lsnr}}
    end.

first_desc([{_N, _S, _H, {_, Desc}} | _]) -> Desc.

set_dispatch(Listener, Selectors, Fallback) ->
    ets:insert(?ETS, {pget(port, Listener), Listener, Selectors, Fallback}).

match_request([], _) ->
    not_found;
match_request([{_Name, Selector, Dispatch, _Link}|Rest], Req) ->
    case Selector(Req) of
        true  -> Dispatch;
        false -> match_request(Rest, Req)
    end.

list() ->
    [{Path, Desc, Listener} ||
        {_P, Listener, Selectors, _F} <- ets:tab2list(?ETS),
        {_N, _S, _H, {Path, Desc}} <- Selectors].

-spec listener_by_name(atom()) -> {ok, term()} | {error, not_found}.
listener_by_name(Name) ->
    case [L || {_P, L, S, _F} <- ets:tab2list(?ETS), contains_name(Name, S)] of
        [Listener] -> {ok, Listener};
        []         -> {error, not_found}
    end.

contains_name(Name, Selectors) ->
    lists:member(Name, [N || {N, _S, _H, _L} <- Selectors]).

list(Listener) ->
    {ok, {Selectors, _Fallback}} = lookup_dispatch(Listener),
    [{Path, Desc} || {_N, _S, _H, {Path, Desc}} <- Selectors].

%%---------------------------------------------------------------------------

listing_fallback_handler(Listener) ->
    cowboy_router:compile([{'_', [
        {"/", rabbit_web_dispatch_listing_handler, Listener}
    ]}]).
