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
%% Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_web_dispatch_registry).

-behaviour(gen_server).

-export([start_link/0]).
-export([add/5, remove/1, set_fallback/2, lookup/2, list_all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

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

set_fallback(Listener, FallbackHandler) ->
    gen_server:call(?MODULE, {set_fallback, Listener, FallbackHandler},
                    infinity).

lookup(Listener, Req) ->
    case lookup_dispatch(Listener) of
        {ok, {Selectors, Fallback}} ->
            case catch match_request(Selectors, Req) of
                {'EXIT', Reason} -> {lookup_failure, Reason};
                no_handler       -> {handler, Fallback};
                Handler          -> {handler, Handler}
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
    Listener = listener_by_name(Name),
    {ok, {Selectors, Fallback}} = lookup_dispatch(Listener),
    Selectors1 = lists:keydelete(Name, 1, Selectors),
    set_dispatch(Listener, Selectors1, Fallback),
    case Selectors1 of
        [] -> rabbit_web_dispatch_sup:stop_listener(Listener);
        _  -> ok
    end,
    {reply, ok, undefined};

handle_call({set_fallback, Listener, FallbackHandler}, _From,
            undefined) ->
    {ok, {Selectors, _OldFallback}} = lookup_dispatch(Listener),
    set_dispatch(Listener, Selectors, FallbackHandler),
    {reply, ok, undefined};

handle_call(list_all, _From, undefined) ->
    {reply, list(), undefined};

handle_call(Req, _From, State) ->
    error_logger:format("Unexpected call to ~p: ~p~n", [?MODULE, Req]),
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

port(Listener) -> proplists:get_value(port, Listener).

lookup_dispatch(Lsnr) ->
    case ets:lookup(?ETS, port(Lsnr)) of
        [{_, Lsnr, S, F}]   -> {ok, {S, F}};
        [{_, Lsnr2, S, _F}] -> {error, {different, first_desc(S), Lsnr2}};
        []                  -> {error, {no_record_for_listener, Lsnr}}
    end.

first_desc([{_N, _S, _H, {_, Desc}} | _]) -> Desc.

set_dispatch(Listener, Selectors, Fallback) ->
    ets:insert(?ETS, {port(Listener), Listener, Selectors, Fallback}).

match_request([], _) ->
    no_handler;
match_request([{_Name, Selector, Handler, _Link}|Rest], Req) ->
    case Selector(Req) of
        true  -> Handler;
        false -> match_request(Rest, Req)
    end.

list() ->
    [{Path, Desc, Listener} ||
        {_P, Listener, Selectors, _F} <- ets:tab2list(?ETS),
        {_N, _S, _H, {Path, Desc}} <- Selectors].

listener_by_name(Name) ->
    case [L || {_P, L, S, _F} <- ets:tab2list(?ETS), contains_name(Name, S)] of
        [Listener] -> Listener;
        []         -> exit({not_found, Name})
    end.

contains_name(Name, Selectors) ->
    lists:member(Name, [N || {N, _S, _H, _L} <- Selectors]).

list(Listener) ->
    {ok, {Selectors, _Fallback}} = lookup_dispatch(Listener),
    [{Path, Desc} || {_N, _S, _H, {Path, Desc}} <- Selectors].

%%---------------------------------------------------------------------------

listing_fallback_handler(Listener) ->
    fun(Req) ->
            HTMLPrefix =
                "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">"
                "<head><title>RabbitMQ Web Server</title></head>"
                "<body><h1>RabbitMQ Web Server</h1><p>Contexts available:</p><ul>",
            HTMLSuffix = "</ul></body></html>",
            {ReqPath, _, _} = mochiweb_util:urlsplit_path(Req:get(raw_path)),
            List =
                case list(Listener) of
                    [] ->
                        "<li>No contexts installed</li>";
                    Contexts ->
                        [handler_listing(Path, ReqPath, Desc)
                         || {Path, Desc} <- Contexts]
                end,
            Req:respond({200, [], HTMLPrefix ++ List ++ HTMLSuffix})
    end.

handler_listing(Path, ReqPath, Desc) ->
    io_lib:format(
      "<li><a href=\"~s\">~s</a></li>",
      [rabbit_web_dispatch_util:relativise(ReqPath, "/" ++ Path), Desc]).
