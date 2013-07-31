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

-module(rabbit_web_dispatch_sup).

-behaviour(supervisor).

-define(SUP, ?MODULE).

%% External exports
-export([start_link/0, ensure_listener/1, stop_listener/1]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

ensure_listener(Listener) ->
    case proplists:get_value(port, Listener) of
        undefined ->
            {error, {no_port_given, Listener}};
        _ ->
            Child = {{rabbit_web_dispatch_web, name(Listener)},
                     {mochiweb_http, start, [mochi_options(Listener)]},
                     transient, 5000, worker, dynamic},
            case supervisor:start_child(?SUP, Child) of
                {ok,                      _}  -> new;
                {error, {already_started, _}} -> existing;
                {error, {E, _}}               -> check_error(Listener, E)
            end
    end.

stop_listener(Listener) ->
    Name = name(Listener),
    ok = supervisor:terminate_child(?SUP, {rabbit_web_dispatch_web, Name}),
    ok = supervisor:delete_child(?SUP, {rabbit_web_dispatch_web, Name}).

%% @spec init([[instance()]]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    Registry = {rabbit_web_dispatch_registry,
                {rabbit_web_dispatch_registry, start_link, []},
                transient, 5000, worker, dynamic},
    {ok, {{one_for_one, 10, 10}, [Registry]}}.

%% ----------------------------------------------------------------------

mochi_options(Listener) ->
    [{name, name(Listener)},
     {loop, loopfun(Listener)} |
     easy_ssl(proplists:delete(
                name, proplists:delete(ignore_in_use, Listener)))].

loopfun(Listener) ->
    fun (Req) ->
            case rabbit_web_dispatch_registry:lookup(Listener, Req) of
                no_handler ->
                    Req:not_found();
                {error, Reason} ->
                    Req:respond({500, [], "Registry Error: " ++ Reason});
                {handler, Handler} ->
                    Handler(Req)
            end
    end.

name(Listener) ->
    Port = proplists:get_value(port, Listener),
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(Port)).

easy_ssl(Options) ->
    case {proplists:get_value(ssl, Options),
          proplists:get_value(ssl_opts, Options)} of
        {true, undefined} ->
            {ok, ServerOpts} = application:get_env(rabbit, ssl_options),
            SSLOpts = [{K, V} ||
                          {K, V} <- ServerOpts,
                          not lists:member(K, [verify, fail_if_no_peer_cert])],
            [{ssl_opts, SSLOpts}|Options];
        _ ->
            Options
    end.

check_error(Listener, Error) ->
    Ignore = proplists:get_value(ignore_in_use, Listener, false),
    case {Error, Ignore} of
        {eaddrinuse, true} -> ignore;
        _                  -> exit({could_not_start_listener, Listener, Error})
    end.
