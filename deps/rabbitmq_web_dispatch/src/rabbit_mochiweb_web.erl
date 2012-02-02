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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mochiweb_web).

-export([start/1, stop/1]).

%% ----------------------------------------------------------------------
%% HTTPish API
%% ----------------------------------------------------------------------

start({ListenerSpec, Env}) ->
    case proplists:get_value(port, Env, undefined) of
        undefined ->
            {error, {no_port_given, ListenerSpec, Env}};
        _ ->
            Loop = loopfun(ListenerSpec),
            OtherOptions = proplists:delete(name, Env),
            Name = name(ListenerSpec),
            mochiweb_http:start(
              [{name, Name}, {loop, Loop} | easy_ssl(OtherOptions)])
    end.

stop(Listener) ->
    mochiweb_http:stop(name(Listener)).

loopfun(Listener) ->
    fun (Req) ->
            case rabbit_mochiweb_registry:lookup(Listener, Req) of
                no_handler ->
                    Req:not_found();
                {lookup_failure, Reason} ->
                    Req:respond({500, [], "Registry Error: " ++ Reason});
                {handler, Handler} ->
                    Handler(Req)
            end
    end.

name(Listener) ->
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Listener)).

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
