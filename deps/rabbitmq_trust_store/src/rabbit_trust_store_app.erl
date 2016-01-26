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

-module(rabbit_trust_store_app).
-behaviour(application).
-export([change_SSL_options/0]).
-export([start/2, stop/1]).


-rabbit_boot_step({rabbit_trust_store, [
    {description, "Change necessary SSL options."},
    {mfa, {?MODULE, change_SSL_options, []}},
    %% {cleanup, ...}, {requires, ...},
    {enables, networking}]}).

change_SSL_options() ->
    case application:get_env(rabbit, ssl_options) of
        undefined ->
            edit([]);
        {ok, Before} when is_list(Before) ->
            After = edit(Before),
            ok = application:set_env(rabbit,
                ssl_options, After, [{persistent, true}])
    end.

start(normal, _) ->
    case ready('SSL') of
        no  -> {error, information('SSL')};
        yes -> ready('whitelist directory')
    end.

stop(_) ->
    ok.


%% Ancillary

edit(Options) ->
    %% Only enter those options neccessary for this application.
    case lists:keymember(verify_fun, 1, Options) of
        true ->
            {error, information(edit)};
        false ->
            lists:keymerge(1, required_options(),
                [{verify_fun, {delegate(procedure), []}}|Options])
    end.

information(edit) ->
    <<"The prerequisite SSL option, `verify_fun`, is already set.">>;
information('SSL') ->
    <<"The SSL `verify_fun` procedure must interface with this application.">>;
information(whitelist) ->
    <<"The Trust-Store must be configured with a valid directory for whitelisted certificates.">>.

delegate(procedure) ->
    M = delegate(module), fun M:interface/3;
delegate(module) ->
    rabbit_trust_store.

required_options() ->
    [{verify, verify_peer}, {fail_if_no_peer_cert, true}].

ready('SSL') ->
    {ok, Options} = application:get_env(rabbit, ssl_options),
    case lists:keyfind(verify_fun, 1, Options) of
        false ->
            no;
        {_, {Interface, []}} ->
            here(Interface)
    end;
ready('whitelist directory') ->
    V = whitelist_path(),
    case filelib:ensure_dir(V) of
        {error, _} -> {error, information(whitelist)};
        ok         -> rabbit_trust_store_sup:start_link()
    end.

here(Procedure) ->
    M = delegate(module),
    case erlang:fun_info(Procedure, module) of
        {module, M} -> yes;
        {module, _} -> no
    end.

whitelist_path() ->
    case application:get_env(rabbitmq_trust_store, whitelist) of
        undefined -> default_directory();
        {ok, V}   -> V
    end.

default_directory() ->
    filename:join([os:getenv("HOME"), "rabbit", "whitelist"]) ++ "/".
