%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ClearAuthBackendCacheCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-export([
         usage/0,
         usage_additional/0,
         usage_doc_guides/0,
         flags/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         switches/0,
         aliases/0,
         output/2,
         scopes/0,
         formatter/0,
         help_section/0,
         description/0
        ]).


%%----------------------------------------------------------------------------
%% Callbacks
%%----------------------------------------------------------------------------
scopes() ->
    [vmware, ctl].

switches() ->
    [].

usage() ->
    <<"clear_auth_backend_cache">>.

usage_additional() -> 
    [].

usage_doc_guides() ->
    [].

help_section() ->
    {plugin, rabbitmq_auth_backend_cache}.

description() ->
    <<"Clears rabbitmq_auth_backend_cache plugin's cache on the target node">>.

flags() ->
    [].

validate(_, _) ->
    ok.

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.Table'.

merge_defaults(A, O) ->
    {A, O}.

banner(_, _) ->
    <<"Will clear rabbitmq_auth_backend_cache plugin's cache on the target node...">>.

run(_Args, #{node := Node}) ->
    case rabbit_misc:rpc_call(Node, rabbit_auth_backend_cache, clear_cache_cluster_wide, []) of
        {badrpc, _} = Error ->
            Error;
        Deleted ->
            Deleted
    end.

aliases() ->
    [].

output(Value, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Value).
    