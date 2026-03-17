%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.RefreshTrustStoreCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-export([
         usage/0,
         usage_doc_guides/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         aliases/0,
         output/2,
         help_section/0,
         description/0
        ]).

%%
%% Command Behavior
%%

usage() ->
    <<"refresh_trust_store">>.

usage_doc_guides() ->
    [<<"https://rabbitmq.com/docs/ssl">>].

description() ->
    <<"Refreshes trust store certificates on a node">>.

help_section() ->
    {plugin, trust_store}.

validate(_, _) ->
    ok.

merge_defaults(A, O) ->
    {A, O}.

banner(_, #{node := Node}) ->
    erlang:iolist_to_binary([<<"Refreshing trust store certificates on node ">>,
                             atom_to_binary(Node, utf8), <<"...">>]).

run(_Args, #{node := Node}) ->
    case rabbit_misc:rpc_call(Node, rabbit_trust_store, refresh, []) of
        {badrpc, _} = Error ->
            Error;
        ok ->
            ok
    end.

aliases() ->
    [].

output(Output, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Output).
