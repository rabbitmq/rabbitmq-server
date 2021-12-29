%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ResetStatsDbCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref({'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1}).

-export([
         usage/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         output/2,
         switches/0,
         description/0
        ]).


%%----------------------------------------------------------------------------
%% Callbacks
%%----------------------------------------------------------------------------
usage() ->
     <<"reset_stats_db [--all]">>.

validate(_, _) ->
    ok.

merge_defaults(A, Opts) ->
    {A, maps:merge(#{all => false}, Opts)}.

switches() ->
    [{all, boolean}].

run(_Args, #{node := Node, all := true}) ->
    rabbit_misc:rpc_call(Node, rabbit_mgmt_storage, reset_all, []);
run(_Args, #{node := Node, all := false}) ->
    rabbit_misc:rpc_call(Node, rabbit_mgmt_storage, reset, []).

output(Output, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Output).

banner(_, #{all := true}) ->
    <<"Resetting statistics database in all nodes">>;
banner(_, #{node := Node}) ->
    erlang:iolist_to_binary([<<"Resetting statistics database on node ">>,
                             atom_to_binary(Node, utf8)]).

description() ->
    <<"Resets statistics database. This will remove all metrics data!">>.
