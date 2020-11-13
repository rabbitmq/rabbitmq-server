%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ResetStatsDbCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

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
