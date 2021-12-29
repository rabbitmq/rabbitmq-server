%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ListAmqp10ConnectionsCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').
-include("rabbit_amqp1_0.hrl").

-ignore_xref([
    {'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1},
    {'Elixir.RabbitMQ.CLI.Core.Helpers', nodes_in_cluster, 1},
    {'Elixir.RabbitMQ.CLI.Ctl.InfoKeys', prepare_info_keys, 1},
    {'Elixir.RabbitMQ.CLI.Ctl.RpcStream', receive_list_items, 7},
    {'Elixir.RabbitMQ.CLI.Ctl.InfoKeys', validate_info_keys, 2},
    {'Elixir.Enum', join, 2}
]).

-export([formatter/0,
         scopes/0,
         switches/0,
         aliases/0,
         usage/0,
         usage_additional/0,
         banner/2,
         validate/2,
         merge_defaults/2,
         run/2,
         output/2,
         help_section/0,
         description/0]).

formatter() -> 'Elixir.RabbitMQ.CLI.Formatters.Table'.
scopes() -> [ctl, diagnostics].
switches() -> [{verbose, boolean}].
aliases() -> [{'V', verbose}].

validate(Args, _) ->
    case 'Elixir.RabbitMQ.CLI.Ctl.InfoKeys':validate_info_keys(Args,
                                                               ?INFO_ITEMS) of
        {ok, _} -> ok;
        Error   -> Error
    end.

merge_defaults([], Opts) ->
    merge_defaults([<<"pid">>], Opts);
merge_defaults(Args, Opts) ->
    {Args, maps:merge(#{verbose => false}, Opts)}.

usage() ->
    <<"list_amqp10_connections [<column> ...]">>.

usage_additional() ->
    Prefix = <<" must be one of ">>,
    InfoItems = 'Elixir.Enum':join(lists:usort(?INFO_ITEMS), <<", ">>),
    [
      {<<"<column>">>, <<Prefix/binary, InfoItems/binary>>}
    ].

description() -> <<"Lists AMQP 1.0 connections on the target node">>.

help_section() ->
    {plugin, 'amqp1.0'}.

run(Args, #{node := NodeName,
            timeout := Timeout,
            verbose := Verbose}) ->
    InfoKeys = case Verbose of
        true  -> ?INFO_ITEMS;
        false -> 'Elixir.RabbitMQ.CLI.Ctl.InfoKeys':prepare_info_keys(Args)
    end,
    Nodes = 'Elixir.RabbitMQ.CLI.Core.Helpers':nodes_in_cluster(NodeName),

    'Elixir.RabbitMQ.CLI.Ctl.RpcStream':receive_list_items(
        NodeName,
        rabbit_amqp1_0,
        emit_connection_info_all,
        [Nodes, InfoKeys],
        Timeout,
        InfoKeys,
        length(Nodes)).

banner(_, _) -> <<"Listing AMQP 1.0 connections ...">>.

output(Result, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Result).
