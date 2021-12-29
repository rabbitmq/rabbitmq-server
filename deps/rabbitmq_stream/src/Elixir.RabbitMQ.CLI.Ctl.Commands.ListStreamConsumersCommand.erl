%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumersCommand').

-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref([{'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1},
              {'Elixir.RabbitMQ.CLI.Core.Helpers', nodes_in_cluster, 1},
              {'Elixir.RabbitMQ.CLI.Ctl.InfoKeys', prepare_info_keys, 1},
              {'Elixir.RabbitMQ.CLI.Ctl.RpcStream', receive_list_items, 7},
              {'Elixir.RabbitMQ.CLI.Ctl.InfoKeys', validate_info_keys, 2},
              {'Elixir.Enum', join, 2}]).

-export([formatter/0,
         scopes/0,
         switches/0,
         aliases/0,
         usage/0,
         usage_additional/0,
         usage_doc_guides/0,
         banner/2,
         validate/2,
         merge_defaults/2,
         run/2,
         output/2,
         description/0,
         help_section/0]).

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.PrettyTable'.

scopes() ->
    [ctl, diagnostics, streams].

switches() ->
    [{verbose, boolean}].

aliases() ->
    [{'V', verbose}].

description() ->
    <<"Lists all stream consumers for a vhost">>.

help_section() ->
    {plugin, stream}.

validate(Args, _) ->
    case 'Elixir.RabbitMQ.CLI.Ctl.InfoKeys':validate_info_keys(Args,
                                                               ?CONSUMER_INFO_ITEMS)
    of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

merge_defaults([], Opts) ->
    merge_defaults([rabbit_data_coercion:to_binary(Item)
                    || Item <- ?CONSUMER_INFO_ITEMS],
                   Opts);
merge_defaults(Args, Opts) ->
    {Args, maps:merge(#{verbose => false, vhost => <<"/">>}, Opts)}.

usage() ->
    <<"list_stream_consumers [--vhost <vhost>] [<column> "
      "...]">>.

usage_additional() ->
    Prefix = <<" must be one of ">>,
    InfoItems =
        'Elixir.Enum':join(
            lists:usort(?CONSUMER_INFO_ITEMS), <<", ">>),
    [{<<"<column>">>, <<Prefix/binary, InfoItems/binary>>}].

usage_doc_guides() ->
    [?STREAM_GUIDE_URL].

run(Args,
    #{node := NodeName,
      vhost := VHost,
      timeout := Timeout,
      verbose := Verbose}) ->
    InfoKeys =
        case Verbose of
            true ->
                ?CONSUMER_INFO_ITEMS;
            false ->
                'Elixir.RabbitMQ.CLI.Ctl.InfoKeys':prepare_info_keys(Args)
        end,
    Nodes = 'Elixir.RabbitMQ.CLI.Core.Helpers':nodes_in_cluster(NodeName),

    'Elixir.RabbitMQ.CLI.Ctl.RpcStream':receive_list_items(NodeName,
                                                           rabbit_stream,
                                                           emit_consumer_info_all,
                                                           [Nodes, VHost,
                                                            InfoKeys],
                                                           Timeout,
                                                           InfoKeys,
                                                           length(Nodes)).

banner(_, _) ->
    <<"Listing stream consumers ...">>.

output(Result, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Result).
