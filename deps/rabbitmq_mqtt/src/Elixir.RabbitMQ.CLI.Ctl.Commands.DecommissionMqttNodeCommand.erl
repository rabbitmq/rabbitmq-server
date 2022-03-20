%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.DecommissionMqttNodeCommand').

-include("rabbit_mqtt.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref({'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1}).

-export([scopes/0,
         switches/0,
         aliases/0,
         usage/0,
         usage_doc_guides/0,
         banner/2,
         validate/2,
         merge_defaults/2,
         run/2,
         output/2,
         description/0,
         help_section/0]).

scopes() -> [ctl].
switches() -> [].
aliases() -> [].

description() -> <<"Removes cluster member and permanently deletes its cluster-wide MQTT state">>.

help_section() ->
    {plugin, mqtt}.

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_, _ | _], _Opts) ->
    {validation_failure, too_many_args};
validate([_], _) ->
    ok.

merge_defaults(Args, Opts) ->
    {Args, Opts}.

usage() ->
    <<"decommission_mqtt_node <node>">>.

usage_doc_guides() ->
    [?MQTT_GUIDE_URL].

run([Node], #{node := NodeName,
              timeout := Timeout}) ->
    case rabbit_misc:rpc_call(NodeName, rabbit_mqtt_collector, leave, [Node], Timeout) of
        {badrpc, _} = Error ->
            Error;
        nodedown ->
            {ok, list_to_binary(io_lib:format("Node ~s is down but has been successfully removed"
                                         " from the cluster", [Node]))};
        Result ->
            %% 'ok' or 'timeout'
            %% TODO: Ra will timeout if the node is not a cluster member - should this be fixed??
            Result
    end.

banner([Node], _) -> list_to_binary(io_lib:format("Removing node ~s from the list of MQTT nodes...", [Node])).

output(Result, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Result).
