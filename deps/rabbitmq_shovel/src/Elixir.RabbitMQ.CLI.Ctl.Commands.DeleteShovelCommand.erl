%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteShovelCommand').

-include("rabbit_shovel.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-export([
         usage/0,
         usage_additional/0,
         usage_doc_guides/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         switches/0,
         aliases/0,
         output/2,
         help_section/0,
         description/0
        ]).


%%----------------------------------------------------------------------------
%% Callbacks
%%----------------------------------------------------------------------------
usage() ->
    <<"delete_shovel [--vhost <vhost>] [--force] <name>">>.

usage_additional() ->
    [
      {<<"<name>">>, <<"Shovel to delete">>}
    ].

usage_doc_guides() ->
    [?SHOVEL_GUIDE_URL].

description() ->
    <<"Deletes a Shovel">>.

help_section() ->
    {plugin, shovel}.

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_, _| _], _Opts) ->
    {validation_failure, too_many_args};
validate([_], _Opts) ->
    ok.

merge_defaults(A, Opts) ->
    {A, maps:merge(#{vhost => <<"/">>,
                     force => false}, Opts)}.

banner([Name], #{vhost := VHost}) ->
    erlang:list_to_binary(io_lib:format("Deleting shovel ~ts in vhost ~ts",
                                        [Name, VHost])).

run([Name], #{node := Node, vhost := VHost, force := Force}) ->
    ActingUser = case Force of
                     true -> ?INTERNAL_USER;
                     false -> 'Elixir.RabbitMQ.CLI.Core.Helpers':cli_acting_user()
                 end,

    case rabbit_misc:rpc_call(Node, rabbit_shovel_status, cluster_status_with_nodes, []) of
        {badrpc, _} = Error ->
            Error;
        Xs when is_list(Xs) ->
            ErrMsg = rabbit_misc:format("Shovel with the given name was not found "
                                        "on the target node '~ts' and/or virtual host '~ts'. "
                                        "It may be failing to connect and report its state, will delete its runtime parameter...",
                                        [Node, VHost]),
            case rabbit_shovel_status:find_matching_shovel(VHost, Name, Xs) of
                undefined ->
                    try_force_removing(Node, VHost, Name, ActingUser),
                    {error, rabbit_data_coercion:to_binary(ErrMsg)};
                {{_Name, _VHost}, _Type, {_State, Opts}, _Metrics, _Timestamp} ->
                    delete_shovel(ErrMsg, VHost, Name, ActingUser, Opts, Node);
                {{_Name, _VHost}, _Type, {_State, Opts}, _Timestamp} ->
                    delete_shovel(ErrMsg, VHost, Name, ActingUser, Opts, Node)
            end
    end.

delete_shovel(ErrMsg, VHost, Name, ActingUser, Opts, Node) ->
    {_, HostingNode} = lists:keyfind(node, 1, Opts),
    case rabbit_misc:rpc_call(
        HostingNode, rabbit_shovel_util, delete_shovel, [VHost, Name, ActingUser]) of
        {badrpc, _} = Error ->
            Error;
        {error, not_found} ->
            try_force_removing(HostingNode, VHost, Name, ActingUser),
            {error, rabbit_data_coercion:to_binary(ErrMsg)};
        ok ->
            _ = try_clearing_runtime_parameter(Node, VHost, Name, ActingUser),
            ok
    end.

switches() ->
    [{force, boolean}].

aliases() ->
    [].

output(E, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(E).

try_force_removing(Node, VHost, ShovelName, ActingUser) ->
    %% Deleting the runtime parameter will cause the dynamic Shovel's child tree to be stopped eventually
    %% regardless of the node it is hosted on. MK.
    _ = try_clearing_runtime_parameter(Node, VHost, ShovelName, ActingUser),
    %% These are best effort attempts to delete the Shovel. Clearing the parameter does all the heavy lifting. MK.
    _ = try_stopping_child_process(Node, VHost, ShovelName).

try_clearing_runtime_parameter(Node, VHost, ShovelName, ActingUser) ->
    _ = rabbit_misc:rpc_call(Node, rabbit_runtime_parameters, clear, [VHost, <<"shovel">>, ShovelName, ActingUser]).

try_stopping_child_process(Node, VHost, ShovelName) ->
    _ = rabbit_misc:rpc_call(Node, rabbit_shovel_dyn_worker_sup_sup, stop_child, [{VHost, ShovelName}]).
