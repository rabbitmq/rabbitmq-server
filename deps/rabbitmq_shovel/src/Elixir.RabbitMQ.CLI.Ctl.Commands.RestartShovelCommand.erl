%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.RestartShovelCommand').

-include("rabbit_shovel.hrl").

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-ignore_xref({'Elixir.RabbitMQ.CLI.DefaultOutput', output, 1}).

-export([
         usage/0,
         usage_additional/0,
         usage_doc_guides/0,
         flags/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         aliases/0,
         output/2,
         help_section/0,
         description/0
        ]).


%%----------------------------------------------------------------------------
%% Callbacks
%%----------------------------------------------------------------------------

flags() ->
    [].

aliases() ->
    [].

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_], _Opts) ->
    ok;
validate(_, _Opts) ->
    {validation_failure, too_many_args}.

merge_defaults(A, Opts) ->
    {A, maps:merge(#{vhost => <<"/">>}, Opts)}.

banner([Name], #{node := Node, vhost := VHost}) ->
    erlang:iolist_to_binary([<<"Restarting dynamic Shovel ">>, Name, <<" in virtual host ">>, VHost,
                             << " on node ">>, atom_to_binary(Node, utf8)]).

run([Name], #{node := Node, vhost := VHost}) ->
    case rabbit_misc:rpc_call(Node, rabbit_shovel_status, cluster_status_with_nodes, []) of
        {badrpc, _} = Error ->
            Error;
        Xs when is_list(Xs) ->
            ErrMsg = rabbit_misc:format("Shovel with the given name was not found "
                                        "on the target node '~s' and / or virtual host '~s'",
                                        [Node, VHost]),
            case rabbit_shovel_status:find_matching_shovel(VHost, Name, Xs) of
                undefined ->
                    {error, rabbit_data_coercion:to_binary(ErrMsg)};
                Match ->
                    {{_Name, _VHost}, _Type, {_State, Opts}, _Timestamp} = Match,
                    {_, HostingNode} = lists:keyfind(node, 1, Opts),
                    case rabbit_misc:rpc_call(
                        HostingNode, rabbit_shovel_util, restart_shovel, [VHost, Name]) of
                        {badrpc, _} = Error ->
                            Error;
                        {error, not_found} ->
                            {error, rabbit_data_coercion:to_binary(ErrMsg)};
                        ok -> ok
                    end
            end
    end.

output(Output, _Opts) ->
    'Elixir.RabbitMQ.CLI.DefaultOutput':output(Output).

usage() ->
     <<"restart_shovel <name>">>.

usage_additional() ->
   [
      {<<"<name>">>, <<"name of the Shovel to restart">>}
   ].

usage_doc_guides() ->
    [?SHOVEL_GUIDE_URL].

help_section() ->
   {plugin, shovel}.

description() ->
   <<"Restarts a dynamic Shovel">>.
