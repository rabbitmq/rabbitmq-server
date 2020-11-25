%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_mode_nodes).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_mirror_queue_mode).

-export([description/0, suggested_queue_nodes/5, validate_policy/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "mirror mode nodes"},
                    {mfa,         {rabbit_registry, register,
                                   [ha_mode, <<"nodes">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

description() ->
    [{description, <<"Mirror queue to specified nodes">>}].

suggested_queue_nodes(PolicyNodes0, CurrentMaster, _SNodes, SSNodes, NodesRunningRabbitMQ) ->
    PolicyNodes1 = [list_to_atom(binary_to_list(Node)) || Node <- PolicyNodes0],
    %% If the current master is not in the nodes specified, then what we want
    %% to do depends on whether there are any synchronised mirrors. If there
    %% are then we can just kill the current master - the admin has asked for
    %% a migration and we should give it to them. If there are not however
    %% then we must keep the master around so as not to lose messages.

    PolicyNodes = case SSNodes of
                      [] -> lists:usort([CurrentMaster | PolicyNodes1]);
                      _  -> PolicyNodes1
                  end,
    Unavailable = PolicyNodes -- NodesRunningRabbitMQ,
    AvailablePolicyNodes = PolicyNodes -- Unavailable,
    case AvailablePolicyNodes of
        [] -> %% We have never heard of anything? Not much we can do but
              %% keep the master alive.
              {CurrentMaster, []};
        _  -> case lists:member(CurrentMaster, AvailablePolicyNodes) of
                  true  -> {CurrentMaster,
                            AvailablePolicyNodes -- [CurrentMaster]};
                  false -> %% Make sure the new master is synced! In order to
                           %% get here SSNodes must not be empty.
                           SyncPolicyNodes = [Node ||
                                              Node <- AvailablePolicyNodes,
                                              lists:member(Node, SSNodes)],
                           NewMaster = case SyncPolicyNodes of
                                          [Node | _] -> Node;
                                          []         -> erlang:hd(SSNodes)
                                      end,
                           {NewMaster, AvailablePolicyNodes -- [NewMaster]}
              end
    end.

validate_policy([]) ->
    {error, "ha-mode=\"nodes\" list must be non-empty", []};
validate_policy(Nodes) when is_list(Nodes) ->
    case [I || I <- Nodes, not is_binary(I)] of
        []      -> ok;
        Invalid -> {error, "ha-mode=\"nodes\" takes a list of strings, "
                    "~p was not a string", [Invalid]}
    end;
validate_policy(Params) ->
    {error, "ha-mode=\"nodes\" takes a list, ~p given", [Params]}.
