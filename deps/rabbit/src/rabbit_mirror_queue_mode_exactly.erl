%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_mode_exactly).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_mirror_queue_mode).

-export([description/0, suggested_queue_nodes/5, validate_policy/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "mirror mode exactly"},
                    {mfa,         {rabbit_registry, register,
                                   [ha_mode, <<"exactly">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

description() ->
    [{description, <<"Mirror queue to a specified number of nodes">>}].

%% When we need to add nodes, we randomise our candidate list as a
%% crude form of load-balancing. TODO it would also be nice to
%% randomise the list of ones to remove when we have too many - we
%% would have to take account of synchronisation though.
suggested_queue_nodes(Count, MNode, SNodes, _SSNodes, Poss) ->
    SCount = Count - 1,
    {MNode, case SCount > length(SNodes) of
                true  -> Cand = shuffle((Poss -- [MNode]) -- SNodes),
                         SNodes ++ lists:sublist(Cand, SCount - length(SNodes));
                false -> lists:sublist(SNodes, SCount)
            end}.

shuffle(L) ->
    {_, L1} = lists:unzip(lists:keysort(1, [{rand:uniform(), N} || N <- L])),
    L1.

validate_policy(N) when is_integer(N) andalso N > 0 ->
    ok;
validate_policy(Params) ->
    {error, "ha-mode=\"exactly\" takes an integer, ~p given", [Params]}.
