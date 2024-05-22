%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mirror_queue_mode_all).

-behaviour(rabbit_mirror_queue_mode).

-export([description/0, suggested_queue_nodes/5, validate_policy/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "mirror mode all"},
                    {mfa,         {rabbit_registry, register,
                                   [ha_mode, <<"all">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

description() ->
    [{description, <<"Mirror queue to all nodes">>}].

suggested_queue_nodes(_Params, MNode, _SNodes, _SSNodes, Poss) ->
    {MNode, Poss -- [MNode]}.

validate_policy(none) ->
    ok;
validate_policy(_Params) ->
    {error, "ha-mode=\"all\" does not take parameters", []}.
