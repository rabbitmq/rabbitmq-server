%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_mode_all).

-include_lib("rabbit_common/include/rabbit.hrl").

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
