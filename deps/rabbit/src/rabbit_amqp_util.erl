%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_util).
-include("rabbit_amqp.hrl").

<<<<<<< HEAD
-export([protocol_error/3]).
=======
-export([protocol_error/3,
         capabilities/1]).
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)

-spec protocol_error(term(), io:format(), [term()]) ->
    no_return().
protocol_error(Condition, Msg, Args) ->
    Description = unicode:characters_to_binary(lists:flatten(io_lib:format(Msg, Args))),
    Reason = #'v1_0.error'{condition = Condition,
                           description = {utf8, Description}},
    exit(Reason).
<<<<<<< HEAD
=======

-spec capabilities([binary()]) ->
    undefined | {array, symbol, [{symbol, binary()}]}.
capabilities([]) ->
    undefined;
capabilities(Capabilities) ->
    Caps = [{symbol, C} || C <- Capabilities],
    {array, symbol, Caps}.
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)
