%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_shovel_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     amqp_encoded_data_list,
     amqp_encoded_amqp_value
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

amqp_encoded_data_list(_Config) ->
    meck:new(rabbit_shovel_behaviour, [passthrough]),
    meck:expect(rabbit_shovel_behaviour, forward,
                fun (_, Msg, S) ->
                        ?assert(mc:is(Msg)),
                        S
                end),
    %% fake some shovel state
    State = #{source => #{},
              dest => #{module => rabbit_amqp10_shovel},
              ack_mode => no_ack},
    Body = [
            #'v1_0.data'{content = <<"one">>},
            #'v1_0.data'{content = <<"two">>}
           ],
    [_Transfer | Sections] = amqp10_msg:to_amqp_records(amqp10_msg:new(<<"55">>, Body)),
    Bin = iolist_to_binary([amqp10_framing:encode_bin(S) || S <- Sections]),
    Msg = amqp10_raw_msg:new(true, 55, Bin),
    rabbit_amqp10_shovel:handle_source({amqp10_msg, linkref, Msg}, State),

    ?assert(meck:validate(rabbit_shovel_behaviour)),
    ok.

amqp_encoded_amqp_value(_Config) ->
    meck:new(rabbit_shovel_behaviour, [passthrough]),
    meck:expect(rabbit_shovel_behaviour, forward,
                fun (_, Msg, S) ->
                        ?assert(mc:is(Msg)),
                        S
                end),
    %% fake some shovel state
    State = #{source => #{},
              dest => #{module => rabbit_amqp10_shovel},
              ack_mode => no_ack},

    Body = #'v1_0.amqp_value'{content = {utf8, <<"hi">>}},
    [_Transfer | Sections] = amqp10_msg:to_amqp_records(amqp10_msg:new(<<"55">>, Body)),
    Bin = iolist_to_binary([amqp10_framing:encode_bin(S) || S <- Sections]),
    Msg = amqp10_raw_msg:new(true, 55, Bin),
    rabbit_amqp10_shovel:handle_source({amqp10_msg, linkref, Msg}, State),

    ?assert(meck:validate(rabbit_shovel_behaviour)),
    ok.

%% Utility
