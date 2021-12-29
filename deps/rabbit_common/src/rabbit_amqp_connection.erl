%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp_connection).

-export([amqp_params/2]).

-spec amqp_params(pid(), timeout()) -> [{atom(), term()}].
amqp_params(ConnPid, Timeout) ->
    P = try
            gen_server:call(ConnPid, {info, [amqp_params]}, Timeout)
        catch exit:{noproc, Error} ->
                rabbit_log:debug("file ~p, line ~p - connection process ~p not alive: ~p",
                                 [?FILE, ?LINE, ConnPid, Error]),
            [];
              _:Error ->
                rabbit_log:debug("file ~p, line ~p - failed to get amqp_params from connection process ~p: ~p",
                                 [?FILE, ?LINE, ConnPid, Error]),
            []
        end,
    process_amqp_params_result(P).

process_amqp_params_result({error, {bad_argument, amqp_params}}) ->
    %% Some connection process modules do not handle the {info, [amqp_params]}
    %% message (like rabbit_reader) and throw a bad_argument error
    [];
process_amqp_params_result({ok, AmqpParams}) ->
    AmqpParams;
process_amqp_params_result(AmqpParams) ->
    AmqpParams.
