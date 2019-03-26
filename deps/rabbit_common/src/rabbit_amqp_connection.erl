%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_amqp_connection).

-export([amqp_params/2]).

-spec amqp_params(pid(), timeout()) -> [{atom(), term()}].
amqp_params(ConnPid, Timeout) ->
    P = try
            gen_server:call(ConnPid, {info, [amqp_params]}, Timeout)
        catch exit:{noproc, Error} ->
                rabbit_log:debug("file ~p, line ~p - connection process ~p not alive: ~p~n",
                                 [?FILE, ?LINE, ConnPid, Error]),
            [];
              _:Error ->
                rabbit_log:debug("file ~p, line ~p - failed to get amqp_params from connection process ~p: ~p~n",
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
