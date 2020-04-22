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
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_upgrade_preparation).

-export([await_online_quorum_plus_one/1, await_online_synchronised_mirrors/1]).

%%
%% API
%%

-define(SAMPLING_INTERVAL, 200).

await_online_quorum_plus_one(Timeout) ->
    Iterations = ceil(Timeout / ?SAMPLING_INTERVAL),
    do_await_safe_online_quorum(Iterations).


await_online_synchronised_mirrors(Timeout) ->
    Iterations = ceil(Timeout / ?SAMPLING_INTERVAL),
    do_await_online_synchronised_mirrors(Iterations).


%%
%% Implementation
%%

do_await_safe_online_quorum(0) ->
    false;
do_await_safe_online_quorum(IterationsLeft) ->
    case rabbit_quorum_queue:list_with_minimum_quorum() of
        []  -> true;
        List when is_list(List) ->
            timer:sleep(?SAMPLING_INTERVAL),
            do_await_safe_online_quorum(IterationsLeft - 1)
    end.


do_await_online_synchronised_mirrors(0) ->
    false;
do_await_online_synchronised_mirrors(IterationsLeft) ->
    case rabbit_amqqueue:list_local_mirrored_classic_without_synchronised_mirrors() of
        []  -> true;
        List when is_list(List) ->
            timer:sleep(?SAMPLING_INTERVAL),
            do_await_online_synchronised_mirrors(IterationsLeft - 1)
    end.
