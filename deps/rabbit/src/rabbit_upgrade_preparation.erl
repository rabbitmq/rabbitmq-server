%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_upgrade_preparation).

-export([await_online_quorum_plus_one/1,
         await_online_synchronised_mirrors/1,
         list_with_minimum_quorum_for_cli/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
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
    EndangeredQueues = lists:append(
                         rabbit_quorum_queue:list_with_minimum_quorum(),
                         rabbit_stream_queue:list_with_minimum_quorum()),
    case EndangeredQueues of
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

-spec list_with_minimum_quorum_for_cli() -> [#{binary() => term()}].
list_with_minimum_quorum_for_cli() ->
    EndangeredQueues = lists:append(
                         rabbit_quorum_queue:list_with_minimum_quorum(),
                         rabbit_stream_queue:list_with_minimum_quorum()),
    [begin
         #resource{name = Name} = QName = amqqueue:get_name(Q),
         #{
           <<"readable_name">> => rabbit_data_coercion:to_binary(rabbit_misc:rs(QName)),
           <<"name">> => Name,
           <<"virtual_host">> => amqqueue:get_vhost(Q),
           <<"type">> => amqqueue:get_type(Q)
          }
     end || Q <- EndangeredQueues].
