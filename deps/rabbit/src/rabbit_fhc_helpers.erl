%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_fhc_helpers).

-export([clear_read_cache/0]).

-include("amqqueue.hrl").

clear_read_cache() ->
    case application:get_env(rabbit, fhc_read_buffering) of
        {ok, true} ->
            file_handle_cache:clear_read_cache(),
            clear_vhost_read_cache(rabbit_vhost:list_names());
        _ -> %% undefined or {ok, false}
            ok
    end.

clear_vhost_read_cache([]) ->
    ok;
clear_vhost_read_cache([VHost | Rest]) ->
    clear_queue_read_cache(rabbit_amqqueue:list(VHost)),
    clear_vhost_read_cache(Rest).

clear_queue_read_cache([]) ->
    ok;
clear_queue_read_cache([Q | Rest]) when ?is_amqqueue(Q) ->
    MPid = amqqueue:get_pid(Q),
    SPids = amqqueue:get_slave_pids(Q),
    %% Limit the action to the current node.
    Pids = [P || P <- [MPid | SPids], node(P) =:= node()],
    %% This function is executed in the context of the backing queue
    %% process because the read buffer is stored in the process
    %% dictionary.
    Fun = fun(_, State) ->
                  _ = file_handle_cache:clear_process_read_cache(),
                  State
          end,
    [rabbit_amqqueue:run_backing_queue(Pid, rabbit_variable_queue, Fun)
     || Pid <- Pids],
    clear_queue_read_cache(Rest).
