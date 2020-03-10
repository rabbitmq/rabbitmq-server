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
