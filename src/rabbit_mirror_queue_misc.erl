%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_misc).

-export([remove_from_queue/2]).

-include("rabbit.hrl").

remove_from_queue(QueueName, DeadPids) ->
    DeadNodes = [node(DeadPid) || DeadPid <- DeadPids],
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              %% Someone else could have deleted the queue before we
              %% get here.
              case mnesia:read({rabbit_queue, QueueName}) of
                  [] -> {error, not_found};
                  [Q = #amqqueue { pid         = QPid,
                                   mirror_pids = MPids }] ->
                      [QPid1 | MPids1] =
                          [Pid || Pid <- [QPid | MPids],
                                  not lists:member(node(Pid), DeadNodes)],
                      case {{QPid, MPids}, {QPid1, MPids1}} of
                          {Same, Same} ->
                              {ok, QPid};
                          _ ->
                              Q1 = Q #amqqueue { pid         = QPid1,
                                                 mirror_pids = MPids1 },
                              mnesia:write(rabbit_queue, Q1, write),
                              {ok, QPid1}
                      end
              end
      end).
