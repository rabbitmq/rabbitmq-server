%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_heartbeat).

-export([start_heartbeat/3,
         start_heartbeat_sender/2,
         start_heartbeat_receiver/2]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_heartbeat/3 :: (pid(), rabbit_net:socket(), non_neg_integer()) ->
                                rabbit_types:maybe({pid(), pid()})).
-spec(start_heartbeat_sender/2 :: (rabbit_net:socket(), non_neg_integer()) ->
                                       rabbit_types:ok(pid())).
-spec(start_heartbeat_receiver/2 :: (rabbit_net:socket(), non_neg_integer()) ->
                                         rabbit_types:ok(pid())).

-endif.

%%----------------------------------------------------------------------------

start_heartbeat(_Sup, _Sock, 0) ->
    none;
start_heartbeat(Sup, Sock, TimeoutSec) ->
    {ok, Sender} =
        supervisor:start_child(
          Sup, {heartbeat_sender,
                {?MODULE, start_heartbeat_sender, [Sock, TimeoutSec]},
                transient, ?MAX_WAIT, worker, [rabbit_heartbeat]}),
    {ok, Receiver} =
        supervisor:start_child(
          Sup, {heartbeat_receiver,
                {?MODULE, start_heartbeat_receiver, [Sock, TimeoutSec]},
                transient, ?MAX_WAIT, worker, [rabbit_heartbeat]}),
    {Sender, Receiver}.

start_heartbeat_sender(Sock, TimeoutSec) ->
    %% the 'div 2' is there so that we don't end up waiting for nearly
    %% 2 * TimeoutSec before sending a heartbeat in the boundary case
    %% where the last message was sent just after a heartbeat.
    Parent = self(),
    {ok, proc_lib:spawn_link(
           fun () -> heartbeater({Sock, TimeoutSec * 1000 div 2,
                                 send_oct, 0,
                                 fun () ->
                                         catch rabbit_net:send(Sock, rabbit_binary_generator:build_heartbeat_frame()),
                                         continue
                                 end}, Parent)
           end)}.

start_heartbeat_receiver(Sock, TimeoutSec) ->
    %% we check for incoming data every interval, and time out after
    %% two checks with no change. As a result we will time out between
    %% 2 and 3 intervals after the last data has been received.
    Parent = self(),
    {ok, proc_lib:spawn_link(
           fun () -> heartbeater({Sock, TimeoutSec * 1000,
                                 recv_oct, 1,
                                 fun () -> exit(timeout) end}, Parent) end)}.

heartbeater(Params, Parent) ->
    heartbeater(Params, erlang:monitor(process, Parent), {0, 0}).

heartbeater({Sock, TimeoutMillisec, StatName, Threshold, Handler} = Params,
            MonitorRef, {StatVal, SameCount}) ->
    receive
        {'DOWN', MonitorRef, process, _Object, _Info} ->
            ok;
        Other ->
            exit({unexpected_message, Other})
    after TimeoutMillisec ->
            case rabbit_net:getstat(Sock, [StatName]) of
                {ok, [{StatName, NewStatVal}]} ->
                    Recurse = fun (V) -> heartbeater(Params, MonitorRef, V) end,
                    if NewStatVal =/= StatVal ->
                            Recurse({NewStatVal, 0});
                       SameCount < Threshold ->
                            Recurse({NewStatVal, SameCount + 1});
                       true ->
                            case Handler() of
                                continue -> Recurse({NewStatVal, 0})
                            end
                    end;
                {error, einval} ->
                    %% the socket is dead, most likely because the
                    %% connection is being shut down -> terminate
                    ok;
                {error, Reason} ->
                    exit({cannot_get_socket_stats, Reason})
            end
    end.
