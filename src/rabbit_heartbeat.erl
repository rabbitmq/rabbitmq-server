%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_heartbeat).

-export([start_heartbeat_sender/3, start_heartbeat_receiver/3,
         start_heartbeat_fun/1, pause_monitor/1, resume_monitor/1]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([heartbeaters/0]).
-export_type([start_heartbeat_fun/0]).

-type(heartbeaters() :: {rabbit_types:maybe(pid()), rabbit_types:maybe(pid())}).

-type(heartbeat_callback() :: fun (() -> any())).

-type(start_heartbeat_fun() ::
        fun((rabbit_net:socket(), non_neg_integer(), heartbeat_callback(),
             non_neg_integer(), heartbeat_callback()) ->
                   no_return())).

-spec(start_heartbeat_sender/3 ::
        (rabbit_net:socket(), non_neg_integer(), heartbeat_callback()) ->
                                       rabbit_types:ok(pid())).
-spec(start_heartbeat_receiver/3 ::
        (rabbit_net:socket(), non_neg_integer(), heartbeat_callback()) ->
                                         rabbit_types:ok(pid())).

-spec(start_heartbeat_fun/1 ::
        (pid()) -> start_heartbeat_fun()).


-spec(pause_monitor/1 :: (heartbeaters()) -> 'ok').
-spec(resume_monitor/1 :: (heartbeaters()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_heartbeat_sender(Sock, TimeoutSec, SendFun) ->
    %% the 'div 2' is there so that we don't end up waiting for nearly
    %% 2 * TimeoutSec before sending a heartbeat in the boundary case
    %% where the last message was sent just after a heartbeat.
    heartbeater({Sock, TimeoutSec * 1000 div 2, send_oct, 0,
                 fun () -> SendFun(), continue end}).

start_heartbeat_receiver(Sock, TimeoutSec, ReceiveFun) ->
    %% we check for incoming data every interval, and time out after
    %% two checks with no change. As a result we will time out between
    %% 2 and 3 intervals after the last data has been received.
    heartbeater({Sock, TimeoutSec * 1000, recv_oct, 1,
                 fun () -> ReceiveFun(), stop end}).

start_heartbeat_fun(SupPid) ->
    fun (Sock, SendTimeoutSec, SendFun, ReceiveTimeoutSec, ReceiveFun) ->
            {ok, Sender} =
                start_heartbeater(SendTimeoutSec, SupPid, Sock,
                                  SendFun, heartbeat_sender,
                                  start_heartbeat_sender),
            {ok, Receiver} =
                start_heartbeater(ReceiveTimeoutSec, SupPid, Sock,
                                  ReceiveFun, heartbeat_receiver,
                                  start_heartbeat_receiver),
            {Sender, Receiver}
    end.

pause_monitor({_Sender,     none}) -> ok;
pause_monitor({_Sender, Receiver}) -> Receiver ! pause, ok.

resume_monitor({_Sender,     none}) -> ok;
resume_monitor({_Sender, Receiver}) -> Receiver ! resume, ok.

%%----------------------------------------------------------------------------
start_heartbeater(0, _SupPid, _Sock, _TimeoutFun, _Name, _Callback) ->
    {ok, none};
start_heartbeater(TimeoutSec, SupPid, Sock, TimeoutFun, Name, Callback) ->
    supervisor2:start_child(
      SupPid, {Name,
               {rabbit_heartbeat, Callback, [Sock, TimeoutSec, TimeoutFun]},
               transient, ?MAX_WAIT, worker, [rabbit_heartbeat]}).

heartbeater(Params) ->
    {ok, proc_lib:spawn_link(fun () -> heartbeater(Params, {0, 0}) end)}.

heartbeater({Sock, TimeoutMillisec, StatName, Threshold, Handler} = Params,
            {StatVal, SameCount}) ->
    Recurse = fun (V) -> heartbeater(Params, V) end,
    receive
        pause -> receive
                     resume -> Recurse({0, 0});
                     Other  -> exit({unexpected_message, Other})
                 end;
        Other -> exit({unexpected_message, Other})
    after TimeoutMillisec ->
            case rabbit_net:getstat(Sock, [StatName]) of
                {ok, [{StatName, NewStatVal}]} ->
                    if NewStatVal =/= StatVal ->
                            Recurse({NewStatVal, 0});
                       SameCount < Threshold ->
                            Recurse({NewStatVal, SameCount + 1});
                       true ->
                            case Handler() of
                                stop     -> ok;
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
