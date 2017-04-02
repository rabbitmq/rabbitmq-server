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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_heartbeat).

-export([start/6, start/7]).
-export([start_heartbeat_sender/4, start_heartbeat_receiver/4,
         pause_monitor/1, resume_monitor/1]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-export_type([heartbeaters/0]).

-type heartbeaters() :: {rabbit_types:maybe(pid()), rabbit_types:maybe(pid())}.

-type heartbeat_callback() :: fun (() -> any()).

-spec start
        (pid(), rabbit_net:socket(), non_neg_integer(), heartbeat_callback(),
         non_neg_integer(), heartbeat_callback()) ->
            heartbeaters().

-spec start
        (pid(), rabbit_net:socket(), rabbit_types:proc_name(),
         non_neg_integer(), heartbeat_callback(), non_neg_integer(),
         heartbeat_callback()) ->
            heartbeaters().

-spec start_heartbeat_sender
        (rabbit_net:socket(), non_neg_integer(), heartbeat_callback(),
         rabbit_types:proc_type_and_name()) ->
            rabbit_types:ok(pid()).
-spec start_heartbeat_receiver
        (rabbit_net:socket(), non_neg_integer(), heartbeat_callback(),
         rabbit_types:proc_type_and_name()) ->
            rabbit_types:ok(pid()).

-spec pause_monitor(heartbeaters()) -> 'ok'.
-spec resume_monitor(heartbeaters()) -> 'ok'.

-spec system_code_change(_,_,_,_) -> {'ok',_}.
-spec system_continue(_,_,{_, _}) -> any().
-spec system_terminate(_,_,_,_) -> none().

%%----------------------------------------------------------------------------
start(SupPid, Sock, SendTimeoutSec, SendFun, ReceiveTimeoutSec, ReceiveFun) ->
    start(SupPid, Sock, unknown,
          SendTimeoutSec, SendFun, ReceiveTimeoutSec, ReceiveFun).

start(SupPid, Sock, Identity,
      SendTimeoutSec, SendFun, ReceiveTimeoutSec, ReceiveFun) ->
    {ok, Sender} =
        start_heartbeater(SendTimeoutSec, SupPid, Sock,
                          SendFun, heartbeat_sender,
                          start_heartbeat_sender, Identity),
    {ok, Receiver} =
        start_heartbeater(ReceiveTimeoutSec, SupPid, Sock,
                          ReceiveFun, heartbeat_receiver,
                          start_heartbeat_receiver, Identity),
    {Sender, Receiver}.

start_heartbeat_sender(Sock, TimeoutSec, SendFun, Identity) ->
    %% the 'div 2' is there so that we don't end up waiting for nearly
    %% 2 * TimeoutSec before sending a heartbeat in the boundary case
    %% where the last message was sent just after a heartbeat.
    heartbeater({Sock, TimeoutSec * 1000 div 2, send_oct, 0,
                 fun () -> SendFun(), continue end}, Identity).

start_heartbeat_receiver(Sock, TimeoutSec, ReceiveFun, Identity) ->
    %% we check for incoming data every interval, and time out after
    %% two checks with no change. As a result we will time out between
    %% 2 and 3 intervals after the last data has been received.
    heartbeater({Sock, TimeoutSec * 1000, recv_oct, 1,
                 fun () -> ReceiveFun(), stop end}, Identity).

pause_monitor({_Sender,     none}) -> ok;
pause_monitor({_Sender, Receiver}) -> Receiver ! pause, ok.

resume_monitor({_Sender,     none}) -> ok;
resume_monitor({_Sender, Receiver}) -> Receiver ! resume, ok.

system_continue(_Parent, Deb, {Params, State}) ->
    heartbeater(Params, Deb, State).

system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

%%----------------------------------------------------------------------------
start_heartbeater(0, _SupPid, _Sock, _TimeoutFun, _Name, _Callback,
                  _Identity) ->
    {ok, none};
start_heartbeater(TimeoutSec, SupPid, Sock, TimeoutFun, Name, Callback,
                  Identity) ->
    supervisor2:start_child(
      SupPid, {Name,
               {rabbit_heartbeat, Callback,
                [Sock, TimeoutSec, TimeoutFun, {Name, Identity}]},
               transient, ?WORKER_WAIT, worker, [rabbit_heartbeat]}).

heartbeater(Params, Identity) ->
    Deb = sys:debug_options([]),
    {ok, proc_lib:spawn_link(fun () ->
                                     rabbit_misc:store_proc_name(Identity),
                                     heartbeater(Params, Deb, {0, 0})
                             end)}.

heartbeater({Sock, TimeoutMillisec, StatName, Threshold, Handler} = Params,
            Deb, {StatVal, SameCount} = State) ->
    Recurse = fun (State1) -> heartbeater(Params, Deb, State1) end,
    System  = fun (From, Req) ->
                      sys:handle_system_msg(
                        Req, From, self(), ?MODULE, Deb, {Params, State})
              end,
    receive
        pause ->
            receive
                resume              -> Recurse({0, 0});
                {system, From, Req} -> System(From, Req);
                Other               -> exit({unexpected_message, Other})
            end;
        {system, From, Req} ->
            System(From, Req);
        Other ->
            exit({unexpected_message, Other})
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
