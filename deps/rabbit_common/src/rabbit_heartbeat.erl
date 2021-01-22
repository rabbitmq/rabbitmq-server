%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

-export_type([heartbeat_timeout/0]).
-type heartbeat_timeout() :: non_neg_integer().

-spec start
        (pid(), rabbit_net:socket(), heartbeat_timeout(), heartbeat_callback(),
         heartbeat_timeout(), heartbeat_callback()) ->
            heartbeaters().

-spec start
        (pid(), rabbit_net:socket(), rabbit_types:proc_name(),
         heartbeat_timeout(), heartbeat_callback(), heartbeat_timeout(),
         heartbeat_callback()) ->
            heartbeaters().

-spec start_heartbeat_sender
        (rabbit_net:socket(), heartbeat_timeout(), heartbeat_callback(),
         rabbit_types:proc_type_and_name()) ->
            rabbit_types:ok(pid()).
-spec start_heartbeat_receiver
        (rabbit_net:socket(), heartbeat_timeout(), heartbeat_callback(),
         rabbit_types:proc_type_and_name()) ->
            rabbit_types:ok(pid()).

-spec pause_monitor(heartbeaters()) -> 'ok'.
-spec resume_monitor(heartbeaters()) -> 'ok'.

-spec system_code_change(_,_,_,_) -> {'ok',_}.
-spec system_continue(_,_,{_, _}) -> any().
-spec system_terminate(_,_,_,_) -> no_return().

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
            Deb, {StatVal0, SameCount} = State) ->
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
              OkFun = fun(StatVal1) ->
                              if StatVal0 =:= 0 andalso StatName =:= send_oct ->
                                     % Note: this clause is necessary to ensure the
                                     % first RMQ -> client heartbeat is sent at the
                                     % first interval, instead of waiting the first
                                     % two intervals
                                     {run_handler, {StatVal1, 0}};
                                 StatVal1 =/= StatVal0 ->
                                     {recurse, {StatVal1, 0}};
                                 SameCount < Threshold ->
                                     {recurse, {StatVal1, SameCount +1}};
                                 true ->
                                     {run_handler, {StatVal1, 0}}
                              end
                      end,
              SSResult = get_sock_stats(Sock, StatName, OkFun),
              handle_get_sock_stats(SSResult, Sock, StatName, Recurse, Handler)
    end.

handle_get_sock_stats(stop, _Sock, _StatName, _Recurse, _Handler) ->
    ok;
handle_get_sock_stats({recurse, RecurseArg}, _Sock, _StatName, Recurse, _Handler) ->
    Recurse(RecurseArg);
handle_get_sock_stats({run_handler, {_, SameCount}}, Sock, StatName, Recurse, Handler) ->
    case Handler() of
        stop     -> ok;
        continue ->
            OkFun = fun(StatVal) ->
                            {recurse, {StatVal, SameCount}}
                    end,
            SSResult = get_sock_stats(Sock, StatName, OkFun),
            handle_get_sock_stats(SSResult, Sock, StatName, Recurse, Handler)
    end.

get_sock_stats(Sock, StatName, OkFun) ->
    case rabbit_net:getstat(Sock, [StatName]) of
        {ok, [{StatName, StatVal}]} ->
            OkFun(StatVal);
        {error, einval} ->
            %% the socket is dead, most likely because the
            %% connection is being shut down -> terminate
            stop;
        {error, Reason} ->
            exit({cannot_get_socket_stats, Reason})
    end.
