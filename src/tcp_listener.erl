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

-module(tcp_listener).

-behaviour(gen_server).

-export([start_link/9]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {sock, on_startup, on_shutdown, label}).

-include_lib("ssl/src/ssl_int.hrl").

%%--------------------------------------------------------------------

start_link(IPAddress, Port, SocketOpts,
           ConcurrentAcceptorCount, AcceptorSup,
           OnStartup, OnShutdown, Label, Kind) ->
    gen_server:start_link(
      ?MODULE, {IPAddress, Port, SocketOpts,
                ConcurrentAcceptorCount, AcceptorSup,
                OnStartup, OnShutdown, Label, Kind}, []).

%%--------------------------------------------------------------------

init({IPAddress, Port, SocketOpts,
      ConcurrentAcceptorCount, AcceptorSup,
      {M,F,A} = OnStartup, OnShutdown, Label, Kind}) ->
    process_flag(trap_exit, true),
    Listen = case Kind of
                 tcp -> gen_tcp:listen(Port, SocketOpts ++ [{ip, IPAddress},
                                                            {active, false}]);
                 {ssl, SslOpts} ->
                     ssl:listen(Port,
                                SocketOpts
                                ++ [{ip, IPAddress}, {active, false}]
                                ++ SslOpts);
                 E -> {stop, {unknown_listen_kind, IPAddress, Port, E}}
             end,
    case Listen of
        {ok, LSock} ->
            lists:foreach(fun (_) ->
                                  {ok, _APid} = supervisor:start_child(
                                                  AcceptorSup, [LSock])
                          end,
                          lists:duplicate(ConcurrentAcceptorCount, dummy)),
            LSockReal = rabbit_misc:get_real_sock(LSock),
            {ok, {LIPAddress, LPort}} = inet:sockname(LSockReal),
            error_logger:info_msg("started ~s on ~s:~p~n",
                                  [Label, inet_parse:ntoa(LIPAddress), LPort]),
            apply(M, F, A ++ [IPAddress, Port]),
            {ok, #state{sock = LSockReal,
                        on_startup = OnStartup, on_shutdown = OnShutdown,
                        label = Label}};
        {error, Reason} ->
            error_logger:error_msg(
              "failed to start ~s on ~s:~p - ~p~n",
              [Label, inet_parse:ntoa(IPAddress), Port, Reason]),
            {stop, {cannot_listen, IPAddress, Port, Reason}}
    end.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{sock=LSock, on_shutdown = {M,F,A}, label=Label}) ->
    {ok, {IPAddress, Port}} = inet:sockname(LSock),
    gen_tcp:close(LSock),
    error_logger:info_msg("stopped ~s on ~s:~p~n",
                          [Label, inet_parse:ntoa(IPAddress), Port]),
    apply(M, F, A ++ [IPAddress, Port]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
