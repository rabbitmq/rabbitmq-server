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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2012-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_ws_client_sup).
-behaviour(supervisor2).

-export([start_client/1]).
-export([init/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_stomp/include/rabbit_stomp.hrl").

%% --------------------------------------------------------------------------

start_client({Conn}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, Processor} = supervisor2:start_child(SupPid, proc_spec(Conn)),
    {ok, Client} = supervisor2:start_child(
                     SupPid, client_spec(Processor, Conn)),
    {ok, SupPid, Client}.

proc_spec(Conn) ->
    StompConfig = #stomp_configuration{implicit_connect = false},

    SendFun = fun (_Sync, Data) ->
                      Conn:send(Data),
                      ok
              end,
    Info = Conn:info(),
    {PeerAddr, PeerPort} = proplists:get_value(peername, Info),
    {SockAddr, SockPort} = proplists:get_value(sockname, Info),
    Name = rabbit_misc:format("~s:~b -> ~s:~b",
                              [rabbit_misc:ntoa(PeerAddr), PeerPort,
                               rabbit_misc:ntoa(SockAddr), SockPort]),
    AdapterInfo = #amqp_adapter_info{protocol        = {'Web STOMP', 0},
                                     host            = SockAddr,
                                     port            = SockPort,
                                     peer_host       = PeerAddr,
                                     peer_port       = PeerPort,
                                     name            = list_to_binary(Name),
                                     additional_info = [{ssl, false}]},

    {rabbit_stomp_processor,
     {rabbit_stomp_processor, start_link,
      [[SendFun, AdapterInfo, fun (_, _, _, _) -> ok end, none, StompConfig]]},
     intrinsic, ?MAX_WAIT, worker,
     [rabbit_stomp_processor]}.

client_spec(Processor, Conn) ->
    {rabbit_ws_client, {rabbit_ws_client, start_link, [{Processor, Conn}]},
     intrinsic, ?MAX_WAIT, worker, [rabbit_ws_client]}.

init(_Any) ->
    {ok, {{one_for_all, 0, 1}, []}}.
