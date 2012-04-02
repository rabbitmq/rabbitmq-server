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
%% Copyright (c) 2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_ws_client_sup).
-behaviour(supervisor2).

-export([start_client/1, start_processor/1]).
-export([init/1]).

-include_lib("amqp_client/include/amqp_client.hrl").

%% --------------------------------------------------------------------------

start_client({Conn}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    supervisor2:start_child(SupPid,
                            {rabbit_ws_client,
                             {rabbit_ws_client, start_link, [{SupPid, Conn}]},
                             intrinsic, ?MAX_WAIT, worker,
                             [rabbit_ws_client]}).

start_processor({SupPid, Conn, StompConfig}) ->
    SendFun = fun (_Sync, Data) ->
                      Conn:send(Data),
                      ok
              end,
    Info = Conn:info(),
    {PeerAddr, PeerPort} = proplists:get_value(peername, Info),
    {SockAddr, SockPort} = proplists:get_value(sockname, Info),

    AdapterInfo = #adapter_info{protocol        = {'WEB-STOMP', 0},
                                address         = SockAddr,
                                port            = SockPort,
                                peer_address    = PeerAddr,
                                peer_port       = PeerPort,
                                additional_info = [{ssl, false}]},

    Args = [SendFun, AdapterInfo, fun (_, _, _, _) -> ok end,
            none, StompConfig],

    supervisor2:start_child(SupPid,
                            {rabbit_stomp_processor,
                             {rabbit_stomp_processor, start_link, [Args]},
                             intrinsic, ?MAX_WAIT, worker,
                             [rabbit_stomp_processor]}).

init(_Any) ->
    {ok, {{one_for_all, 0, 1}, []}}.
