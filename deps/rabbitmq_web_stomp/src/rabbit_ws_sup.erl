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

-module(rabbit_ws_sup).
-behaviour(supervisor).

-export([init/1, start_link/0, start_processor/1]).

-include_lib("amqp_client/include/amqp_client.hrl").

%%----------------------------------------------------------------------------

-spec start_link() -> ignore | {'ok', pid()} | {'error', any()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Spec = {rabbit_stomp_processor,
            {rabbit_stomp_processor, start_link, []},
            temporary, ?MAX_WAIT, worker,
            [rabbit_stomp_processor]},
    {ok, {{simple_one_for_one, 5, 10},
          [Spec]}}.


start_processor({Configuration, Conn}) ->
    %% Boilerplate to get rabbit_stomp_processor started with SockJS.
    SendFun = fun (_Sync, Data) ->
                      sockjs:send(Data, Conn),
                      ok
              end,
    Info = sockjs:info(Conn),
    {PeerAddr, PeerPort} = proplists:get_value(peername, Info),
    {SockAddr, SockPort} = proplists:get_value(sockname, Info),

    AdapterInfo = #adapter_info{protocol        = {'WEB-STOMP', 0},
                                address         = SockAddr,
                                port            = SockPort,
                                peer_address    = PeerAddr,
                                peer_port       = PeerPort,
                                additional_info = [{ssl, false}]},

    Args = [SendFun, AdapterInfo, fun (_, _, _, _) -> ok end,
            none, Configuration],

    supervisor:start_child(rabbit_ws_sup, [Args]).
