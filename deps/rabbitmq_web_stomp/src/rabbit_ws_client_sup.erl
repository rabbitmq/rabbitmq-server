-module(rabbit_ws_client_sup).
-behaviour(supervisor2).

-export([start_link/1]).
-export([init/1]).

-include_lib("amqp_client/include/amqp_client.hrl").

%% --------------------------------------------------------------------------

start_link({Configuration, Conn}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    SendFrame = fun (_Sync, Data) ->
                        sockjs:send(Data, Conn),
                        ok
                end,
    AdapterInfo = #adapter_info{address         = {1,2,3,4},
                                port            = 1,
                                peer_address    = {2,3,4,5},
                                peer_port       = 2,
                                additional_info = [{ssl, false}]},

    supervisor2:start_child(SupPid,
                            {rabbit_stomp_processor,
                             {rabbit_stomp_processor, start_link,
                              [SendFrame,
                               AdapterInfo,
                               fun (_, _, _, _) -> ok end,
                               Configuration]},
                             intrinsic, ?MAX_WAIT, worker,
                             [rabbit_stomp_processor]}).

init(_Any) ->
    {ok, {{one_for_all, 0, 1}, []}}.
