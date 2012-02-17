-module(rabbit_ws_client_sup).
-behaviour(supervisor2).

-export([start_link/1]).
-export([init/1]).

-define(MAX_WAIT, 16#ffffffff).

%% --------------------------------------------------------------------------

start_link({Configuration, Conn}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    supervisor2:start_child(SupPid,
                            {rabbit_stomp_processor,
                             {rabbit_stomp_processor, start_link,
                              [{rabbit_ws_sockjs_net, Conn},
                               fun (_, _, _, _, _) -> ok end,
                               Configuration]},
                             intrinsic, ?MAX_WAIT, worker,
                             [rabbit_stomp_processor]}).

init(Any) ->
    {ok, {{one_for_all, 0, 1}, []}}.
