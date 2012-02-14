-module(rabbit_ws_sockjs).

-export([init/0]).

%% --------------------------------------------------------------------------
-spec init() -> ok.
init() ->
    Port = get_env(port, 8081),
    SockjsOpts = get_env(sockjs_opts, []) ++ [{logger, fun logger/3}],

    State = {"127.0.0.1", 61613},

    SockjsState = sockjs_handler:init_state(
                    <<"/stomp">>, fun service_stomp/3, State, SockjsOpts),
    VhostRoutes = [{[<<"stomp">>, '...'], sockjs_cowboy_handler, SockjsState}],
    Routes = [{'_',  VhostRoutes}], % any vhost

    rabbit_log:info("rabbit_web_stomp: started on ~s:~w~n",
                    ["0.0.0.0", Port]),
    cowboy:start_listener(http, 100,
                          cowboy_tcp_transport, [{port,     Port}],
                          cowboy_http_protocol, [{dispatch, Routes}]),
    ok.

get_env(Key, Default) ->
    case application:get_env(web_stomp, Key) of
        undefined -> Default;
        V         -> V
    end.


%% Don't log anything to the screen
logger(_Service, Req, _Type) ->
    Req.

%% --------------------------------------------------------------------------

service_stomp(Conn, init, State) ->
    {ok, Pid} = rabbit_ws:start_link({State, Conn}),
    {ok, {Pid, State}};

service_stomp(Conn, {recv, Data}, {Pid, State}) ->
    rabbit_ws:received(Pid, Data),
    ok;

service_stomp(Conn, closed, {Pid, State}) ->
    rabbit_ws:closed(Pid),
    ok.
