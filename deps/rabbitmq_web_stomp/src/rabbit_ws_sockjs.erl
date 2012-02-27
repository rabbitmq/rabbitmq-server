-module(rabbit_ws_sockjs).

-export([init/0]).

-include_lib("rabbitmq_stomp/include/rabbit_stomp.hrl").


-record(state, {processor, parse_state}).

%% --------------------------------------------------------------------------

-spec init() -> ok.
init() ->
    Port = get_env(port, 55674),
    SockjsOpts = get_env(sockjs_opts, []) ++ [{logger, fun logger/3}],

    State = #state{},

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
    case application:get_env(rabbitmq_web_stomp, Key) of
        undefined -> Default;
        {ok, V}   -> V
    end.


%% Don't print sockjs logs
logger(_Service, Req, _Type) ->
    Req.

%% --------------------------------------------------------------------------
process_received_bytes(Bytes, Processor, ParseState) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {ok, Frame, Rest} ->
            rabbit_stomp_processor:process_frame(Processor, Frame),
            ParseState1 = rabbit_stomp_frame:initial_state(),
            process_received_bytes(Rest, Processor, ParseState1);
        {more, ParseState1, _Length} ->
            ParseState1
    end.


service_stomp(Conn, init, State) ->
    StompConfig = #stomp_configuration{implicit_connect = false},

    {ok, Processor} = rabbit_ws_sup:start_processor(
                           {StompConfig, Conn}),

    Fun = fun () ->
                  ok = file_handle_cache:obtain(),
                  process_flag(trap_exit, true),
                  link(Processor),
                  receive
                      {'EXIT', Processor, _Reason} ->
                          ok = file_handle_cache:release(),
                          sockjs:close(Conn)
                  end
          end,
    spawn(Fun),
    {ok, State#state{processor   = Processor,
                     parse_state = rabbit_stomp_frame:initial_state()}};


service_stomp(_Conn, {recv, Data}, State = #state{processor   = Processor,
                                                  parse_state = ParseState}) ->
    ParseState1 = process_received_bytes(Data, Processor, ParseState),
    {ok, State#state{parse_state = ParseState1}};


service_stomp(_Conn, closed, #state{processor = Processor}) ->
    rabbit_stomp_processor:flush_and_die(Processor),
    ok.
