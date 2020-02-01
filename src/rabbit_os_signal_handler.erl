-module(rabbit_os_signal_handler).

-behaviour(gen_event).

-export([start_link/0, init/1,
         handle_event/2, handle_call/2, handle_info/2,
         terminate/2]).

%%
%% API
%%

start_link() ->
    rabbit_log:info("Swapping OS signal event handler (erl_signal_server) for our own"),
    %% delete any previous incarnations, otherwise we would be accumulating
    %% handlers
    _ = gen_event:delete_handler(erl_signal_server, ?MODULE, []),
    %% swap the standard OTP signal handler if there is one
    ok = gen_event:swap_sup_handler(
        erl_signal_server,
        %% what to swap
        {erl_signal_handler, []},
        %% new event handler
        {?MODULE, []}),
    gen_event:start_link({local, ?MODULE}).

init(_) ->
    {ok, #{}}.

handle_event(sigterm, State) ->
    rabbit_log:info("Received a SIGTERM, will shut down gracefully"),
    rabbit:stop_and_halt(),
    {ok, State};
handle_event(sigquit, State) ->
    rabbit_log:info("Received a SIGQUIT, will shut down gracefully"),
    rabbit:stop_and_halt(),
    {ok, State};
handle_event(sigusr1, State) ->
    rabbit_log:info("Received a SIGUSR1, ignoring it"),
    {ok, State};
handle_event(sigusr2, State) ->
    rabbit_log:info("Received a SIGUSR2, ignoring it"),
    {ok, State};
%% note: SIGHUP can/will be handled by shells and process managers
handle_event(sighup, State) ->
    rabbit_log:info("Received a SIGHUP, ignoring it"),
    {ok, State};
handle_event(Msg, S) ->
    %% delegate all unknown events to the default OTP signal handler
    erl_signal_handler:handle_event(Msg, S),
    {ok, S}.

handle_info(_, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

terminate(_Args, _State) ->
    ok.
