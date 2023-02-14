-module(test_rabbit_event_handler).

-behaviour(gen_event).

-export([okay/0]).
-export([init/1, handle_call/2, handle_event/2, handle_info/2,
    terminate/2, code_change/3]).

-include_lib("rabbit_common/include/rabbit.hrl").

% an exported callable func, used to allow rabbit_ct_broker_helpers
% to load this code when rpc'd
okay() -> ok.

init([]) ->
    {ok, #{events => []}}.

handle_event(#event{} = Event, #{events := Events} = State) ->
    {ok, State#{events => [Event | Events]}};
handle_event(_, State) ->
    {ok, State}.

handle_call(events,  #{events := Events} = State) ->
    {ok, Events, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
