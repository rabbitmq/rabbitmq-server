-module(mod_http_test_server).

-behaviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

init(_Args) ->
    {ok, no_state}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

handle_call({jsonrpc, <<"test_proc">>, _ModData, [Value]}, _From, State) ->
    {reply, {result, <<"mod_http_test: ", Value/binary>>}, State}.

handle_cast(Request, State) ->
    error_logger:error_msg("Unhandled cast in test_jsonrpc: ~p", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    error_logger:error_msg("Unhandled info in test_jsonrpc: ~p", [Info]),
    {noreply, State}.
    