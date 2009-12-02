%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_exchange_events).
-include("rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-ifdef(use_specs).
-spec(start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}).
-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

%%---------------------------------------------------------------------------

with_exchange(#binding{exchange_name = ExchangeName}, Fun) ->
    case rabbit_exchange:lookup(ExchangeName) of
        {ok, X} ->
            Fun(X);
        not_found ->
            ok
    end.

handle_table_event({write, rabbit_exchange, X = #exchange{type = Type}, _OldRecs, _ActivityId}) ->
    %% Exchange created/recovered.
    ok = Type:init(X);
handle_table_event({delete, rabbit_exchange, {rabbit_exchange, _ExchangeName},
                    [X = #exchange{type = Type}], _ActivityId}) ->
    %% Exchange deleted.
    ok = Type:delete(X);
handle_table_event({write, rabbit_route, #route{binding = B}, _OldRecs, _ActivityId}) ->
    %% New binding.
    ok = with_exchange(B, fun (X = #exchange{type = Type}) -> Type:add_binding(X, B) end);
handle_table_event({delete, rabbit_route, #route{binding = B}, _OldRecs, _ActivityId}) ->
    %% Deleted binding.
    ok = with_exchange(B, fun (X = #exchange{type = Type}) -> Type:delete_binding(X, B) end);
handle_table_event(_Event) ->
    {error, unhandled_table_event}.

%%---------------------------------------------------------------------------

init([]) ->
    mnesia:subscribe({table, rabbit_exchange, detailed}),
    mnesia:subscribe({table, rabbit_route, detailed}),
    {ok, no_state}.

handle_call(Request, _From, State) ->
    {stop, {unhandled_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {unhandled_cast, Request}, State}.

handle_info({mnesia_table_event, Event}, State) ->
    case catch handle_table_event(Event) of
        {'EXIT', Reason} ->
            rabbit_log:error("Exchange event callback failed~n~p~n", [[{event, Event},
                                                                       {reason, Reason}]]);
        ok ->
            ok;
        {error, unhandled_table_event} ->
            rabbit_log:error("Unexpected mnesia_table_event~n~p~n", [Event])
    end,
    {noreply, State};
handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
