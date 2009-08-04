-module(mod_http_registry).

-behaviour(gen_server).

-record(state, {selectors}).

-export([start_link/0]).
-export([add/2, lookup/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add(Selector, Handler) ->
    gen_server:call(?MODULE, {add, Selector, Handler}).

lookup(Req) ->
    gen_server:call(?MODULE, {lookup, Req}).
    
%% Callback Methods

init([]) ->
    {ok, #state{selectors = []}}.

handle_call({add, Selector, Handler}, _From, 
            State = #state{selectors = Selectors}) ->
    UpdatedState =  State#state{selectors = Selectors ++ [{Selector, Handler}]},
    {reply, ok, UpdatedState};
handle_call({lookup, Req}, _From,
            State = #state{ selectors = Selectors }) ->
    case catch match_request(Selectors, Req) of
        {'EXIT', Reason} ->
            {reply, {lookup_failure, Reason}, State};
        no_handler ->
            {reply, no_handler, State};
        Handler ->
            {reply, {handler, Handler}, State}
    end;
handle_call(Req, _From, State) ->
    error_logger:format("Unexpected call to ~p: ~p~n", [?MODULE, Req]),
    {reply, unknown_request, State}.
    
handle_cast(_, State) -> 
	{noreply, State}.

handle_info(_, State) ->
	{noreply, State}.

terminate(_, _) ->
	ok.

code_change(_, State, _) ->
	{ok, State}.

%% Internal Methods
        
match_request([], _) ->
    no_handler;
match_request([{Selector, Handler}|Rest], Req) ->
    case Selector(Req) of
        true  -> Handler;
        false -> match_request(Rest, Req)
    end.
