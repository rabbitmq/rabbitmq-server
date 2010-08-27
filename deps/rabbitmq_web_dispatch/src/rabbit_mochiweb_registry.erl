-module(rabbit_mochiweb_registry).

-behaviour(gen_server).

-record(state, {selectors, fallback}).

-export([start_link/0]).
-export([add/3, set_fallback/1, lookup/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add(Selector, Handler, Link) ->
    gen_server:call(?MODULE, {add, Selector, Handler, Link}).

set_fallback(FallbackHandler) ->
    gen_server:call(?MODULE, {set_fallback, FallbackHandler}).

lookup(Req) ->
    gen_server:call(?MODULE, {lookup, Req}).

%% Callback Methods

init([]) ->
    {ok, #state{selectors = [], fallback = fun listing_fallback_handler/1}}.

handle_call({add, Selector, Handler, Link}, _From,
            State = #state{selectors = Selectors}) ->
    UpdatedState =
        State#state{selectors = Selectors ++ [{Selector, Handler, Link}]},
    {reply, ok, UpdatedState};
handle_call({set_fallback, FallbackHandler}, _From,
            State) ->
    {reply, ok, State#state{fallback = FallbackHandler}};
handle_call({lookup, Req}, _From,
            State = #state{ selectors = Selectors, fallback = FallbackHandler }) ->

    case catch match_request(Selectors, Req) of
        {'EXIT', Reason} ->
            {reply, {lookup_failure, Reason}, State};
        no_handler ->
            {reply, {handler, FallbackHandler}, State};
        Handler ->
            {reply, {handler, Handler}, State}
    end;

handle_call(list, _From, State = #state{ selectors = Selectors }) ->
    {reply, [{P, D} || {_S, _H, {P, D}} <- Selectors, D =/= none], State};

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

%%---------------------------------------------------------------------------

%% Internal Methods

match_request([], _) ->
    no_handler;
match_request([{Selector, Handler, _Link}|Rest], Req) ->
    case Selector(Req) of
        true  -> Handler;
        false -> match_request(Rest, Req)
    end.

%%---------------------------------------------------------------------------

listing_fallback_handler(Req) ->
    List = [io_lib:format("<li><a href=\"~s\">~s</a></li>", [Name, Path])
            || {Name, Path} <- gen_server:call(?MODULE, list)],
    Req:respond({200, [],
                 "<h1>RabbitMQ Web Server</h1><ul>" ++ lists:flatten(List)
                 ++ "</ul>"}).
