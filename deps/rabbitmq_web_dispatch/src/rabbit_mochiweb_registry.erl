-module(rabbit_mochiweb_registry).

-behaviour(gen_server).

-export([start_link/1]).
-export([add/4, set_fallback/2, lookup/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(APP, rabbit_mochiweb).

%% This gen_server is merely to serialise modifications to the dispatch
%% table for instances.

start_link(InstanceSpecs) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [InstanceSpecs], []).

add(Context, Selector, Handler, Link) ->
    gen_server:call(?MODULE, {add, Context, Selector, Handler, Link}).

set_fallback(Instance, FallbackHandler) ->
    gen_server:call(?MODULE, {set_fallback, Instance, FallbackHandler}).

lookup(Instance, Req) ->
    case lookup_dispatch(Instance) of
        {Selectors, Fallback} ->
            case catch match_request(Selectors, Req) of
                {'EXIT', Reason} ->
                    {lookup_failure, Reason};
                no_handler ->
                    {handler, Fallback};
                Handler ->
                    {handler, Handler}
            end;
        Err ->
            {lookup_failure, Err}
    end.

%% Callback Methods

init([Instances]) ->
    [application:set_env(?APP, {dispatch, Instance},
                         {[], listing_fallback_handler(Instance)})
     || {Instance, _} <- Instances],
    {ok, undefined}.

handle_call({add, Context, Selector, Handler, Link}, _From,
            undefined) ->
    Instance = rabbit_mochiweb:context_listener(Context),
    case lookup_dispatch(Instance) of
        {Selectors, Fallback} ->
            set_dispatch(Instance,
                         {Selectors ++ [{Context, Selector, Handler, Link}],
                          Fallback}),
            {reply, ok, undefined};
        Err ->
            {stop, Err, undefined}
    end;
handle_call({set_fallback, Instance, FallbackHandler}, _From,
            undefined) ->
    case lookup_dispatch(Instance) of
        {Selectors, _OldFallback} ->
            set_dispatch(Instance, {Selectors, FallbackHandler}),
            {reply, ok, undefined};
        Err ->
            {stop, Err, undefined}
    end;

%% NB This isn't exposed by an exported procedure
handle_call({list, Instance}, _From, undefined) ->
    case lookup_dispatch(Instance) of
        {Selectors, _Fallback} ->
            {reply, [Link || {_S, _H, Link} <- Selectors], undefined};
        Err ->
            {stop, Err, undefined}
    end;

handle_call(Req, _From, State) ->
    error_logger:format("Unexpected call to ~p: ~p~n", [?MODULE, Req]),
    {stop, unknown_request, State}.

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

lookup_dispatch(Instance) ->
    case application:get_env(?APP, {dispatch, Instance}) of
        {ok, Dispatch} ->
            Dispatch;
        undefined ->
            {no_record_for_instance, Instance}
    end.

set_dispatch(Instance, Dispatch) ->
    application:set_env(?APP, {dispatch, Instance}, Dispatch).

match_request([], _) ->
    no_handler;
match_request([{_Context, Selector, Handler, _Link}|Rest], Req) ->
    case Selector(Req) of
        true  -> Handler;
        false -> match_request(Rest, Req)
    end.

%%---------------------------------------------------------------------------

listing_fallback_handler(Instance) ->
    fun(Req) ->
            HTMLPrefix =
                "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">"
                "<head><title>RabbitMQ Web Server</title></head>"
                "<body><h1>RabbitMQ Web Server</h1><p>Contexts available:</p><ul>",
            HTMLSuffix = "</ul></body></html>",
    Contexts = [{"/" ++ P, D} ||
                   {P, D} <- gen_server:call(?MODULE, {list, Instance})],
            List =
                case Contexts of
                    [] ->
                        "<li>No contexts installed</li>";
                    _ ->
                        [io_lib:format("<li><a href=\"~s\">~s</a></li>", [Path, Desc])
                         || {Path, Desc} <- Contexts, Desc =/= none] ++
                            [io_lib:format("<li>~s</li>", [Path])
                             || {Path, Desc} <- Contexts, Desc == none]
                end,
            Req:respond({200, [], HTMLPrefix ++ List ++ HTMLSuffix})
    end.
