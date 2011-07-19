-module(rabbit_mochiweb_registry).

-behaviour(gen_server).

-export([start_link/1]).
-export([add/4, set_fallback/2, lookup/2, list_all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(APP, rabbitmq_mochiweb).

%% This gen_server is merely to serialise modifications to the dispatch
%% table for listeners.

start_link(ListenerSpecs) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [ListenerSpecs], []).

add(Context, Selector, Handler, Link) ->
    gen_server:call(?MODULE, {add, Context, Selector, Handler, Link}, infinity).

set_fallback(Listener, FallbackHandler) ->
    gen_server:call(?MODULE, {set_fallback, Listener, FallbackHandler},
                    infinity).

lookup(Listener, Req) ->
    case lookup_dispatch(Listener) of
        {Selectors, Fallback} ->
            case catch match_request(Selectors, Req) of
                {'EXIT', Reason} -> {lookup_failure, Reason};
                no_handler       -> {handler, Fallback};
                Handler          -> {handler, Handler}
            end;
        Err ->
            {lookup_failure, Err}
    end.

list_all() ->
    gen_server:call(?MODULE, list_all, infinity).

%% Callback Methods

init([ListenerSpecs]) ->
    [application:set_env(?APP, {dispatch, Listener},
                         {[], listing_fallback_handler(Listener)})
     || {Listener, _} <- ListenerSpecs],
    {ok, undefined}.

handle_call({add, Context, Selector, Handler, Link}, _From,
            undefined) ->
    ListenerSpec = {Listener, _Opts} =
        rabbit_mochiweb:context_listener(Context),
    rabbit_mochiweb_sup:ensure_listener(ListenerSpec),
    case lookup_dispatch(Listener) of
        {Selectors, Fallback} ->
            set_dispatch(Listener,
                         {Selectors ++ [{Context, Selector, Handler, Link}],
                          Fallback}),
            {reply, ok, undefined};
        Err ->
            {stop, Err, undefined}
    end;
handle_call({set_fallback, Listener, FallbackHandler}, _From,
            undefined) ->
    case lookup_dispatch(Listener) of
        {Selectors, _OldFallback} ->
            set_dispatch(Listener, {Selectors, FallbackHandler}),
            {reply, ok, undefined};
        Err ->
            {stop, Err, undefined}
    end;

%% NB This isn't exposed by an exported procedure
handle_call({list, Listener}, _From, undefined) ->
    case lookup_dispatch(Listener) of
        {Selectors, _Fallback} ->
            {reply, [Link || {_C, _S, _H, Link} <- Selectors], undefined};
        Err ->
            {stop, Err, undefined}
    end;

handle_call(list_all, _From, undefined) ->
    Listeners = rabbit_mochiweb:all_listeners(),
    Res = [{Path, Desc, proplists:get_value(Name, Listeners)} ||
                {{dispatch, Name}, {V, _}} <- application:get_all_env(),
                {_, _, _, {Path, Desc}} <- V],
    {reply, Res, undefined};

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

lookup_dispatch(Listener) ->
    case application:get_env(?APP, {dispatch, Listener}) of
        {ok, Dispatch} -> Dispatch;
        undefined      -> {no_record_for_listener, Listener}
    end.

set_dispatch(Listener, Dispatch) ->
    application:set_env(?APP, {dispatch, Listener}, Dispatch).

match_request([], _) ->
    no_handler;
match_request([{_Context, Selector, Handler, _Link}|Rest], Req) ->
    case Selector(Req) of
        true  -> Handler;
        false -> match_request(Rest, Req)
    end.

%%---------------------------------------------------------------------------

listing_fallback_handler(Listener) ->
    fun(Req) ->
            HTMLPrefix =
                "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">"
                "<head><title>RabbitMQ Web Server</title></head>"
                "<body><h1>RabbitMQ Web Server</h1><p>Contexts available:</p><ul>",
            HTMLSuffix = "</ul></body></html>",
            {ReqPath, _, _} = mochiweb_util:urlsplit_path(Req:get(raw_path)),
            Contexts = gen_server:call(?MODULE, {list, Listener}, infinity),
            List =
                case Contexts of
                    [] ->
                        "<li>No contexts installed</li>";
                    _ ->
                        [handler_listing(Path, ReqPath, Desc)
                         || {Path, Desc} <- Contexts]
                end,
            Req:respond({200, [], HTMLPrefix ++ List ++ HTMLSuffix})
    end.

handler_listing(Path, ReqPath, Desc) ->
    io_lib:format(
      "<li><a href=\"~s\">~s</a></li>",
      [rabbit_mochiweb_util:relativise(ReqPath, "/" ++ Path), Desc]).
