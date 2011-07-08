-module(rabbit_mochiweb_app).

-behaviour(application).
-export([start/2,stop/1]).

%% For testing
-export([check_contexts/2]).

-define(APP, rabbitmq_mochiweb).

%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for rabbit_mochiweb.
start(_Type, _StartArgs) ->
    Listeners = rabbit_mochiweb:all_listeners(),
    {ok, Contexts} = application:get_env(?APP, contexts),
    case check_contexts(Listeners, Contexts) of
        ok  -> rabbit_mochiweb_sup:start_link(Listeners);
        Err -> Err
    end.

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for rabbit_mochiweb.
stop(_State) ->
    ok.

%% Internal

%% Check that no context mentions a listener that doesn't exist.
check_contexts(Listeners, Contexts) when
      is_list(Contexts), is_list(Listeners) ->
    HasListener = fun(Listener, Acc) ->
                          case proplists:get_value(Listener, Listeners) of
                              undefined -> [Listener | Acc];
                              _         -> Acc
                          end
                  end,
    Checks = lists:foldl(
               fun ({_Name, {Listener, _Path}}, Acc) ->
                       HasListener(Listener, Acc);
                   ({_Name, Listener}, Acc) ->
                       HasListener(Listener, Acc)
               end, [], Contexts),
    case Checks of
        [] ->
            ok;
        Errors ->
            {error, {undefined_listeners, Errors}}
    end;
check_contexts(_Cs, _Ls) ->
    {error, invalid_configuration}.
