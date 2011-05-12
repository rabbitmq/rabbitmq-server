-module(rabbit_mochiweb_app).

-behaviour(application).
-export([start/2,stop/1]).

%% For testing
-export([check_contexts/2]).

-define(APP, rabbitmq_mochiweb).

%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for rabbit_mochiweb.
start(_Type, _StartArgs) ->
    {ok, Listeners} = application:get_env(?APP, listeners),
    {ok, Contexts} = application:get_env(?APP, contexts),
    case check_contexts(Listeners, Contexts) of
        ok ->
            rabbit_mochiweb_sup:start_link(Listeners);
        Err ->
            Err
    end.

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for rabbit_mochiweb.
stop(_State) ->
    ok.

%% Internal

%% Check that there is a default listener '*', and no context mentions
%% a listener that doesn't exist.
check_contexts(Listeners, Contexts) when
      is_list(Contexts), is_list(Listeners) ->
    case proplists:get_value('*', Listeners) of
        undefined ->
            {error, no_default_listener};
        _ ->
            Checks = lists:foldl(
                       fun ({_Name, Listener}, Acc) ->
                               case proplists:get_value(Listener, Listeners) of
                                   undefined ->
                                       [Listener | Acc];
                                   _ ->
                                       Acc
                               end
                       end, [], Contexts),
            case Checks of
                [] ->
                    ok;
                Errors ->
                    {error, {undefined_listeners, Errors}}
            end
    end;
check_contexts(_Cs, _Ls) ->
    {error, invalid_configuration}.
