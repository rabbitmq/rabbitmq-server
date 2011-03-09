-module(rabbit_mochiweb_app).

-behaviour(application).
-export([start/2,stop/1]).

%% For testing
-export([check_contexts/2]).

-define(APP, rabbit_mochiweb).

%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for rabbit_mochiweb.
start(_Type, _StartArgs) ->
    {ok, Instances} = application:get_env(?APP, listeners),
    Contexts = case application:get_env(?APP, contexts) of
                   {ok, Cs}  -> Cs;
                   undefined -> []
               end,
    case check_contexts(Contexts, Instances) of
        ok ->
            rabbit_mochiweb_sup:start_link(Instances);
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
check_contexts(Contexts, Listeners) ->
    case proplists:get_value('*', Listeners) of
        undefined ->
            {error, no_default_listener};
        _ ->
            Checks = lists:foldl(
                       fun ({_Name, Listener}, Acc) ->
                               case proplists:get_value(Listener, Listeners) of
                                   undefined ->
                                       [{error, Listener} | Acc];
                                   _ ->
                                       [ok | Acc]
                               end
                       end, [], Contexts),
            case lists:usort([L || {error, L} <- Checks]) of
                [] ->
                    ok;
                Errors ->
                    {error, {undefined_listeners, Errors}}
            end
    end.
