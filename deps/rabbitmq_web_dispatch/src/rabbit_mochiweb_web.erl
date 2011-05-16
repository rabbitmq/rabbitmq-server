-module(rabbit_mochiweb_web).

-export([start/1, stop/1]).

%% ----------------------------------------------------------------------
%% HTTPish API
%% ----------------------------------------------------------------------

start({ListenerSpec, Env}) ->
    case proplists:get_value(port, Env, undefined) of
        undefined ->
            {error, {no_port_given, ListenerSpec, Env}};
        _ ->
            Loop = loopfun(ListenerSpec),
            OtherOptions = proplists:delete(name, Env),
            Name = name(ListenerSpec),
            mochiweb_http:start(
              [{name, Name}, {loop, Loop} | OtherOptions])
    end.

stop(Listener) ->
    mochiweb_http:stop(name(Listener)).

loopfun(Listener) ->
    fun (Req) ->
            case rabbit_mochiweb_registry:lookup(Listener, Req) of
                no_handler ->
                    Req:not_found();
                {lookup_failure, Reason} ->
                    Req:respond({500, [], "Registry Error: " ++ Reason});
                {handler, Handler} ->
                    Handler(Req)
            end
    end.

name(Listener) ->
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Listener)).
