-module(rabbit_mochiweb_web).

-export([start/1, stop/1]).

%% ----------------------------------------------------------------------
%% HTTPish API
%% ----------------------------------------------------------------------

start({Instance, Env}) ->
    case proplists:get_value(port, Env, undefined) of
        undefined ->
            {error, {no_port_given, Instance, Env}};
        P ->
            Loop = loopfun(Instance),
            {_, OtherOptions} = proplists:split(Env, [port]),
            Name = name(Instance),
            mochiweb_http:start(
              [{name, Name}, {port, P}, {loop, Loop}] ++
              OtherOptions)
    end.

stop(Instance) ->
    mochiweb_http:stop(name(Instance)).

loopfun(Instance) ->
    fun (Req) ->
            case rabbit_mochiweb_registry:lookup(Instance, Req) of
                no_handler ->
                    Req:not_found();
                {lookup_failure, Reason} ->
                    Req:respond({500, [], "Registry Error: " ++ Reason});
                {handler, Handler} ->
                    Handler(Req)
            end
    end.

name(Instance) ->
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Instance)).
