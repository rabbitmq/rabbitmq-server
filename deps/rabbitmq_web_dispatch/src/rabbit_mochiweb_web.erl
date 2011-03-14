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
              easy_ssl(OtherOptions))
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

easy_ssl(Options) ->
    case {proplists:get_value(ssl, Options),
          proplists:get_value(ssl_opts, Options)} of
        {true, undefined} ->
            {ok, ServerOpts} = application:get_env(rabbit, ssl_options),
            SSLOpts = [{K, V} ||
                          {K, V} <- ServerOpts,
                          not lists:member(K, [verify, fail_if_no_peer_cert])],
            [{ssl_opts, SSLOpts}|Options];
        _ ->
            Options
    end.
