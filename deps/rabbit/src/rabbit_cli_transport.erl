-module(rabbit_cli_transport).

-export([connect/1,
         rpc/4]).

-record(http, {uri :: uri_string:uri_map(),
               gun :: pid()}).

connect(#{node := NodenameOrUri} = ArgMap) ->
    case re:run(NodenameOrUri, "://", [{capture, none}]) of
        nomatch ->
            connect_using_erldist(ArgMap);
        match ->
            connect_using_other_proto(ArgMap)
    end;
connect(ArgMap) ->
    connect_using_erldist(ArgMap).

rpc(Nodename, Mod, Func, Args) when is_atom(Nodename) ->
    rpc_using_erldist(Nodename, Mod, Func, Args).

%% -------------------------------------------------------------------
%% Erlang distribution.
%% -------------------------------------------------------------------

connect_using_erldist(#{node := Nodename}) ->
    do_connect_using_erldist(Nodename);
connect_using_erldist(_ArgMap) ->
    GuessedNodename = guess_rabbitmq_nodename(),
    do_connect_using_erldist(GuessedNodename).

do_connect_using_erldist(Nodename) ->
    maybe
        Nodename1 = complete_nodename(Nodename),
        {ok, _} ?= net_kernel:start(
                     undefined, #{name_domain => shortnames}),

        %% Can we reach the remote node?
        case net_kernel:connect_node(Nodename1) of
            true ->
                {ok, Nodename1};
            false ->
                {error, noconnection}
        end
    end.

guess_rabbitmq_nodename() ->
    case net_adm:names() of
        {ok, NamesAndPorts} ->
            Names0 = [Name || {Name, _Port} <- NamesAndPorts],
            Names1 = lists:sort(Names0),
            Names2 = lists:filter(
                       fun
                           ("rabbit" ++ _) -> true;
                           (_) ->             false
                       end, Names1),
            case Names2 of
                [First | _] ->
                    First;
                [] ->
                    "rabbit"
            end;
        {error, address} ->
            "rabbit"
    end.

complete_nodename(Nodename) ->
    case re:run(Nodename, "@", [{capture, none}]) of
        nomatch ->
            {ok, ThisHost} = inet:gethostname(),
            list_to_atom(Nodename ++ "@" ++ ThisHost);
        match ->
            list_to_atom(Nodename)
    end.

rpc_using_erldist(Nodename, Mod, Func, Args) ->
    erpc:call(Nodename, Mod, Func, Args).

%% -------------------------------------------------------------------
%% HTTP(S) transport.
%% -------------------------------------------------------------------

connect_using_other_proto(#{node := Uri}) ->
    maybe
        #{host := Host, port := Port} = UriMap = uri_string:parse(Uri),
        {ok, Gun} ?= gun:open(Host, Port),
        State = #http{uri = UriMap,
                      gun = Gun},
        {ok, State}
    end.
