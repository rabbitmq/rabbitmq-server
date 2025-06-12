-module(rabbit_cli_transport2).

-export([connect/0, connect/1,
         rpc/4,
         send/3]).

-record(?MODULE, {type :: erldist | http,
                  peer :: atom() | pid()}).

connect() ->
    Nodename = guess_rabbitmq_nodename(),
    connect(erldist, Nodename).

connect(NodenameOrUri) ->
    Proto = determine_proto(NodenameOrUri),
    connect(Proto, NodenameOrUri).

connect(erldist = Proto, Nodename) ->
    maybe
        Nodename1 = complete_nodename(Nodename),
        {ok, _} ?= net_kernel:start(undefined, #{name_domain => shortnames}),

        %% Can we reach the remote node?
        case net_kernel:connect_node(Nodename1) of
            true ->
                Connection = #?MODULE{type = Proto,
                                      peer = Nodename1},
                {ok, Connection};
            false ->
                {error, noconnection}
        end
    end;
connect(http = Proto, Uri) ->
    maybe
        {ok, Client} = rabbit_cli_http_client:start_link(Uri),
        Connection = #?MODULE{type = Proto,
                              peer = Client},
        {ok, Connection}
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

determine_proto(NodenameOrUri) ->
    case re:run(NodenameOrUri, "://", [{capture, none}]) of
        nomatch ->
            erldist;
        match ->
            http
    end.

complete_nodename(Nodename) ->
    case re:run(Nodename, "@", [{capture, none}]) of
        nomatch ->
            {ok, ThisHost} = inet:gethostname(),
            list_to_atom(Nodename ++ "@" ++ ThisHost);
        match ->
            list_to_atom(Nodename)
    end.

rpc(#?MODULE{type = erldist}, Module, Function, Args) ->
    erlang:apply(Module, Function, Args);
rpc(#?MODULE{type = http, peer = Pid}, Module, Function, Args) ->
    rabbit_cli_http_client:rpc(Pid, Module, Function, Args).

send(#?MODULE{type = erldist}, Dest, Msg) ->
    erlang:send(Dest, Msg);
send(#?MODULE{type = http, peer = Pid}, Dest, Msg) ->
    rabbit_cli_http_client:send(Pid, Dest, Msg).
