-module(rabbit_cli_transport).

-include_lib("kernel/include/logger.hrl").

-export([connect/0, connect/1,
         get_client_info/1,
         run_command/2,
         gen_reply/3,
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
        ?LOG_DEBUG(
           "CLI: connect to node ~s using Erlang distribution",
           [Nodename1]),

        %% FIXME: Handle short vs. long names.
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
        ?LOG_DEBUG(
           "CLI: connect to URI ~s using HTTP",
           [Uri]),
        {ok, Client} ?= rabbit_cli_http_client:start_link(Uri),
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

get_client_info(#?MODULE{type = Proto}) ->
    {ok, Hostname} = inet:gethostname(),
    #{hostname => Hostname,
      proto => Proto};
get_client_info(none) ->
    {ok, Hostname} = inet:gethostname(),
    #{hostname => Hostname,
      proto => none}.

run_command(#?MODULE{type = erldist, peer = Node}, ContextMap) ->
    Caller = self(),
    erpc:call(Node, rabbit_cli_backend, run_command, [ContextMap, Caller]);
run_command(#?MODULE{type = http, peer = Client}, ContextMap) ->
    rabbit_cli_http_client:run_command(Client, ContextMap).

gen_reply(#?MODULE{type = erldist}, From, Reply) ->
    gen:reply(From, Reply);
gen_reply(#?MODULE{type = http, peer = Client}, From, Reply) ->
    rabbit_cli_http_client:gen_reply(Client, From, Reply);
gen_reply(none, From, Reply) ->
    gen:reply(From, Reply).

send(#?MODULE{type = erldist}, Dest, Msg) ->
    erlang:send(Dest, Msg);
send(#?MODULE{type = http, peer = Client}, Dest, Msg) ->
    rabbit_cli_http_client:send(Client, Dest, Msg);
send(none, Dest, Msg) ->
    erlang:send(Dest, Msg).
