-module(rabbit_cli_transport).
-behaviour(gen_server).

-export([connect/2,
         rpc/4,
         rpc_with_io/4]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         config_change/3]).

-record(http, {uri :: uri_string:uri_map(),
               conn :: pid(),
               stream :: gun:stream_ref(),
               stream_ready = false :: boolean(),
               pending = [] :: [any()],
               io :: pid()
              }).

connect(#{node := NodenameOrUri} = ArgMap, IO) ->
    case re:run(NodenameOrUri, "://", [{capture, none}]) of
        nomatch ->
            connect_using_erldist(ArgMap, IO);
        match ->
            connect_using_transport(ArgMap, IO)
    end;
connect(ArgMap, IO) ->
    connect_using_erldist(ArgMap, IO).

rpc({Nodename, _IO} = Connection, Mod, Func, Args) when is_atom(Nodename) ->
    rpc_using_erldist(Connection, Mod, Func, Args);
rpc(TransportPid, Mod, Func, Args) when is_pid(TransportPid) ->
    rpc_using_transport(TransportPid, Mod, Func, Args).

rpc_with_io({Nodename, _IO} = Connection, Mod, Func, Args) when is_atom(Nodename) ->
    rpc_with_io_using_erldist(Connection, Mod, Func, Args);
rpc_with_io(TransportPid, Mod, Func, Args) when is_pid(TransportPid) ->
    rpc_with_io_using_transport(TransportPid, Mod, Func, Args).

%% -------------------------------------------------------------------
%% Erlang distribution.
%% -------------------------------------------------------------------

connect_using_erldist(#{node := Nodename}, IO) ->
    do_connect_using_erldist(Nodename, IO);
connect_using_erldist(_ArgMap, IO) ->
    GuessedNodename = guess_rabbitmq_nodename(),
    do_connect_using_erldist(GuessedNodename, IO).

do_connect_using_erldist(Nodename, IO) ->
    maybe
        Nodename1 = complete_nodename(Nodename),
        {ok, _} ?= net_kernel:start(
                     undefined, #{name_domain => shortnames}),

        %% Can we reach the remote node?
        case net_kernel:connect_node(Nodename1) of
            true ->
                {ok, {Nodename1, IO}};
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

rpc_using_erldist({Nodename, _IO}, Mod, Func, Args) ->
    erpc:call(Nodename, Mod, Func, Args).

rpc_with_io_using_erldist({Nodename, IO}, Mod, Func, Args) ->
    erpc:call(Nodename, Mod, Func, Args ++ [IO]).

%% -------------------------------------------------------------------
%% HTTP(S) transport.
%% -------------------------------------------------------------------

connect_using_transport(ArgMap, IO) ->
    gen_server:start_link(?MODULE, {ArgMap, IO}, []).

rpc_using_transport(TransportPid, Mod, Func, Args) when is_pid(TransportPid) ->
    gen_server:call(TransportPid, {rpc, {Mod, Func, Args}, #{io => false}}).

rpc_with_io_using_transport(TransportPid, Mod, Func, Args) when is_pid(TransportPid) ->
    gen_server:call(TransportPid, {rpc, {Mod, Func, Args}, #{io => true}}).

init({#{node := Uri}, IO}) ->
    maybe
        {ok, _} ?= application:ensure_all_started(gun),
        #{host := Host, port := Port} = UriMap = uri_string:parse(Uri),
        {ok, ConnPid} ?= gun:open(Host, Port),
        State = #http{uri = UriMap,
                      conn = ConnPid,
                      io = IO},
        %logger:alert("Transport: State=~p", [State]),
        {ok, State}
    end.

handle_call(
  Request, From,
  #http{stream_ready = true} = State) ->
    send_call(Request, From, State),
    {noreply, State};
handle_call(
  Request, From,
  #http{stream_ready = false, pending = Pending} = State) ->
    %logger:alert("Transport(call): ~p", [Request]),
    State1 = State#http{pending = [{From, Request} | Pending]},
    {noreply, State1};
handle_call(_Request, _From, State) ->
    %logger:alert("Transport(call): ~p", [_Request]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    %logger:alert("Transport(cast): ~p", [_Request]),
    {noreply, State}.

handle_info(
  {gun_up, ConnPid, _},
  #http{conn = ConnPid} = State) ->
    %logger:alert("Transport(info): Conn up"),
    StreamRef = gun:ws_upgrade(ConnPid, "/", []),
    State1 = State#http{stream = StreamRef},
    {noreply, State1};
handle_info(
  {gun_upgrade, ConnPid, StreamRef, _Frames, _},
  #http{conn = ConnPid, stream = StreamRef, pending = Pending} = State) ->
    %logger:alert("Transport(info): WS upgraded, ~p", [_Frames]),
    State1 = State#http{stream_ready = true, pending = []},
    Pending1 = lists:reverse(Pending),
    lists:foreach(
      fun({From, Request}) ->
              send_call(Request, From, State1)
      end, Pending1),
    {noreply, State1};
handle_info(
  {gun_ws, ConnPid, StreamRef, {binary, ReplyBin}},
  #http{conn = ConnPid, stream = StreamRef, io = IO} = State) ->
    Reply = binary_to_term(ReplyBin),
    case Reply of
        {io_call, From, Msg} ->
            %logger:alert("IO call from WS: ~p -> ~p", [Msg, From]),
            Ret = gen_server:call(IO, Msg),
            RequestBin = term_to_binary({io_reply, From, Ret}),
            Frame = {binary, RequestBin},
            gun:ws_send(ConnPid, StreamRef, Frame);
        {io_cast, Msg} ->
            %logger:alert("IO cast from WS: ~p", [Msg]),
            gen_server:cast(IO, Msg);
        {ret, From, Ret} ->
            %logger:alert("Reply from WS: ~p -> ~p", [Ret, From]),
            gen_server:reply(From, Ret);
        _Other ->
            %logger:alert("Reply from WS: ~p", [_Other]),
            ok
    end,
    {noreply, State};
handle_info(_Info, State) ->
    %logger:alert("Transport(info): ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

config_change(_OldVsn, State, _Extra) ->
    {ok, State}.

send_call(Request, From, #http{conn = ConnPid, stream = StreamRef}) ->
    RequestBin = term_to_binary({call, From, Request}),
    Frame = {binary, RequestBin},
    gun:ws_send(ConnPid, StreamRef, Frame).
