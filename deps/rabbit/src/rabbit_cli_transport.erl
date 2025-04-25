-module(rabbit_cli_transport).
-behaviour(gen_server).

-export([connect/1,
         rpc/4]).
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
               pending_io_requests = #{} :: map(),
               group_leader :: pid()
              }).

connect(#{arg_map := #{node := NodenameOrUri}} = Context) ->
    case re:run(NodenameOrUri, "://", [{capture, none}]) of
        nomatch ->
            connect_using_erldist(Context);
        match ->
            connect_using_transport(Context)
    end;
connect(Context) ->
    connect_using_erldist(Context).

rpc(Nodename, Mod, Func, Args) when is_atom(Nodename) ->
    rpc_using_erldist(Nodename, Mod, Func, Args);
rpc(TransportPid, Mod, Func, Args) when is_pid(TransportPid) ->
    rpc_using_transport(TransportPid, Mod, Func, Args).

%% -------------------------------------------------------------------
%% Erlang distribution.
%% -------------------------------------------------------------------

connect_using_erldist(#{arg_map := #{node := Nodename}}) ->
    do_connect_using_erldist(Nodename);
connect_using_erldist(_Context) ->
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

connect_using_transport(Context) ->
    gen_server:start_link(?MODULE, Context, []).

rpc_using_transport(TransportPid, Mod, Func, Args) when is_pid(TransportPid) ->
    gen_server:call(TransportPid, {rpc, {Mod, Func, Args}}).

init(#{arg_map := #{node := Uri}, group_leader := GL}) ->
    maybe
        {ok, _} ?= application:ensure_all_started(gun),
        #{host := Host, port := Port} = UriMap = uri_string:parse(Uri),
        {ok, ConnPid} ?= gun:open(Host, Port),
        State = #http{uri = UriMap,
                      group_leader = GL,
                      conn = ConnPid},
        %logger:alert("Transport: State=~p", [State]),
        {ok, State}
    end.

handle_call(
  Request, From,
  #http{stream_ready = true} = State) ->
    %% HTTP message to the server side.
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
  #http{conn = ConnPid,
        stream = StreamRef,
        group_leader = GL,
        pending_io_requests = Pending} = State) ->
    %% HTTP message from the server side.
    Reply = binary_to_term(ReplyBin),
    State1 = case Reply of
                 % {io_call, From, Msg} ->
                 %     %logger:alert("IO call from WS: ~p -> ~p", [Msg, From]),
                 %     Ret = gen_server:call(IO, Msg),
                 %     RequestBin = term_to_binary({io_reply, From, Ret}),
                 %     Frame = {binary, RequestBin},
                 %     gun:ws_send(ConnPid, StreamRef, Frame);
                 % {io_cast, Msg} ->
                 %     %logger:alert("IO cast from WS: ~p", [Msg]),
                 %     gen_server:cast(IO, Msg);
                 {msg, group_leader, {io_request, RemoteFrom, ReplyAs, Request} = _Msg} ->
                     % logger:alert("Message from WS: ~p", [Msg]),
                     Ref = erlang:make_ref(),
                     IoRequest1 = {io_request, self(), Ref, Request},
                     GL ! IoRequest1,
                     Pending1 = Pending#{Ref => {RemoteFrom, ReplyAs}},
                     State#http{pending_io_requests = Pending1};
                 {ret, From, Ret} ->
                     %logger:alert("Reply from WS: ~p -> ~p", [Ret, From]),
                     gen_server:reply(From, Ret),
                     State;
                 _Other ->
                     %logger:alert("Reply from WS: ~p", [_Other]),
                     State
             end,
    {noreply, State1};
handle_info(
  {io_reply, ReplyAs, Reply} = _IoReply,
  #http{conn = ConnPid,
        stream = StreamRef,
        pending_io_requests = Pending} = State) ->
    % logger:alert("io_reply to WS: ~p", [IoReply]),
    {RemoteFrom, RemoteReplyAs} = maps:get(ReplyAs, Pending),
    Msg = {io_reply, RemoteReplyAs, Reply},
    RequestBin = term_to_binary({msg, RemoteFrom, Msg}),
    Frame = {binary, RequestBin},
    gun:ws_send(ConnPid, StreamRef, Frame),

    Pending1 = maps:remove(ReplyAs, Pending),
    State1 = State#http{pending_io_requests = Pending1},
    {noreply, State1};
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
