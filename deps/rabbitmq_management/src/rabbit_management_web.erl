%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Status Plugin.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_management_web).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([handle_request_unauth/1]).
-export([update/0]).

-export([handle_http_request/2, handle_json_request/2]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(REFRESH_RATIO, 15000).


%%--------------------------------------------------------------------

-record(state, {
        time_ms,
        datetime,
        bound_to,
        connections,
        queues,
        fd_used,
        fd_total,
        mem_used,
        mem_total,
        proc_used,
        proc_total
        }).


%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_context(Timeout) ->
    gen_server2:call(?MODULE, get_context, Timeout).

% By default, let's not wait too long. If that takes more than 1 second,
% it's better to quickly return 408 "request timeout" rather than hang.
get_context() ->
    get_context(1000).

update() ->
    gen_server2:cast(?MODULE, update).


%%--------------------------------------------------------------------

handle_request_unauth(Req) ->
    case Req:get_header_value("Authorization") of
        undefined ->
            send_auth_request(Req);
        AuthHeader ->
            {_Type, [_Space|Auth]} = lists:splitwith(fun (A) -> A =/= 32 end,
                                                    AuthHeader),
            {User, [_Colon|Pass]} = lists:splitwith(fun (A) -> A =/= $: end,
                                                base64:decode_to_string(Auth)),

            case rabbit_access_control:lookup_user(list_to_binary(User)) of
                {ok, U}  -> case list_to_binary(Pass) == U#user.password of
                                true -> handle_request(Req);
                                false -> send_auth_request(Req)
                            end;
                {error, _} -> send_auth_request(Req)
            end
    end.

send_auth_request(Req) ->
    Req:respond({401, [
        {"WWW-Authenticate", "Basic realm=\"RabbitMQ Status Page\""},
        {"Content-Type", "text/html"}
        ], "401 Unauthorised.\n"}).


handle_request(Req) ->
    case Req:get(path) of
        "/"      -> apply_context(handle_http_request, Req);
        "/json"  -> apply_context(handle_json_request, Req);
        "/json/" -> apply_context(handle_json_request, Req);
        _ ->  Req:respond({404, [{"Content-Type", "text/html; charset=utf-8"}],
                                    <<"404 Not found.">>})
    end.


handle_json_request(Req, Context) ->
    [Datetime, BoundTo,
        RConns, RQueues,
        FdUsed, FdTotal,
        MemUsed, MemTotal,
        ProcUsed, ProcTotal ]
            = Context,

    Json = {struct,
            [{node, node()},
             {pid, list_to_binary(os:getpid())},
             {datetime, list_to_binary(Datetime)},
             {bound_to, list_to_binary(BoundTo)},
             {connections, [{struct,RConn} || RConn <- RConns]},
             {queues, [{struct,RQueue} || RQueue <- RQueues]},
             {fd_used, FdUsed},
             {fd_total, FdTotal},
             {mem_used, MemUsed},
             {mem_total, MemTotal},
             {proc_used, ProcUsed},
             {proc_total, ProcTotal},
             {fd_warn, get_warning_level(FdUsed, FdTotal)},
             {mem_warn, get_warning_level(MemUsed, MemTotal)},
             {proc_warn, get_warning_level(ProcUsed, ProcTotal)},
             {mem_ets, erlang:memory(ets)},
             {mem_binary, erlang:memory(binary)}
            ]},
    Resp = mochijson2:encode(Json),
    Req:respond({200, [
                {"Refresh", status_render:print("~p", trunc(?REFRESH_RATIO/1000))},
                {"Content-Type", "application/json; charset=utf-8"}
            ], Resp}).


apply_context(Fun, Req) ->
    Res = try
	      {ok, get_context()}
	  catch
	      exit:{timeout, _} ->
		  {timeout, undefined}
	  end,
    case Res of
	{ok, Context} ->
	    apply(?MODULE, Fun, [Req, Context]);
	{timeout, _} ->
	    Req:respond({408, [{"Refresh", status_render:print(
					     "~p", trunc(?REFRESH_RATIO/1000))},
			       {"Content-Type", "text/plain; charset=utf-8"}
			      ], <<"408 Request Timeout.\n">>})
    end.


handle_http_request(Req, Context) ->
    [Datetime, BoundTo,
     RConns, RQueues,
     FdUsed, FdTotal,
     MemUsed, MemTotal,
     ProcUsed, ProcTotal ] = Context,

    FdWarn = get_warning_level(FdUsed, FdTotal),
    MemWarn = get_warning_level(MemUsed, MemTotal),
    ProcWarn = get_warning_level(ProcUsed, ProcTotal),

    Resp0 = template:render([os:getpid(),
                            Datetime, BoundTo,
                            [[ V || {_K, V} <- RConn] || RConn <- RConns],
                            [[ V || {_K, V} <- RQueue] || RQueue <- RQueues],
                            ProcUsed, ProcTotal, ProcWarn,
                            FdUsed, FdTotal, FdWarn,
                            status_render:format_info(memory, MemUsed),
                            status_render:format_info(memory, MemTotal),
                            MemWarn,
                            status_render:format_info(memory, erlang:memory(ets)),
                            status_render:format_info(memory, erlang:memory(binary))]),
    Resp1 = [status_render:widget_to_binary(A) || A <- Resp0],
    Req:respond({200, [
                {"Refresh", status_render:print("~p", trunc(?REFRESH_RATIO/1000))},
                {"Content-Type", "text/html; charset=utf-8"}
            ], iolist_to_binary(Resp1)}).


%%--------------------------------------------------------------------

get_total_fd_ulimit() ->
    {MaxFds, _} = string:to_integer(os:cmd("ulimit -n")),
    MaxFds.

get_total_fd() ->
    get_total_fd(os:type()).

get_total_fd({unix, Os}) when Os =:= linux
                       orelse Os =:= darwin
                       orelse Os =:= freebsd
                       orelse Os =:= sunos ->
    get_total_fd_ulimit();

get_total_fd(_) ->
    unknown.


get_used_fd_lsof() ->
    Lsof = os:cmd("lsof -d \"0-9999999\" -lna -p " ++ os:getpid()),
    string:words(Lsof, $\n).

get_used_fd() ->
    get_used_fd(os:type()).

get_used_fd({unix, Os}) when Os =:= linux
                      orelse Os =:= darwin
                      orelse Os =:= freebsd ->
    get_used_fd_lsof();


get_used_fd(_) ->
    unknown.


get_total_memory() ->
    vm_memory_monitor:get_vm_memory_high_watermark() *
	vm_memory_monitor:get_total_memory().


get_warning_level(Used, Total) ->
    if
        is_number(Used) andalso is_number(Total) ->
            Ratio = Used/Total,
            if
                Ratio > 0.75 -> red;
                Ratio > 0.50 -> yellow;
                true         -> green
            end;
        true -> none
    end.


%%--------------------------------------------------------------------

init([]) ->
    {ok, Binds} = application:get_env(rabbit, tcp_listeners),
    BoundTo = lists:flatten( [ status_render:print("~s:~p ", [Addr,Port])
                                                || {Addr, Port} <- Binds ] ),
    State = #state{
            fd_total = get_total_fd(),
            mem_total = get_total_memory(),
            proc_total = erlang:system_info(process_limit),
            bound_to = BoundTo
        },
    {ok, internal_update(State)}.


handle_call(get_context, _From, State0) ->
    State = case now_ms() - State0#state.time_ms > ?REFRESH_RATIO of
        true  -> internal_update(State0);
        false -> State0
    end,

    Context = [ State#state.datetime,
                State#state.bound_to,
                State#state.connections,
                State#state.queues,
                State#state.fd_used,
                State#state.fd_total,
                State#state.mem_used,
                State#state.mem_total,
                State#state.proc_used,
                State#state.proc_total ],
    {reply, Context, State};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.


handle_cast(update, State) ->
    {noreply, internal_update(State)};

handle_cast(_C, State) ->
    {noreply, State}.


handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.


internal_update(State) ->
    State#state{
        time_ms = now_ms(),
        datetime = httpd_util:rfc1123_date(erlang:universaltime()),
        connections = status_render:render_conns(),
        queues = status_render:render_queues(),
        fd_used = get_used_fd(),
        mem_used = erlang:memory(total),
        proc_used = erlang:system_info(process_count)
    }.


now_ms() ->
    {MegaSecs, Secs, MicroSecs} = now(),
    trunc(MegaSecs*1000000000 + Secs*1000 + MicroSecs/1000).
