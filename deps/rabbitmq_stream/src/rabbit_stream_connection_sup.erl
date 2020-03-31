-module(rabbit_stream_connection_sup).

-behaviour(supervisor2).
-behaviour(ranch_protocol).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/4, start_keepalive_link/0]).

-export([init/1]).


start_link(Ref, _Sock, Transport, Opts) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, KeepaliveSup} = supervisor2:start_child(
        SupPid,
        {rabbit_stream_keepalive_sup,
            {rabbit_stream_connection_sup, start_keepalive_link, []},
            intrinsic, infinity, supervisor, [rabbit_keepalive_sup]}),
    {ok, ReaderPid} = supervisor2:start_child(
        SupPid,
        {rabbit_stream_reader,
            {rabbit_stream_reader, start_link, [KeepaliveSup, Transport, Ref, Opts]},
            intrinsic, ?WORKER_WAIT, worker, [rabbit_mqtt_reader]}),
    {ok, SupPid, ReaderPid}.

start_keepalive_link() ->
    supervisor2:start_link(?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
