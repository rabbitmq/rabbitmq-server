-module(rabbit_amqp1_0_client_sup).
-behaviour(supervisor2).

-define(MAX_WAIT, 16#ffffffff).
-export([start_link/0, init/1]).

start_link() ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, Collector} =
        supervisor2:start_child(
          SupPid,
          {collector, {rabbit_queue_collector, start_link, []},
           intrinsic, ?MAX_WAIT, worker, [rabbit_queue_collector]}),
    {ok, ChannelSupSupPid} =
        supervisor2:start_child(
          SupPid,
          {channel_sup_sup, {rabbit_channel_sup_sup, start_link, []},
           intrinsic, infinity, supervisor, [rabbit_channel_sup_sup]}),
    {ok, ReaderPid} =
        supervisor2:start_child(
          SupPid,
          {reader, {rabbit_amqp1_0_reader, start_link,
                    [ChannelSupSupPid, Collector,
                     rabbit_heartbeat:start_heartbeat_fun(SupPid)]},
           intrinsic, ?MAX_WAIT, worker, [rabbit_amqp1_0_reader]}),
    {ok, SupPid, ReaderPid}.

reader(Pid) ->
    hd(supervisor2:find_child(Pid, reader)).

%%--------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
