-module(queue_utils).

-include_lib("eunit/include/eunit.hrl").

-export([
         wait_for_messages_ready/3,
         wait_for_messages_pending_ack/3,
         wait_for_messages_total/3,
         wait_for_messages/2,
         wait_for_messages/3,
         wait_for_messages/4,
         wait_for_min_messages/3,
         wait_for_max_messages/3,
         dirty_query/3,
         ra_name/1,
         ra_machines_use_same_version/3,
         wait_for_local_stream_member/4,
         has_local_stream_member_rpc/1
        ]).

-define(WFM_SLEEP, 256).
-define(WFM_DEFAULT_NUMS, 30_000 div ?WFM_SLEEP). %% ~30s

wait_for_messages_ready(Servers, QName, Ready) ->
    wait_for_messages(Servers, QName, Ready,
                      fun rabbit_fifo:query_messages_ready/1,
                      ?WFM_DEFAULT_NUMS).

wait_for_messages_pending_ack(Servers, QName, Ready) ->
    wait_for_messages(Servers, QName, Ready,
                      fun rabbit_fifo:query_messages_checked_out/1,
                      ?WFM_DEFAULT_NUMS).

wait_for_messages_total(Servers, QName, Total) ->
    wait_for_messages(Servers, QName, Total,
                      fun rabbit_fifo:query_messages_total/1,
                      ?WFM_DEFAULT_NUMS).

wait_for_messages(Servers, QName, Total, Fun) ->
    wait_for_messages(Servers, QName, Total, Fun, ?WFM_DEFAULT_NUMS).

wait_for_messages(Servers, QName, Number, Fun, 0) ->
    Msgs = dirty_query(Servers, QName, Fun),
    ?assertEqual([Number || _ <- lists:seq(1, length(Servers))], Msgs);
wait_for_messages(Servers, QName, Number, Fun, N) ->
    Msgs = dirty_query(Servers, QName, Fun),
    ct:log("Got messages ~tp ~tp", [QName, Msgs]),
    %% hack to allow the check to succeed in mixed versions clusters if at
    %% least one node matches the criteria rather than all nodes for
    F = case rabbit_ct_helpers:is_mixed_versions() of
            true ->
                any;
            false ->
                all
        end,
    case lists:F(fun(C) when is_integer(C) ->
                         C == Number;
                    (_) ->
                         false
                 end, Msgs) of
        true ->
            ok;
        _ ->
            timer:sleep(?WFM_SLEEP),
            wait_for_messages(Servers, QName, Number, Fun, N - 1)
    end.

wait_for_messages(Config, Stats) ->
    wait_for_messages(Config, lists:sort(Stats), ?WFM_DEFAULT_NUMS).

wait_for_messages(Config, Stats, 0) ->
    ?assertEqual(Stats,
                 lists:sort(
                   filter_queues(Stats,
                                 rabbit_ct_broker_helpers:rabbitmqctl_list(
                                   Config, 0, ["list_queues", "name", "messages", "messages_ready",
                                               "messages_unacknowledged"]))));
wait_for_messages(Config, Stats, N) ->
    case lists:sort(
           filter_queues(Stats,
                         rabbit_ct_broker_helpers:rabbitmqctl_list(
                           Config, 0, ["list_queues", "name", "messages", "messages_ready",
                                       "messages_unacknowledged"]))) of
        Stats0 when Stats0 == Stats ->
            ok;
        _ ->
            timer:sleep(?WFM_SLEEP),
            wait_for_messages(Config, Stats, N - 1)
    end.

wait_for_min_messages(Config, Queue, Msgs) ->
    wait_for_min_messages(Config, Queue, Msgs, ?WFM_DEFAULT_NUMS).

wait_for_min_messages(Config, Queue, Msgs, 0) ->
    [[_, Got]] = filter_queues([[Queue, Msgs]],
                               rabbit_ct_broker_helpers:rabbitmqctl_list(
                                 Config, 0, ["list_queues", "name", "messages"])),
    ct:pal("Got ~tp messages on queue ~tp", [Got, Queue]),
    ?assert(binary_to_integer(Got) >= Msgs);
wait_for_min_messages(Config, Queue, Msgs, N) ->
    case filter_queues([[Queue, Msgs]],
                       rabbit_ct_broker_helpers:rabbitmqctl_list(
                         Config, 0, ["list_queues", "name", "messages"])) of
        [[_, Msgs0]] ->
            case (binary_to_integer(Msgs0) >= Msgs) of
                true ->
                    ok;
                false ->
                    timer:sleep(?WFM_SLEEP),
                    wait_for_min_messages(Config, Queue, Msgs, N - 1)
            end;
        _ ->
            timer:sleep(?WFM_SLEEP),
            wait_for_min_messages(Config, Queue, Msgs, N - 1)
    end.

wait_for_max_messages(Config, Queue, Msgs) ->
    wait_for_max_messages(Config, Queue, Msgs, ?WFM_DEFAULT_NUMS).

wait_for_max_messages(Config, Queue, Msgs, 0) ->
    [[_, Got]] = filter_queues([[Queue, Msgs]],
                               rabbit_ct_broker_helpers:rabbitmqctl_list(
                                 Config, 0, ["list_queues", "name", "messages"])),
    ct:pal("Got ~tp messages on queue ~tp", [Got, Queue]),
    ?assert(binary_to_integer(Got) =< Msgs);
wait_for_max_messages(Config, Queue, Msgs, N) ->
    case filter_queues([[Queue, Msgs]],
                       rabbit_ct_broker_helpers:rabbitmqctl_list(
                         Config, 0, ["list_queues", "name", "messages"])) of
        [[_, Msgs0]] ->
            case (binary_to_integer(Msgs0) =< Msgs) of
                true ->
                    ok;
                false ->
                    timer:sleep(?WFM_SLEEP),
                    wait_for_max_messages(Config, Queue, Msgs, N - 1)
            end;
        _ ->
            timer:sleep(?WFM_SLEEP),
            wait_for_max_messages(Config, Queue, Msgs, N - 1)
    end.

dirty_query(Servers, QName, Fun) ->
    lists:map(
      fun(N) ->
              case rpc:call(N, ra, local_query, [{QName, N}, Fun]) of
                  {ok, {_, Msgs}, _} ->
                      Msgs;
                  E ->
                      ct:log(error, "~s:~s rpc:call ra:local_query failed with ~p", [?MODULE, ?FUNCTION_NAME, E]),
                      undefined
              end
      end, Servers).

ra_name(Q) ->
    binary_to_atom(<<"%2F_", Q/binary>>, utf8).

filter_queues(Expected, Got) ->
    Keys = [hd(E) || E <- Expected],
    lists:filter(fun(G) ->
                         lists:member(hd(G), Keys)
                 end, Got).

ra_machines_use_same_version(MachineModule, Config, Nodenames)
  when length(Nodenames) >= 1 ->
    [MachineAVersion | OtherMachinesVersions] =
    [(catch rabbit_ct_broker_helpers:rpc(
              Config, Nodename,
              MachineModule, version, []))
     || Nodename <- Nodenames],
    lists:all(fun(V) -> V =:= MachineAVersion end, OtherMachinesVersions).

wait_for_local_stream_member(Node, Vhost, QNameBin, Config) ->
    QName = rabbit_misc:queue_resource(Vhost, QNameBin),
    rabbit_ct_helpers:await_condition(
      fun() ->
              rabbit_ct_broker_helpers:rpc(
                Config, Node, ?MODULE, has_local_stream_member_rpc, [QName])
      end, 60_000).

has_local_stream_member_rpc(QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            #{name := StreamId} = amqqueue:get_type_state(Q),
            case rabbit_stream_coordinator:local_pid(StreamId) of
                {ok, Pid} ->
                    is_process_alive(Pid);
                {error, _} ->
                    false
            end;
        {error, _} ->
            false
    end.
