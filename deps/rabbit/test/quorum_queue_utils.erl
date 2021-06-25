-module(quorum_queue_utils).

-include_lib("eunit/include/eunit.hrl").

-export([
         wait_for_messages_ready/3,
         wait_for_messages_pending_ack/3,
         wait_for_messages_total/3,
         wait_for_messages/2,
         wait_for_messages/3,
         wait_for_min_messages/3,
         wait_for_max_messages/3,
         dirty_query/3,
         ra_name/1,
         fifo_machines_use_same_version/1,
         fifo_machines_use_same_version/2
        ]).

wait_for_messages_ready(Servers, QName, Ready) ->
    wait_for_messages(Servers, QName, Ready,
                      fun rabbit_fifo:query_messages_ready/1, 60).

wait_for_messages_pending_ack(Servers, QName, Ready) ->
    wait_for_messages(Servers, QName, Ready,
                      fun rabbit_fifo:query_messages_checked_out/1, 60).

wait_for_messages_total(Servers, QName, Total) ->
    wait_for_messages(Servers, QName, Total,
                      fun rabbit_fifo:query_messages_total/1, 60).

wait_for_messages(Servers, QName, Number, Fun, 0) ->
    Msgs = dirty_query(Servers, QName, Fun),
    ?assertEqual([Number || _ <- lists:seq(1, length(Servers))], Msgs);
wait_for_messages(Servers, QName, Number, Fun, N) ->
    Msgs = dirty_query(Servers, QName, Fun),
    ct:pal("Got messages ~p ~p", [QName, Msgs]),
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
            timer:sleep(500),
            wait_for_messages(Servers, QName, Number, Fun, N - 1)
    end.

wait_for_messages(Config, Stats) ->
    wait_for_messages(Config, lists:sort(Stats), 60).

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
            timer:sleep(500),
            wait_for_messages(Config, Stats, N - 1)
    end.

wait_for_min_messages(Config, Queue, Msgs) ->
    wait_for_min_messages(Config, Queue, Msgs, 60).

wait_for_min_messages(Config, Queue, Msgs, 0) ->
    [[_, Got]] = filter_queues([[Queue, Msgs]],
                               rabbit_ct_broker_helpers:rabbitmqctl_list(
                                 Config, 0, ["list_queues", "name", "messages"])),
    ct:pal("Got ~p messages on queue ~p", [Got, Queue]),
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
                    timer:sleep(500),
                    wait_for_min_messages(Config, Queue, Msgs, N - 1)
            end;
        _ ->
            timer:sleep(500),
            wait_for_min_messages(Config, Queue, Msgs, N - 1)
    end.

wait_for_max_messages(Config, Queue, Msgs) ->
    wait_for_max_messages(Config, Queue, Msgs, 60).

wait_for_max_messages(Config, Queue, Msgs, 0) ->
    [[_, Got]] = filter_queues([[Queue, Msgs]],
                               rabbit_ct_broker_helpers:rabbitmqctl_list(
                                 Config, 0, ["list_queues", "name", "messages"])),
    ct:pal("Got ~p messages on queue ~p", [Got, Queue]),
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
                    timer:sleep(500),
                    wait_for_max_messages(Config, Queue, Msgs, N - 1)
            end;
        _ ->
            timer:sleep(500),
            wait_for_max_messages(Config, Queue, Msgs, N - 1)
    end.

dirty_query(Servers, QName, Fun) ->
    lists:map(
      fun(N) ->
              case rpc:call(N, ra, local_query, [{QName, N}, Fun]) of
                  {ok, {_, Msgs}, _} ->
                      Msgs;
                  _E ->
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

fifo_machines_use_same_version(Config) ->
    Nodenames = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    fifo_machines_use_same_version(Config, Nodenames).

fifo_machines_use_same_version(Config, Nodenames)
  when length(Nodenames) >= 1 ->
    [MachineAVersion | OtherMachinesVersions] =
    [(catch rabbit_ct_broker_helpers:rpc(
              Config, Nodename,
              rabbit_fifo, version, []))
     || Nodename <- Nodenames],
    lists:all(fun(V) -> V =:= MachineAVersion end, OtherMachinesVersions).
