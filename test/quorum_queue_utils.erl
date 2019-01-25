-module(quorum_queue_utils).

-include_lib("eunit/include/eunit.hrl").

-export([
         wait_for_messages_ready/3,
         wait_for_messages_pending_ack/3,
         wait_for_messages_total/3,
         dirty_query/3,
         ra_name/1
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
    Totals = lists:map(fun(M) when is_map(M) ->
                               maps:size(M);
                          (_) ->
                               -1
                       end, Msgs),
    ?assertEqual(Totals, [Number || _ <- lists:seq(1, length(Servers))]);
wait_for_messages(Servers, QName, Number, Fun, N) ->
    Msgs = dirty_query(Servers, QName, Fun),
    ct:pal("Got messages ~p", [Msgs]),
    case lists:all(fun(C) when is_integer(C) ->
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

