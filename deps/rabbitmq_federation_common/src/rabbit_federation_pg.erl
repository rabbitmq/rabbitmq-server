%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
<<<<<<< HEAD
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
=======
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
>>>>>>> 512553e09 (Stop federation supervision hierachies before stopping `rabbit`)
%%

-module(rabbit_federation_pg).

-include_lib("kernel/include/logger.hrl").

-export([start_scope/1, stop_scope/1, terminate_all_local_members/2, terminate_all_local_members/4]).

start_scope(Scope) ->
  ?LOG_DEBUG("Starting pg scope ~ts", [Scope]),
  _ = pg:start_link(Scope).

stop_scope(Scope) ->
  case whereis(Scope) of
      Pid when is_pid(Pid) ->
          ?LOG_DEBUG("Stopping pg scope ~ts", [Scope]),
          exit(Pid, normal);
      _ ->
          ok
  end.

-spec terminate_all_local_members(Scope :: atom(), Timeout :: non_neg_integer()) -> ok.
terminate_all_local_members(Scope, Timeout) ->
    terminate_all_local_members(Scope, Timeout, infinity, 0).

-spec terminate_all_local_members(Scope :: atom(), Timeout :: non_neg_integer(),
                                  BatchSize :: pos_integer() | infinity,
                                  ThrottleDelay :: non_neg_integer()) -> ok.
terminate_all_local_members(Scope, Timeout, BatchSize, ThrottleDelay) ->
    Groups = try pg:which_groups(Scope) catch error:badarg -> [] end,
    GetMembers = fun(G) ->
        try pg:get_local_members(Scope, G) catch error:badarg -> [] end
    end,
    AllMembers = lists:usort(lists:flatmap(GetMembers, Groups)),
    case AllMembers of
        [] -> ok;
        _  -> do_terminate_all(AllMembers, Timeout, BatchSize, ThrottleDelay)
    end.

do_terminate_all(Members, Timeout, BatchSize, ThrottleDelay) ->
    Count = length(Members),
    ?LOG_DEBUG("Federation: initiating paced shutdown for ~b link(s) "
               "(batch_size=~p, throttle_delay=~bms, timeout=~bms)",
               [Count, BatchSize, ThrottleDelay, Timeout]),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    Batches = make_batches(Members, BatchSize),
    terminate_batches(Batches, ThrottleDelay, Deadline).

make_batches(Members, infinity) ->
    [Members];
make_batches(Members, BatchSize) when is_integer(BatchSize), BatchSize > 0 ->
    make_batches_loop(Members, BatchSize, []).

make_batches_loop([], _, Acc) ->
    lists:reverse(Acc);
make_batches_loop(Members, BatchSize, Acc) ->
    {Batch, Rest} = lists:split(min(BatchSize, length(Members)), Members),
    make_batches_loop(Rest, BatchSize, [Batch | Acc]).

terminate_batches([], _, _) ->
    ok;
terminate_batches([Batch | Rest], ThrottleDelay, Deadline) ->
    Remaining = max(0, Deadline - erlang:monotonic_time(millisecond)),
    case Remaining of
        0 ->
            force_kill_all([Batch | Rest]);
        _ ->
            Monitors = maps:from_list([{monitor(process, P), P} || P <- Batch]),
            [exit(P, shutdown) || P <- Batch],
            case wait_batch_down(Monitors, Deadline) of
                ok ->
                    case Rest of
                        [] -> ok;
                        _  ->
                            timer:sleep(ThrottleDelay),
                            terminate_batches(Rest, ThrottleDelay, Deadline)
                    end;
                {timeout, Remaining2} ->
                    force_kill_all(Remaining2 ++ Rest)
            end
    end.

force_kill_all(Batches) ->
    AllRemaining = lists:append(Batches),
    Count = length(AllRemaining),
    case Count of
        0 -> ok;
        _ ->
            ?LOG_WARNING("Federation: paced shutdown timed out, "
                         "force killing ~b remaining link(s)", [Count]),
            [exit(P, kill) || P <- AllRemaining],
            ok
    end.

wait_batch_down(Monitors, _) when map_size(Monitors) =:= 0 ->
    ok;
wait_batch_down(Monitors, Deadline) ->
    Remaining = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {'DOWN', MRef, process, _Pid, _Reason} ->
            wait_batch_down(maps:remove(MRef, Monitors), Deadline)
    after Remaining ->
        [demonitor(MRef, [flush]) || MRef <- maps:keys(Monitors)],
        {timeout, [maps:values(Monitors)]}
    end.
