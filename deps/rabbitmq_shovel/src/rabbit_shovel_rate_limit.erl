%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% @doc A token-bucket byte-rate limiter used to cap how fast a Shovel
%% forwards messages to its destination (e.g. `dest-max-bytes-per-second`,
%% `max_bytes_per_second`).
%%
%% This module is intentionally pure/stateless-in-the-OTP-sense: it knows
%% nothing about connections, gen_server state, or timers. Callers record
%% the size of each message they are about to forward and get back either
%% permission to send now, or the number of milliseconds they must wait
%% before it would be within budget to send. Pacing (e.g. via
%% `erlang:send_after/3`) and queueing of deferred messages is the
%% caller's responsibility -- the AMQP 0.9.1 Shovel already has a
%% "pending" queue and a `blocked_by` set for flow-control and
%% `connection.blocked`; a rate-limit block is just one more reason to be
%% blocked, using the exact same machinery.
-module(rabbit_shovel_rate_limit).

-export([init/1, update_rate/2, record/2]).

-record(?MODULE, {
          %% Configured limit, in bytes/second. `undefined` means "no
          %% limit": record/2 then always immediately allows the send.
          max_bytes_per_second :: pos_integer() | undefined,
          %% Bucket contents, in bytes. This can go negative, which
          %% represents a "debt" that must be worked off (by the passage
          %% of real time) before another send is allowed.
          tokens :: number(),
          %% erlang:monotonic_time(millisecond) tokens were last topped up.
          last_ts :: integer()
        }).

-opaque state() :: #?MODULE{}.
-export_type([state/0]).

-spec init(pos_integer() | undefined) -> state().
init(undefined) ->
    #?MODULE{max_bytes_per_second = undefined, tokens = 0, last_ts = now_ms()};
init(MaxBytesPerSecond) when is_integer(MaxBytesPerSecond), MaxBytesPerSecond > 0 ->
    #?MODULE{max_bytes_per_second = MaxBytesPerSecond,
             %% Start with a full bucket (one second's worth of budget)
             %% so a freshly (re)started Shovel doesn't have to "warm up"
             %% before it can send its first message; it can still burst
             %% up to the configured rate immediately, which is the
             %% conventional token-bucket behaviour.
             tokens = MaxBytesPerSecond * 1.0,
             last_ts = now_ms()}.

%% @doc Change the configured rate at runtime, e.g. because a dynamic
%% Shovel's parameter was updated. Keeps the current token balance so an
%% in-progress throttle isn't reset by an unrelated config reload.
-spec update_rate(pos_integer() | undefined, state()) -> state().
update_rate(MaxBytesPerSecond, #?MODULE{tokens = Tokens}) ->
    (init(MaxBytesPerSecond))#?MODULE{tokens = Tokens}.

%% @doc Record that a message of `Bytes` bytes is about to be forwarded,
%% and find out whether the caller may send it right now.
%%
%% Returns `{ok, State}` when sending immediately keeps the Shovel within
%% its configured rate (or no rate is configured at all).
%%
%% Returns `{throttle, DelayMs, State}` when sending immediately would
%% exceed the configured rate. `Bytes` has already been debited from the
%% bucket (going negative) to reserve this message's place in the
%% schedule, so the caller must send this exact message, unconditionally
%% and without calling record/2 for it again, no sooner than `DelayMs`
%% milliseconds from now -- e.g. by queueing it and arming
%% `erlang:send_after(DelayMs, self(), Msg)`.
-spec record(non_neg_integer(), state()) ->
    {ok, state()} | {throttle, non_neg_integer(), state()}.
record(_Bytes, #?MODULE{max_bytes_per_second = undefined} = State) ->
    {ok, State};
record(Bytes, #?MODULE{max_bytes_per_second = Rate,
                       tokens = Tokens0,
                       last_ts = LastTs} = State)
  when is_integer(Bytes), Bytes >= 0 ->
    Now = now_ms(),
    ElapsedMs = erlang:max(0, Now - LastTs),
    %% Top up the bucket based on elapsed time, capped at one second's
    %% worth so a long idle period can't be "cashed in" as one huge burst.
    Refilled = min(Rate * 1.0, Tokens0 + (Rate * ElapsedMs / 1000)),
    Remaining = Refilled - Bytes,
    State1 = State#?MODULE{tokens = Remaining, last_ts = Now},
    case Remaining >= 0 of
        true ->
            {ok, State1};
        false ->
            {throttle, delay_for(-Remaining, Rate), State1}
    end.

%% Milliseconds needed for the bucket to refill by DeficitBytes at Rate
%% bytes/second, rounded up: we would rather wait a fraction of a
%% millisecond too long than let a message through before its tokens
%% have actually accrued.
delay_for(DeficitBytes, Rate) ->
    MsFloat = DeficitBytes * 1000 / Rate,
    Ms = erlang:trunc(MsFloat),
    case MsFloat > Ms of
        true  -> Ms + 1;
        false -> Ms
    end.

now_ms() ->
    erlang:monotonic_time(millisecond).
