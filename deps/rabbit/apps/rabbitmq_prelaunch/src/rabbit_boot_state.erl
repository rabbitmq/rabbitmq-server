%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_boot_state).

-include_lib("eunit/include/eunit.hrl").

-export([get/0,
         set/1,
         wait_for/2,
         has_reached/1,
         has_reached_and_is_active/1]).

-define(PT_KEY_BOOT_STATE,    {?MODULE, boot_state}).

-type boot_state() :: 'stopped' | 'booting' | 'core_started' | 'ready' | 'stopping'.

-export_type([boot_state/0]).

-spec get() -> boot_state().
get() ->
    persistent_term:get(?PT_KEY_BOOT_STATE, stopped).

-spec set(boot_state()) -> ok.
set(BootState) ->
    _ = rabbit_log_prelaunch:debug("Change boot state to `~s`", [BootState]),
    ?assert(is_valid(BootState)),
    case BootState of
        stopped -> persistent_term:erase(?PT_KEY_BOOT_STATE);
        _       -> persistent_term:put(?PT_KEY_BOOT_STATE, BootState)
    end,
    rabbit_boot_state_sup:notify_boot_state_listeners(BootState).

-spec wait_for(boot_state(), timeout()) -> ok | {error, timeout}.
wait_for(BootState, infinity) ->
    case has_reached(BootState) of
        true  -> ok;
        false -> Wait = 200,
                 timer:sleep(Wait),
                 wait_for(BootState, infinity)
    end;
wait_for(BootState, Timeout)
  when is_integer(Timeout) andalso Timeout >= 0 ->
    case has_reached(BootState) of
        true  -> ok;
        false -> Wait = 200,
                 timer:sleep(Wait),
                 wait_for(BootState, Timeout - Wait)
    end;
wait_for(_, _) ->
    {error, timeout}.

boot_state_idx(stopped)      -> 0;
boot_state_idx(booting)      -> 1;
boot_state_idx(core_started) -> 2;
boot_state_idx(ready)        -> 3;
boot_state_idx(stopping)     -> 4.

is_valid(BootState) ->
    is_integer(boot_state_idx(BootState)).

has_reached(TargetBootState) ->
    has_reached(?MODULE:get(), TargetBootState).

has_reached(CurrentBootState, CurrentBootState) ->
    true;
has_reached(stopping, stopped) ->
    false;
has_reached(_CurrentBootState, stopped) ->
    true;
has_reached(stopped, _TargetBootState) ->
    true;
has_reached(CurrentBootState, TargetBootState) ->
    boot_state_idx(TargetBootState) =< boot_state_idx(CurrentBootState).

has_reached_and_is_active(TargetBootState) ->
    case ?MODULE:get() of
        stopped ->
            false;
        CurrentBootState ->
            has_reached(CurrentBootState, TargetBootState)
            andalso
            not has_reached(CurrentBootState, stopping)
    end.
