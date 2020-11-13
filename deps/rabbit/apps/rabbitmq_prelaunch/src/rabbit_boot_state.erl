%%%-------------------------------------------------------------------
%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2019-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_boot_state).

-include_lib("eunit/include/eunit.hrl").

-export([get/0,
         set/1,
         wait_for/2]).

-define(PT_KEY_BOOT_STATE,    {?MODULE, boot_state}).

-type boot_state() :: 'stopped' | 'booting' | 'ready' | 'stopping'.

-export_type([boot_state/0]).

-spec get() -> boot_state().
get() ->
    persistent_term:get(?PT_KEY_BOOT_STATE, stopped).

-spec set(boot_state()) -> ok.
set(BootState) ->
    rabbit_log_prelaunch:debug("Change boot state to `~s`", [BootState]),
    ?assert(is_valid(BootState)),
    case BootState of
        stopped -> persistent_term:erase(?PT_KEY_BOOT_STATE);
        _       -> persistent_term:put(?PT_KEY_BOOT_STATE, BootState)
    end,
    rabbit_boot_state_sup:notify_boot_state_listeners(BootState).

-spec wait_for(boot_state(), timeout()) -> ok | {error, timeout}.
wait_for(BootState, infinity) ->
    case is_reached(BootState) of
        true  -> ok;
        false -> Wait = 200,
                 timer:sleep(Wait),
                 wait_for(BootState, infinity)
    end;
wait_for(BootState, Timeout)
  when is_integer(Timeout) andalso Timeout >= 0 ->
    case is_reached(BootState) of
        true  -> ok;
        false -> Wait = 200,
                 timer:sleep(Wait),
                 wait_for(BootState, Timeout - Wait)
    end;
wait_for(_, _) ->
    {error, timeout}.

boot_state_idx(stopped)  -> 0;
boot_state_idx(booting)  -> 1;
boot_state_idx(ready)    -> 2;
boot_state_idx(stopping) -> 3.

is_valid(BootState) ->
    is_integer(boot_state_idx(BootState)).

is_reached(TargetBootState) ->
    is_reached(?MODULE:get(), TargetBootState).

is_reached(CurrentBootState, CurrentBootState) ->
    true;
is_reached(stopping, stopped) ->
    false;
is_reached(_CurrentBootState, stopped) ->
    true;
is_reached(stopped, _TargetBootState) ->
    true;
is_reached(CurrentBootState, TargetBootState) ->
    boot_state_idx(TargetBootState) =< boot_state_idx(CurrentBootState).
