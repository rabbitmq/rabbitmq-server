%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(amqqueue_v1).

-include_lib("rabbit_common/include/resource.hrl").

-export([new/8,
         new_with_version/9,
         fields/0,
         fields/1,
         field_vhost/0,
         upgrade_to/2,
         % arguments
         get_arguments/1,
         set_arguments/2,
         % decorators
         get_decorators/1,
         set_decorators/2,
         % exclusive_owner
         get_exclusive_owner/1,
         % gm_pids
         get_gm_pids/1,
         set_gm_pids/2,
         % name
         get_name/1,
         set_name/2,
         % operator_policy
         get_operator_policy/1,
         set_operator_policy/2,
         get_options/1,
         % pid
         get_pid/1,
         set_pid/2,
         % policy
         get_policy/1,
         set_policy/2,
         % policy_version
         get_policy_version/1,
         set_policy_version/2,
         % recoverable_slaves
         get_recoverable_slaves/1,
         set_recoverable_slaves/2,
         % slave_pids
         get_slave_pids/1,
         set_slave_pids/2,
         % slave_pids_pending_shutdown
         get_slave_pids_pending_shutdown/1,
         set_slave_pids_pending_shutdown/2,
         % state
         get_state/1,
         set_state/2,
         % sync_slave_pids
         get_sync_slave_pids/1,
         set_sync_slave_pids/2,
         get_vhost/1,
         is_amqqueue/1,
         is_auto_delete/1,
         is_durable/1,
         pattern_match_all/0,
         pattern_match_on_name/1,
         reset_mirroring_and_decorators/1,
         set_immutable/1,
         macros/0]).

-define(record_version, ?MODULE).

% TODO #160169569 what about dialyzer types?

-record(amqqueue, {
          name, durable, auto_delete, exclusive_owner = none, %% immutable
          arguments,                   %% immutable
          pid,                         %% durable (just so we know home node)
          slave_pids, sync_slave_pids, %% transient
          recoverable_slaves,          %% durable
          policy,                      %% durable, implicit update as above
          operator_policy,             %% durable, implicit update as above
          gm_pids,                     %% transient
          decorators,                  %% transient, recalculated as above
          state,                       %% durable (have we crashed?)
          policy_version,
          slave_pids_pending_shutdown,
          vhost,                       %% secondary index
          options = #{}}).

new(Name,
    Pid,
    Durable,
    AutoDelete,
    Owner,
    Args,
    VHost,
    Options) ->
    new_with_version(
      ?record_version,
      Name,
      Pid,
      Durable,
      AutoDelete,
      Owner,
      Args,
      VHost,
      Options).

new_with_version(?record_version,
                 Name,
                 Pid,
                 Durable,
                 AutoDelete,
                 Owner,
                 Args,
                 VHost,
                 Options) ->
    #amqqueue{name               = Name,
              durable            = Durable,
              auto_delete        = AutoDelete,
              arguments          = Args,
              exclusive_owner    = Owner,
              pid                = Pid,
              slave_pids         = [],
              sync_slave_pids    = [],
              recoverable_slaves = [],
              gm_pids            = [],
              state              = live,
              policy_version     = 0,
              slave_pids_pending_shutdown = [],
              vhost              = VHost,
              options            = Options}.

is_amqqueue(#amqqueue{}) -> true;
is_amqqueue(_)           -> false.

upgrade_to(?record_version, #amqqueue{} = Queue) ->
    Queue.

% arguments

get_arguments(#amqqueue{arguments = Args}) -> Args.

set_arguments(#amqqueue{} = Queue, Args) ->
    Queue#amqqueue{arguments = Args}.

% decorators

get_decorators(#amqqueue{decorators = Decorators}) -> Decorators.

set_decorators(#amqqueue{} = Queue, Decorators) ->
    Queue#amqqueue{decorators = Decorators}.

get_exclusive_owner(#amqqueue{exclusive_owner = Owner}) -> Owner.

% gm_pids

get_gm_pids(#amqqueue{gm_pids = GMPids}) -> GMPids.

set_gm_pids(#amqqueue{} = Queue, GMPids) ->
    Queue#amqqueue{gm_pids = GMPids}.

% name

get_name(#amqqueue{name = Name}) -> Name.

set_name(#amqqueue{} = Queue, Name) ->
    Queue#amqqueue{name = Name}.

% operator_policy

get_operator_policy(#amqqueue{operator_policy = OpPolicy}) -> OpPolicy.

set_operator_policy(#amqqueue{} = Queue, OpPolicy) ->
    Queue#amqqueue{operator_policy = OpPolicy}.

get_options(#amqqueue{options = Options}) -> Options.

% pid

get_pid(#amqqueue{pid = Pid}) -> Pid.

set_pid(#amqqueue{} = Queue, Pid) ->
    Queue#amqqueue{pid = Pid}.

% policy

get_policy(#amqqueue{policy = Policy}) -> Policy.

set_policy(#amqqueue{} = Queue, Policy) ->
    Queue#amqqueue{policy = Policy}.

% policy_version

get_policy_version(#amqqueue{policy_version = PV}) ->
    PV.

set_policy_version(#amqqueue{} = Queue, PV) ->
    Queue#amqqueue{policy_version = PV}.

% recoverable_slaves

get_recoverable_slaves(#amqqueue{recoverable_slaves = Slaves}) ->
    Slaves.

set_recoverable_slaves(#amqqueue{} = Queue, Slaves) ->
    Queue#amqqueue{recoverable_slaves = Slaves}.

% slave_pids

get_slave_pids(#amqqueue{slave_pids = Slaves}) ->
    Slaves.

set_slave_pids(#amqqueue{} = Queue, SlavePids) ->
    Queue#amqqueue{slave_pids = SlavePids}.

% slave_pids_pending_shutdown

get_slave_pids_pending_shutdown(#amqqueue{slave_pids_pending_shutdown = Slaves}) ->
    Slaves.

set_slave_pids_pending_shutdown(#amqqueue{} = Queue, SlavePids) ->
    Queue#amqqueue{slave_pids_pending_shutdown = SlavePids}.

% state

get_state(#amqqueue{state = State}) -> State.

set_state(#amqqueue{} = Queue, State) -> Queue#amqqueue{state = State}.

% sync_slave_pids

get_sync_slave_pids(#amqqueue{sync_slave_pids = Pids}) -> Pids.

set_sync_slave_pids(#amqqueue{} = Queue, Pids) ->
    Queue#amqqueue{sync_slave_pids = Pids}.

get_vhost(#amqqueue{vhost = VHost}) -> VHost.

is_auto_delete(#amqqueue{auto_delete = AutoDelete}) -> AutoDelete.

is_durable(#amqqueue{durable = Durable}) -> Durable.

fields() -> fields(?record_version).

fields(?record_version) -> record_info(fields, amqqueue).

field_vhost() -> #amqqueue.vhost.

pattern_match_all() ->#amqqueue{_ = '_'}.

pattern_match_on_name(Name) ->#amqqueue{name = Name, _ = '_'}.

reset_mirroring_and_decorators(#amqqueue{} = Queue) ->
    Queue#amqqueue{slave_pids      = [],
                   sync_slave_pids = [],
                   gm_pids         = [],
                   decorators      = undefined}.

set_immutable(#amqqueue{} = Queue) ->
    Queue#amqqueue{pid                = none,
                   slave_pids         = none,
                   sync_slave_pids    = none,
                   recoverable_slaves = none,
                   gm_pids            = none,
                   policy             = none,
                   decorators         = none,
                   state              = none}.

macros() ->
    io:format(
      "-define(is_~s(Q), is_record(Q, amqqueue, ~b)).~n~n",
      [?record_version, record_info(size, amqqueue)]),
    %% The field number starts at 2 because the first element is the
    %% record name.
    macros(record_info(fields, amqqueue), 2).

macros([Field | Rest], I) ->
    io:format(
      "-define(~s_field_~s(Q), element(~b, Q)).~n",
      [?record_version, Field, I]),
    macros(Rest, I + 1);
macros([], _) ->
    ok.
