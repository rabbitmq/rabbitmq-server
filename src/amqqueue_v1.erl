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
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(amqqueue_v1).

-include_lib("rabbit_common/include/resource.hrl").

-export([new/8,
         new_with_version/9,
         fields/0,
         fields/1,
         field_vhost/0,
         record_version_to_use/0,
         upgrade/1,
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

-record(amqqueue, {
          name :: rabbit_amqqueue:name() | '_', %% immutable
          durable :: boolean() | '_',           %% immutable
          auto_delete :: boolean() | '_',       %% immutable
          exclusive_owner = none :: pid() | none | '_', %% immutable
          arguments = [] :: rabbit_framing:amqp_table() | '_', %% immutable
          pid :: pid() | none | '_',            %% durable (just so we
                                                %% know home node)
          slave_pids = [] :: [pid()] | none | '_',    %% transient
          sync_slave_pids = [] :: [pid()] | none| '_',%% transient
          recoverable_slaves = [] :: [atom()] | none | '_', %% durable
          policy :: binary() | none | undefined | '_', %% durable, implicit
                                                       %% update as above
          operator_policy :: binary() | none | undefined | '_', %% durable,
                                                                %% implicit
                                                                %% update
                                                                %% as above
          gm_pids = [] :: [pid()] | none | '_', %% transient
          decorators :: [atom()] | none | undefined | '_', %% transient,
                                                          %% recalculated
                                                          %% as above
          state = live :: atom() | none | '_', %% durable (have we crashed?)
          policy_version = 0 :: non_neg_integer() | '_',
          slave_pids_pending_shutdown = [] :: [pid()] | '_',
          vhost :: rabbit_types:vhost() | undefined | '_', %% secondary index
          options = #{} :: map() | '_'
         }).

-type amqqueue() :: amqqueue_v1().
-type amqqueue_v1() :: #amqqueue{
                          name :: rabbit_amqqueue:name(),
                          durable :: boolean(),
                          auto_delete :: boolean(),
                          exclusive_owner :: pid() | none,
                          arguments :: rabbit_framing:amqp_table(),
                          pid :: pid() | none,
                          slave_pids :: [pid()] | none,
                          sync_slave_pids :: [pid()] | none,
                          recoverable_slaves :: [atom()] | none,
                          policy :: binary() | none | undefined,
                          operator_policy :: binary() | none | undefined,
                          gm_pids :: [pid()] | none,
                          decorators :: [atom()] | none | undefined,
                          state :: atom() | none,
                          policy_version :: non_neg_integer(),
                          slave_pids_pending_shutdown :: [pid()],
                          vhost :: rabbit_types:vhost() | undefined,
                          options :: map()
                         }.

-type amqqueue_pattern() :: amqqueue_v1_pattern().
-type amqqueue_v1_pattern() :: #amqqueue{
                                  name :: rabbit_amqqueue:name() | '_',
                                  durable :: '_',
                                  auto_delete :: '_',
                                  exclusive_owner :: '_',
                                  arguments :: '_',
                                  pid :: '_',
                                  slave_pids :: '_',
                                  sync_slave_pids :: '_',
                                  recoverable_slaves :: '_',
                                  policy :: '_',
                                  operator_policy :: '_',
                                  gm_pids :: '_',
                                  decorators :: '_',
                                  state :: '_',
                                  policy_version :: '_',
                                  slave_pids_pending_shutdown :: '_',
                                  vhost :: '_',
                                  options :: '_'
                                 }.

-export_type([amqqueue/0,
              amqqueue_v1/0,
              amqqueue_pattern/0,
              amqqueue_v1_pattern/0]).

-spec new(rabbit_amqqueue:name(),
          pid() | none,
          boolean(),
          boolean(),
          pid() | none,
          rabbit_framing:amqp_table(),
          rabbit_types:vhost() | undefined,
          map()) -> amqqueue().

new(#resource{kind = queue} = Name,
    Pid,
    Durable,
    AutoDelete,
    Owner,
    Args,
    VHost,
    Options)
  when (is_pid(Pid) orelse Pid =:= none) andalso
       is_boolean(Durable) andalso
       is_boolean(AutoDelete) andalso
       (is_pid(Owner) orelse Owner =:= none) andalso
       is_list(Args) andalso
       (is_binary(VHost) orelse VHost =:= undefined) andalso
       is_map(Options) ->
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

-spec new_with_version(amqqueue_v1,
                       rabbit_amqqueue:name(),
                       pid() | none,
                       boolean(),
                       boolean(),
                       pid() | none,
                       rabbit_framing:amqp_table(),
                       rabbit_types:vhost() | undefined,
                       map()) -> amqqueue().

new_with_version(?record_version,
                 #resource{kind = queue} = Name,
                 Pid,
                 Durable,
                 AutoDelete,
                 Owner,
                 Args,
                 VHost,
                 Options)
  when (is_pid(Pid) orelse Pid =:= none) andalso
       is_boolean(Durable) andalso
       is_boolean(AutoDelete) andalso
       (is_pid(Owner) orelse Owner =:= none) andalso
       is_list(Args) andalso
       (is_binary(VHost) orelse VHost =:= undefined) andalso
       is_map(Options) ->
    #amqqueue{name            = Name,
              durable         = Durable,
              auto_delete     = AutoDelete,
              arguments       = Args,
              exclusive_owner = Owner,
              pid             = Pid,
              vhost           = VHost,
              options         = Options}.

-spec is_amqqueue(any()) -> boolean().

is_amqqueue(#amqqueue{}) -> true;
is_amqqueue(_)           -> false.

-spec record_version_to_use() -> amqqueue_v1.

record_version_to_use() ->
    ?record_version.

-spec upgrade(amqqueue()) -> amqqueue().

upgrade(#amqqueue{} = Queue) ->
    Queue.

-spec upgrade_to(amqqueue_v1, amqqueue()) -> amqqueue().

upgrade_to(?record_version, #amqqueue{} = Queue) ->
    Queue.

% arguments

-spec get_arguments(amqqueue()) -> rabbit_framing:amqp_table().

get_arguments(#amqqueue{arguments = Args}) -> Args.

-spec set_arguments(amqqueue(), rabbit_framing:amqp_table()) -> amqqueue().

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

-spec pattern_match_all() -> amqqueue_pattern().

pattern_match_all() -> #amqqueue{_ = '_'}.

-spec pattern_match_on_name(rabbit_amqqueue:name()) ->
    amqqueue_pattern().

pattern_match_on_name(Name) -> #amqqueue{name = Name, _ = '_'}.

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
