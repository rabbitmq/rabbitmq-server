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

-module(amqqueue). %% Could become amqqueue_v2 in the future.

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([new/9,
         new_with_version/10,
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
         get_leader/1,
         % name (#resource)
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
         % quorum_nodes
         get_quorum_nodes/1,
         set_quorum_nodes/2,
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
         get_type/1,
         get_vhost/1,
         is_amqqueue/1,
         is_auto_delete/1,
         is_durable/1,
         is_classic/1,
         is_quorum/1,
         pattern_match_all/0,
         pattern_match_on_name/1,
         pattern_match_on_type/1,
         reset_mirroring_and_decorators/1,
         set_immutable/1,
         qnode/1,
         macros/0]).

-define(record_version, amqqueue_v2).

-record(amqqueue, {
          name :: rabbit_amqqueue:name() | '_', %% immutable
          durable :: boolean() | '_',           %% immutable
          auto_delete :: boolean() | '_',       %% immutable
          exclusive_owner = none :: pid() | none | '_', %% immutable
          arguments = [] :: rabbit_framing:amqp_table() | '_', %% immutable
          pid :: pid() | ra_server_id() | none | '_', %% durable (just so we
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
          gm_pids = [] :: [{pid(), pid()} | pid()] | none | '_', %% transient
          decorators :: [atom()] | none | undefined | '_', %% transient,
                                                          %% recalculated
                                                          %% as above
          state = live :: atom() | none | '_', %% durable (have we crashed?)
          policy_version = 0 :: non_neg_integer() | '_',
          slave_pids_pending_shutdown = [] :: [pid()] | '_',
          vhost :: rabbit_types:vhost() | undefined | '_', %% secondary index
          options = #{} :: map() | '_',
          type = ?amqqueue_v1_type :: atom() | '_',
          quorum_nodes = [] :: [node()] | '_'
         }).

-type amqqueue() :: amqqueue_v1:amqqueue_v1() | amqqueue_v2().
-type amqqueue_v2() :: #amqqueue{
                          name :: rabbit_amqqueue:name(),
                          durable :: boolean(),
                          auto_delete :: boolean(),
                          exclusive_owner :: pid() | none,
                          arguments :: rabbit_framing:amqp_table(),
                          pid :: pid() | ra_server_id() | none,
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
                          options :: map(),
                          type :: atom(),
                          quorum_nodes :: [node()]
                         }.

-type ra_server_id() :: {Name :: atom(), Node :: node()}.

-type amqqueue_pattern() :: amqqueue_v1:amqqueue_v1_pattern() |
                            amqqueue_v2_pattern().
-type amqqueue_v2_pattern() :: #amqqueue{
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
                                  options :: '_',
                                  type :: atom() | '_',
                                  quorum_nodes :: '_'
                                 }.

-export_type([amqqueue/0,
              amqqueue_v2/0,
              amqqueue_pattern/0,
              amqqueue_v2_pattern/0,
              ra_server_id/0]).

-spec new(rabbit_amqqueue:name(),
          pid() | ra_server_id() | none,
          boolean(),
          boolean(),
          pid() | none,
          rabbit_framing:amqp_table(),
          rabbit_types:vhost() | undefined,
          map(),
          atom()) -> amqqueue().

new(#resource{kind = queue} = Name,
    Pid,
    Durable,
    AutoDelete,
    Owner,
    Args,
    VHost,
    Options,
    Type)
  when (is_pid(Pid) orelse is_tuple(Pid) orelse Pid =:= none) andalso
       is_boolean(Durable) andalso
       is_boolean(AutoDelete) andalso
       (is_pid(Owner) orelse Owner =:= none) andalso
       is_list(Args) andalso
       (is_binary(VHost) orelse VHost =:= undefined) andalso
       is_map(Options) andalso
       is_atom(Type) ->
    case record_version_to_use() of
        ?record_version ->
            new_with_version(
              ?record_version,
              Name,
              Pid,
              Durable,
              AutoDelete,
              Owner,
              Args,
              VHost,
              Options,
              Type);
        _ ->
            amqqueue_v1:new(
              Name,
              Pid,
              Durable,
              AutoDelete,
              Owner,
              Args,
              VHost,
              Options)
    end.

-spec new_with_version
(amqqueue_v1 | amqqueue_v2,
 rabbit_amqqueue:name(),
 pid() | ra_server_id() | none,
 boolean(),
 boolean(),
 pid() | none,
 rabbit_framing:amqp_table(),
 rabbit_types:vhost() | undefined,
 map(),
 atom()) -> amqqueue().

new_with_version(?record_version,
                 #resource{kind = queue} = Name,
                 Pid,
                 Durable,
                 AutoDelete,
                 Owner,
                 Args,
                 VHost,
                 Options,
                 Type)
  when (is_pid(Pid) orelse is_tuple(Pid) orelse Pid =:= none) andalso
       is_boolean(Durable) andalso
       is_boolean(AutoDelete) andalso
       (is_pid(Owner) orelse Owner =:= none) andalso
       is_list(Args) andalso
       (is_binary(VHost) orelse VHost =:= undefined) andalso
       is_map(Options) andalso
       is_atom(Type) ->
    #amqqueue{name            = Name,
              durable         = Durable,
              auto_delete     = AutoDelete,
              arguments       = Args,
              exclusive_owner = Owner,
              pid             = Pid,
              vhost           = VHost,
              options         = Options,
              type            = Type};
new_with_version(Version,
                 Name,
                 Pid,
                 Durable,
                 AutoDelete,
                 Owner,
                 Args,
                 VHost,
                 Options,
                 ?amqqueue_v1_type) ->
    amqqueue_v1:new_with_version(
      Version,
      Name,
      Pid,
      Durable,
      AutoDelete,
      Owner,
      Args,
      VHost,
      Options).

-spec is_amqqueue(any()) -> boolean().

is_amqqueue(#amqqueue{}) -> true;
is_amqqueue(Queue)       -> amqqueue_v1:is_amqqueue(Queue).

-spec record_version_to_use() -> amqqueue_v1 | amqqueue_v2.

record_version_to_use() ->
    case rabbit_feature_flags:is_enabled(quorum_queue) of
        true  -> ?record_version;
        false -> amqqueue_v1:record_version_to_use()
    end.

-spec upgrade(amqqueue()) -> amqqueue().

upgrade(#amqqueue{} = Queue) -> Queue;
upgrade(OldQueue)            -> upgrade_to(record_version_to_use(), OldQueue).

-spec upgrade_to
(amqqueue_v2, amqqueue()) -> amqqueue_v2();
(amqqueue_v1, amqqueue_v1:amqqueue_v1()) -> amqqueue_v1:amqqueue_v1().

upgrade_to(?record_version, #amqqueue{} = Queue) ->
    Queue;
upgrade_to(?record_version, OldQueue) ->
    Fields = erlang:tuple_to_list(OldQueue) ++ [?amqqueue_v1_type,
                                                undefined],
    #amqqueue{} = erlang:list_to_tuple(Fields);
upgrade_to(Version, OldQueue) ->
    amqqueue_v1:upgrade_to(Version, OldQueue).

% arguments

-spec get_arguments(amqqueue()) -> rabbit_framing:amqp_table().

get_arguments(#amqqueue{arguments = Args}) ->
    Args;
get_arguments(Queue) ->
    amqqueue_v1:get_arguments(Queue).

-spec set_arguments(amqqueue(), rabbit_framing:amqp_table()) -> amqqueue().

set_arguments(#amqqueue{} = Queue, Args) ->
    Queue#amqqueue{arguments = Args};
set_arguments(Queue, Args) ->
    amqqueue_v1:set_arguments(Queue, Args).

% decorators

-spec get_decorators(amqqueue()) -> [atom()] | none | undefined.

get_decorators(#amqqueue{decorators = Decorators}) ->
    Decorators;
get_decorators(Queue) ->
    amqqueue_v1:get_decorators(Queue).

-spec set_decorators(amqqueue(), [atom()] | none | undefined) -> amqqueue().

set_decorators(#amqqueue{} = Queue, Decorators) ->
    Queue#amqqueue{decorators = Decorators};
set_decorators(Queue, Decorators) ->
    amqqueue_v1:set_decorators(Queue, Decorators).

-spec get_exclusive_owner(amqqueue()) -> pid() | none.

get_exclusive_owner(#amqqueue{exclusive_owner = Owner}) ->
    Owner;
get_exclusive_owner(Queue) ->
    amqqueue_v1:get_exclusive_owner(Queue).

-spec get_gm_pids(amqqueue()) -> [{pid(), pid()} | pid()] | none.

get_gm_pids(#amqqueue{gm_pids = GMPids}) ->
    GMPids;
get_gm_pids(Queue) ->
    amqqueue_v1:get_gm_pids(Queue).

-spec set_gm_pids(amqqueue(), [{pid(), pid()} | pid()] | none) -> amqqueue().

set_gm_pids(#amqqueue{} = Queue, GMPids) ->
    Queue#amqqueue{gm_pids = GMPids};
set_gm_pids(Queue, GMPids) ->
    amqqueue_v1:set_gm_pids(Queue, GMPids).

-spec get_leader(amqqueue_v2()) -> node().

get_leader(#amqqueue{type = quorum, pid = {_, Leader}}) -> Leader.

% operator_policy

-spec get_operator_policy(amqqueue()) -> binary() | none | undefined.

get_operator_policy(#amqqueue{operator_policy = OpPolicy}) -> OpPolicy;
get_operator_policy(Queue) -> amqqueue_v1:get_operator_policy(Queue).

-spec set_operator_policy(amqqueue(), binary() | none | undefined) ->
    amqqueue().

set_operator_policy(#amqqueue{} = Queue, Policy) ->
    Queue#amqqueue{operator_policy = Policy};
set_operator_policy(Queue, Policy) ->
    amqqueue_v1:set_operator_policy(Queue, Policy).

% name

-spec get_name(amqqueue()) -> rabbit_amqqueue:name().

get_name(#amqqueue{name = Name}) -> Name;
get_name(Queue)                  -> amqqueue_v1:get_name(Queue).

-spec set_name(amqqueue(), rabbit_amqqueue:name()) -> amqqueue().

set_name(#amqqueue{} = Queue, Name) ->
    Queue#amqqueue{name = Name};
set_name(Queue, Name) ->
    amqqueue_v1:set_name(Queue, Name).

-spec get_options(amqqueue()) -> map().

get_options(#amqqueue{options = Options}) -> Options;
get_options(Queue)                        -> amqqueue_v1:get_options(Queue).

% pid

-spec get_pid
(amqqueue_v2()) -> pid() | ra_server_id() | none;
(amqqueue_v1:amqqueue_v1()) -> pid() | none.

get_pid(#amqqueue{pid = Pid}) -> Pid;
get_pid(Queue)                -> amqqueue_v1:get_pid(Queue).

-spec set_pid
(amqqueue_v2(),  pid() | ra_server_id() | none) -> amqqueue_v2();
(amqqueue_v1:amqqueue_v1(), pid() | none) -> amqqueue_v1:amqqueue_v1().

set_pid(#amqqueue{} = Queue, Pid) ->
    Queue#amqqueue{pid = Pid};
set_pid(Queue, Pid) ->
    amqqueue_v1:set_pid(Queue, Pid).

% policy

-spec get_policy(amqqueue()) -> binary() | none | undefined.

get_policy(#amqqueue{policy = Policy}) -> Policy;
get_policy(Queue) -> amqqueue_v1:get_policy(Queue).

-spec set_policy(amqqueue(), binary() | none | undefined) -> amqqueue().

set_policy(#amqqueue{} = Queue, Policy) ->
    Queue#amqqueue{policy = Policy};
set_policy(Queue, Policy) ->
    amqqueue_v1:set_policy(Queue, Policy).

% policy_version

-spec get_policy_version(amqqueue()) -> non_neg_integer().

get_policy_version(#amqqueue{policy_version = PV}) ->
    PV;
get_policy_version(Queue) ->
    amqqueue_v1:get_policy_version(Queue).

-spec set_policy_version(amqqueue(), non_neg_integer()) -> amqqueue().

set_policy_version(#amqqueue{} = Queue, PV) ->
    Queue#amqqueue{policy_version = PV};
set_policy_version(Queue, PV) ->
    amqqueue_v1:set_policy_version(Queue, PV).

% recoverable_slaves

-spec get_recoverable_slaves(amqqueue()) -> [atom()] | none.

get_recoverable_slaves(#amqqueue{recoverable_slaves = Slaves}) ->
    Slaves;
get_recoverable_slaves(Queue) ->
    amqqueue_v1:get_recoverable_slaves(Queue).

-spec set_recoverable_slaves(amqqueue(), [atom()] | none) -> amqqueue().

set_recoverable_slaves(#amqqueue{} = Queue, Slaves) ->
    Queue#amqqueue{recoverable_slaves = Slaves};
set_recoverable_slaves(Queue, Slaves) ->
    amqqueue_v1:set_recoverable_slaves(Queue, Slaves).

% quorum_nodes (new in v2)

-spec get_quorum_nodes(amqqueue()) -> [node()].

get_quorum_nodes(#amqqueue{quorum_nodes = Nodes}) -> Nodes;
get_quorum_nodes(_)                               -> [].

-spec set_quorum_nodes(amqqueue(), [node()]) -> amqqueue().

set_quorum_nodes(#amqqueue{} = Queue, Nodes) ->
    Queue#amqqueue{quorum_nodes = Nodes};
set_quorum_nodes(Queue, _Nodes) ->
    Queue.

% slave_pids

-spec get_slave_pids(amqqueue()) -> [pid()] | none.

get_slave_pids(#amqqueue{slave_pids = Slaves}) ->
    Slaves;
get_slave_pids(Queue) ->
    amqqueue_v1:get_slave_pids(Queue).

-spec set_slave_pids(amqqueue(), [pid()] | none) -> amqqueue().

set_slave_pids(#amqqueue{} = Queue, SlavePids) ->
    Queue#amqqueue{slave_pids = SlavePids};
set_slave_pids(Queue, SlavePids) ->
    amqqueue_v1:set_slave_pids(Queue, SlavePids).

% slave_pids_pending_shutdown

-spec get_slave_pids_pending_shutdown(amqqueue()) -> [pid()].

get_slave_pids_pending_shutdown(#amqqueue{slave_pids_pending_shutdown = Slaves}) ->
    Slaves;
get_slave_pids_pending_shutdown(Queue) ->
    amqqueue_v1:get_slave_pids_pending_shutdown(Queue).

-spec set_slave_pids_pending_shutdown(amqqueue(), [pid()]) -> amqqueue().

set_slave_pids_pending_shutdown(#amqqueue{} = Queue, SlavePids) ->
    Queue#amqqueue{slave_pids_pending_shutdown = SlavePids};
set_slave_pids_pending_shutdown(Queue, SlavePids) ->
    amqqueue_v1:set_slave_pids_pending_shutdown(Queue, SlavePids).

% state

-spec get_state(amqqueue()) -> atom() | none.

get_state(#amqqueue{state = State}) -> State;
get_state(Queue)                    -> amqqueue_v1:get_state(Queue).

-spec set_state(amqqueue(), atom() | none) -> amqqueue().

set_state(#amqqueue{} = Queue, State) ->
    Queue#amqqueue{state = State};
set_state(Queue, State) ->
    amqqueue_v1:set_state(Queue, State).

% sync_slave_pids

-spec get_sync_slave_pids(amqqueue()) -> [pid()] | none.

get_sync_slave_pids(#amqqueue{sync_slave_pids = Pids}) ->
    Pids;
get_sync_slave_pids(Queue) ->
    amqqueue_v1:get_sync_slave_pids(Queue).

-spec set_sync_slave_pids(amqqueue(), [pid()] | none) -> amqqueue().

set_sync_slave_pids(#amqqueue{} = Queue, Pids) ->
    Queue#amqqueue{sync_slave_pids = Pids};
set_sync_slave_pids(Queue, Pids) ->
    amqqueue_v1:set_sync_slave_pids(Queue, Pids).

%% New in v2.

-spec get_type(amqqueue()) -> atom().

get_type(#amqqueue{type = Type})         -> Type;
get_type(Queue) when ?is_amqqueue(Queue) -> ?amqqueue_v1_type.

-spec get_vhost(amqqueue()) -> rabbit_types:vhost() | undefined.

get_vhost(#amqqueue{vhost = VHost}) -> VHost;
get_vhost(Queue)                    -> amqqueue_v1:get_vhost(Queue).

-spec is_auto_delete(amqqueue()) -> boolean().

is_auto_delete(#amqqueue{auto_delete = AutoDelete}) ->
    AutoDelete;
is_auto_delete(Queue) ->
    amqqueue_v1:is_auto_delete(Queue).

-spec is_durable(amqqueue()) -> boolean().

is_durable(#amqqueue{durable = Durable}) -> Durable;
is_durable(Queue)                        -> amqqueue_v1:is_durable(Queue).

-spec is_classic(amqqueue()) -> boolean().

is_classic(Queue) ->
    get_type(Queue) =:= ?amqqueue_v1_type.

-spec is_quorum(amqqueue()) -> boolean().

is_quorum(Queue) ->
    get_type(Queue) =:= quorum.

fields() ->
    case record_version_to_use() of
        ?record_version -> fields(?record_version);
        _               -> amqqueue_v1:fields()
    end.

fields(?record_version) -> record_info(fields, amqqueue);
fields(Version)         -> amqqueue_v1:fields(Version).

field_vhost() ->
    case record_version_to_use() of
        ?record_version -> #amqqueue.vhost;
        _               -> amqqueue_v1:field_vhost()
    end.

-spec pattern_match_all() -> amqqueue_pattern().

pattern_match_all() ->
    case record_version_to_use() of
        ?record_version -> #amqqueue{_ = '_'};
        _               -> amqqueue_v1:pattern_match_all()
    end.

-spec pattern_match_on_name(rabbit_amqqueue:name()) -> amqqueue_pattern().

pattern_match_on_name(Name) ->
    case record_version_to_use() of
        ?record_version -> #amqqueue{name = Name, _ = '_'};
        _               -> amqqueue_v1:pattern_match_on_name(Name)
    end.

-spec pattern_match_on_type(atom()) -> amqqueue_pattern().

pattern_match_on_type(Type) ->
    case record_version_to_use() of
        ?record_version         -> #amqqueue{type = Type, _ = '_'};
        _ when Type =:= classic -> amqqueue_v1:pattern_match_all();
        %% FIXME: We try a pattern which should never match when the
        %% `quorum_queue` feature flag is not enabled yet. Is there
        %% a better solution?
        _                       -> amqqueue_v1:pattern_match_on_name(
                                     rabbit_misc:r(<<0>>, queue, <<0>>))
    end.

-spec reset_mirroring_and_decorators(amqqueue()) -> amqqueue().

reset_mirroring_and_decorators(#amqqueue{} = Queue) ->
    Queue#amqqueue{slave_pids      = [],
                   sync_slave_pids = [],
                   gm_pids         = [],
                   decorators      = undefined};
reset_mirroring_and_decorators(Queue) ->
    amqqueue_v1:reset_mirroring_and_decorators(Queue).

-spec set_immutable(amqqueue()) -> amqqueue().

set_immutable(#amqqueue{} = Queue) ->
    Queue#amqqueue{pid                = none,
                   slave_pids         = [],
                   sync_slave_pids    = none,
                   recoverable_slaves = none,
                   gm_pids            = none,
                   policy             = none,
                   decorators         = none,
                   state              = none};
set_immutable(Queue) ->
    amqqueue_v1:set_immutable(Queue).

-spec qnode(amqqueue() | pid() | ra_server_id()) -> node().

qnode(Queue) when ?is_amqqueue(Queue) ->
    QPid = get_pid(Queue),
    qnode(QPid);
qnode(QPid) when is_pid(QPid) ->
    node(QPid);
qnode({_, Node}) ->
    Node.

% private

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
