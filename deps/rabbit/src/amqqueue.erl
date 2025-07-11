%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqqueue). %% Could become amqqueue_v2 in the future.

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([new/8,
         new/9,
         new_with_version/9,
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
         get_leader_node/1,
         get_nodes/1,
         % name (#resource)
         get_name/1,
         set_name/2,
         % operator_policy
         get_operator_policy/1,
         set_operator_policy/2,
         % options
         get_options/1,
         set_options/2,
         % pid
         get_pid/1,
         set_pid/2,
         % policy
         get_policy/1,
         set_policy/2,
         % policy_version
         get_policy_version/1,
         set_policy_version/2,
         % type_state
         get_type_state/1,
         set_type_state/2,
         % state
         get_state/1,
         set_state/2,
         get_type/1,
         get_vhost/1,
         is_amqqueue/1,
         is_auto_delete/1,
         is_durable/1,
         is_exclusive/1,
         is_classic/1,
         is_quorum/1,
         is_internal/1,
         internal_owner/1,
         make_internal/1,
         make_internal/2,
         pattern_match_all/0,
         pattern_match_on_name/1,
         pattern_match_on_type/1,
         pattern_match_on_durable/1,
         pattern_match_on_type_and_durable/2,
         pattern_match_on_type_and_vhost/2,
         reset_decorators/1,
         set_immutable/1,
         qnode/1,
         to_printable/1,
         to_printable/2,
         macros/0]).

-define(record_version, amqqueue_v2).
-define(is_backwards_compat_classic(T),
        (T =:= classic orelse T =:= ?amqqueue_v1_type)).

-type amqqueue_options() ::  map() | ets:match_pattern().

-record(amqqueue, {
          %% immutable
          name :: rabbit_amqqueue:name() | ets:match_pattern(),
          %% immutable
          durable :: boolean() | ets:match_pattern(),
          %% immutable
          auto_delete :: boolean() | ets:match_pattern(),
          %% immutable
          exclusive_owner = none :: pid() | none | ets:match_pattern(),
          %% immutable
          arguments = [] :: rabbit_framing:amqp_table() | ets:match_pattern(),
          %% durable (just so we know home node)
          pid :: pid() | ra_server_id() | none | ets:match_pattern(),
          slave_pids = [], %% reserved
          sync_slave_pids = [], %% reserved
          recoverable_slaves = [], %% reserved
          %% durable, implicit update as above
          policy :: proplists:proplist() | none | undefined | ets:match_pattern(),
          %% durable, implicit update as above
          operator_policy :: proplists:proplist() | none | undefined | ets:match_pattern(),
          %% transient
          gm_pids = [] :: [{pid(), pid()}] | none | ets:match_pattern(),
          %% transient, recalculated as above
          decorators :: [atom()] | none | undefined | ets:match_pattern(),
          %% durable (have we crashed?)
          state = live :: atom() | none | ets:match_pattern(),
          policy_version = 0 :: non_neg_integer() | ets:match_pattern(),
          slave_pids_pending_shutdown = [], %% reserved
          %% secondary index
          vhost :: rabbit_types:vhost() | undefined | ets:match_pattern(),
          options = #{} :: amqqueue_options(),
          type = ?amqqueue_v1_type :: module() | ets:match_pattern(),
          type_state = #{} :: map() | ets:match_pattern()
         }).

-type amqqueue() :: amqqueue_v2().
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
                          policy :: proplists:proplist() | none | undefined,
                          operator_policy :: proplists:proplist() |
                                             none | undefined,
                          gm_pids :: [{pid(), pid()}] | none,
                          decorators :: [atom()] | none | undefined,
                          state :: atom() | none,
                          policy_version :: non_neg_integer(),
                          slave_pids_pending_shutdown :: [pid()],
                          vhost :: rabbit_types:vhost() | undefined,
                          options :: map(),
                          type :: atom(),
                          type_state :: #{}
                         }.

-type ra_server_id() :: {Name :: atom(), Node :: node()}.

-type amqqueue_pattern() :: amqqueue_v2_pattern().
-type amqqueue_v2_pattern() :: #amqqueue{
                                  name :: rabbit_amqqueue:name() | ets:match_pattern(),
                                  durable :: ets:match_pattern(),
                                  auto_delete :: ets:match_pattern(),
                                  exclusive_owner :: ets:match_pattern(),
                                  arguments :: ets:match_pattern(),
                                  pid :: ets:match_pattern(),
                                  slave_pids :: ets:match_pattern(),
                                  sync_slave_pids :: ets:match_pattern(),
                                  recoverable_slaves :: ets:match_pattern(),
                                  policy :: ets:match_pattern(),
                                  operator_policy :: ets:match_pattern(),
                                  gm_pids :: ets:match_pattern(),
                                  decorators :: ets:match_pattern(),
                                  state :: ets:match_pattern(),
                                  policy_version :: ets:match_pattern(),
                                  slave_pids_pending_shutdown :: ets:match_pattern(),
                                  vhost :: ets:match_pattern(),
                                  options :: ets:match_pattern(),
                                  type :: atom() | ets:match_pattern(),
                                  type_state :: ets:match_pattern()
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
          map()) -> amqqueue().

new(#resource{kind = queue} = Name,
    Pid,
    Durable,
    AutoDelete,
    Owner,
    Args,
    VHost,
    Options)
  when (is_pid(Pid) orelse is_tuple(Pid) orelse Pid =:= none) andalso
       is_boolean(Durable) andalso
       is_boolean(AutoDelete) andalso
       (is_pid(Owner) orelse Owner =:= none) andalso
       is_list(Args) andalso
       (is_binary(VHost) orelse VHost =:= undefined) andalso
       is_map(Options) ->
    new(Name,
        Pid,
        Durable,
        AutoDelete,
        Owner,
        Args,
        VHost,
        Options,
        ?amqqueue_v1_type).

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
      Type).

-spec new_with_version
(amqqueue_v2,
 rabbit_amqqueue:name(),
 pid() | ra_server_id() | none,
 boolean(),
 boolean(),
 pid() | none,
 rabbit_framing:amqp_table(),
 rabbit_types:vhost() | undefined,
 map()) -> amqqueue().

new_with_version(RecordVersion,
                 #resource{kind = queue} = Name,
                 Pid,
                 Durable,
                 AutoDelete,
                 Owner,
                 Args,
                 VHost,
                 Options)
  when (is_pid(Pid) orelse is_tuple(Pid) orelse Pid =:= none) andalso
       is_boolean(Durable) andalso
       is_boolean(AutoDelete) andalso
       (is_pid(Owner) orelse Owner =:= none) andalso
       is_list(Args) andalso
       (is_binary(VHost) orelse VHost =:= undefined) andalso
       is_map(Options) ->
    new_with_version(RecordVersion,
                     Name,
                     Pid,
                     Durable,
                     AutoDelete,
                     Owner,
                     Args,
                     VHost,
                     Options,
                     ?amqqueue_v1_type).

-spec new_with_version
(amqqueue_v2,
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
              type            = ensure_type_compat(Type)}.

-spec is_amqqueue(any()) -> boolean().

is_amqqueue(#amqqueue{}) -> true.

-spec record_version_to_use() -> amqqueue_v2.

record_version_to_use() ->
    ?record_version.

-spec upgrade(amqqueue()) -> amqqueue().

upgrade(#amqqueue{} = Queue) -> Queue.

-spec upgrade_to(amqqueue_v2, amqqueue()) -> amqqueue_v2().

upgrade_to(?record_version, #amqqueue{} = Queue) ->
    Queue.

% arguments

-spec get_arguments(amqqueue()) -> rabbit_framing:amqp_table().

get_arguments(#amqqueue{arguments = Args}) ->
    Args.

-spec set_arguments(amqqueue(), rabbit_framing:amqp_table()) -> amqqueue().

set_arguments(#amqqueue{} = Queue, Args) ->
    Queue#amqqueue{arguments = Args}.

% options

-spec get_options(amqqueue()) -> amqqueue_options().

get_options(#amqqueue{options = Options}) ->
    Options.

-spec set_options(amqqueue(), amqqueue_options()) -> amqqueue().

set_options(#amqqueue{} = Queue, Options) ->
    Queue#amqqueue{options = Options}.


% decorators

-spec get_decorators(amqqueue()) -> [atom()] | none | undefined.

get_decorators(#amqqueue{decorators = Decorators}) ->
    Decorators.

-spec set_decorators(amqqueue(), [atom()] | none | undefined) -> amqqueue().

set_decorators(#amqqueue{} = Queue, Decorators) ->
    Queue#amqqueue{decorators = Decorators}.

-spec get_exclusive_owner(amqqueue()) -> pid() | none.

get_exclusive_owner(#amqqueue{exclusive_owner = Owner}) ->
    Owner.

-spec get_leader_node(amqqueue_v2()) -> node() | none.

get_leader_node(#amqqueue{pid = {_, Leader}}) -> Leader;
get_leader_node(#amqqueue{pid = none}) -> none;
get_leader_node(#amqqueue{pid = Pid}) -> node(Pid).

-spec get_nodes(amqqueue_v2()) -> [node(),...].

get_nodes(Q) ->
    case amqqueue:get_type_state(Q) of
        #{nodes := Nodes} ->
            Nodes;
        _ ->
            [get_leader_node(Q)]
    end.

% operator_policy

-spec get_operator_policy(amqqueue()) -> binary() | none | undefined.

get_operator_policy(#amqqueue{operator_policy = OpPolicy}) -> OpPolicy.

-spec set_operator_policy(amqqueue(), binary() | none | undefined) ->
    amqqueue().

set_operator_policy(#amqqueue{} = Queue, Policy) ->
    Queue#amqqueue{operator_policy = Policy}.

% name

-spec get_name(amqqueue()) -> rabbit_amqqueue:name().

get_name(#amqqueue{name = Name}) -> Name.

-spec set_name(amqqueue(), rabbit_amqqueue:name()) -> amqqueue().

set_name(#amqqueue{} = Queue, Name) ->
    Queue#amqqueue{name = Name}.

% pid

-spec get_pid(amqqueue_v2()) -> pid() | ra_server_id() | none.

get_pid(#amqqueue{pid = Pid}) -> Pid.

-spec set_pid(amqqueue_v2(), pid() | ra_server_id() | none) -> amqqueue_v2().

set_pid(#amqqueue{} = Queue, Pid) ->
    Queue#amqqueue{pid = Pid}.

% policy

-spec get_policy(amqqueue()) -> proplists:proplist() | none | undefined.

get_policy(#amqqueue{policy = Policy}) -> Policy.

-spec set_policy(amqqueue(), binary() | none | undefined) -> amqqueue().

set_policy(#amqqueue{} = Queue, Policy) ->
    Queue#amqqueue{policy = Policy}.

% policy_version

-spec get_policy_version(amqqueue()) -> non_neg_integer().

get_policy_version(#amqqueue{policy_version = PV}) ->
    PV.

-spec set_policy_version(amqqueue(), non_neg_integer()) -> amqqueue().

set_policy_version(#amqqueue{} = Queue, PV) ->
    Queue#amqqueue{policy_version = PV}.

% type_state (new in v2)

-spec get_type_state(amqqueue()) -> map().
get_type_state(#amqqueue{type_state = TState}) when is_map(TState) ->
    TState;
get_type_state(_) ->
    #{}.

-spec set_type_state(amqqueue(), map()) -> amqqueue().
set_type_state(#amqqueue{} = Queue, TState) ->
    Queue#amqqueue{type_state = TState};
set_type_state(Queue, _TState) ->
    Queue.

% state

-spec get_state(amqqueue()) -> atom() | none.

get_state(#amqqueue{state = State}) -> State.

-spec set_state(amqqueue(), atom() | none) -> amqqueue().

set_state(#amqqueue{} = Queue, State) ->
    Queue#amqqueue{state = State}.

%% New in v2.

-spec get_type(amqqueue()) -> atom().

get_type(#amqqueue{type = Type}) -> Type.

-spec get_vhost(amqqueue()) -> rabbit_types:vhost() | undefined.

get_vhost(#amqqueue{vhost = VHost}) -> VHost.

-spec is_auto_delete(amqqueue()) -> boolean().

is_auto_delete(#amqqueue{auto_delete = AutoDelete}) ->
    AutoDelete.

-spec is_durable(amqqueue()) -> boolean().

is_durable(#amqqueue{durable = Durable}) -> Durable.

-spec is_exclusive(amqqueue()) -> boolean().

is_exclusive(Queue) ->
    is_pid(get_exclusive_owner(Queue)).

-spec is_classic(amqqueue()) -> boolean().

is_classic(Queue) ->
    get_type(Queue) =:= ?amqqueue_v1_type.

-spec is_quorum(amqqueue()) -> boolean().

is_quorum(Queue) ->
    get_type(Queue) =:= rabbit_quorum_queue.

-spec is_internal(amqqueue()) -> boolean().

is_internal(#amqqueue{options = #{internal := true}}) -> true;
is_internal(#amqqueue{}) -> false.

-spec internal_owner(amqqueue()) -> rabbit_types:option(#resource{}).

internal_owner(#amqqueue{options = #{internal := true,
                                     internal_owner := IOwner}}) ->
    IOwner;
internal_owner(#amqqueue{}) ->
    undefined.

-spec make_internal(amqqueue()) -> amqqueue().

make_internal(Q = #amqqueue{options = Options}) when is_map(Options) ->
    Q#amqqueue{options = maps:merge(Options, #{internal => true,
                                               internal_owner => undefined})}.

-spec make_internal(amqqueue(), rabbit_types:r(queue | exchange)) -> amqqueue().

make_internal(Q = #amqqueue{options = Options}, Owner)
  when is_map(Options) andalso is_record(Owner, resource) ->
    Q#amqqueue{options = maps:merge(Options, #{internal => true,
                                              interna_owner => Owner})}.

fields() ->
    fields(?record_version).

fields(?record_version) -> record_info(fields, amqqueue).

field_vhost() ->
    #amqqueue.vhost.

-spec pattern_match_all() -> amqqueue_pattern().

pattern_match_all() ->
    #amqqueue{_ = '_'}.

-spec pattern_match_on_name(Name) -> Pattern when
      Name :: rabbit_amqqueue:name() | ets:match_pattern(),
      Pattern :: amqqueue_pattern().

pattern_match_on_name(Name) ->
    #amqqueue{name = Name, _ = '_'}.

-spec pattern_match_on_type(atom()) -> amqqueue_pattern().

pattern_match_on_type(Type) ->
    #amqqueue{type = Type, _ = '_'}.

-spec pattern_match_on_durable(boolean()) -> amqqueue_pattern().

pattern_match_on_durable(IsDurable) ->
    #amqqueue{durable = IsDurable, _ = '_'}.

-spec pattern_match_on_type_and_durable(atom(), boolean()) ->
    amqqueue_pattern().

pattern_match_on_type_and_durable(Type, IsDurable) ->
    #amqqueue{type = Type, durable = IsDurable, _ = '_'}.

-spec pattern_match_on_type_and_vhost(atom(), binary()) ->
    amqqueue_pattern().

pattern_match_on_type_and_vhost(Type, VHost) ->
    #amqqueue{type = Type, vhost = VHost, _ = '_'}.

-spec reset_decorators(amqqueue()) -> amqqueue().

reset_decorators(#amqqueue{} = Queue) ->
    Queue#amqqueue{decorators      = undefined}.

-spec set_immutable(amqqueue()) -> amqqueue().

set_immutable(#amqqueue{} = Queue) ->
    Queue#amqqueue{pid                = none,
                   policy             = none,
                   decorators         = none,
                   state              = none}.

-spec qnode(amqqueue() | pid() | ra_server_id()) -> node().

qnode(Queue) when ?is_amqqueue(Queue) ->
    QPid = get_pid(Queue),
    qnode(QPid);
qnode(QPid) when is_pid(QPid) ->
    node(QPid);
qnode(none) ->
    undefined;
qnode({_, Node}) ->
    Node.

-spec to_printable(amqqueue()) -> #{binary() => any()}.
to_printable(#amqqueue{name = QName = #resource{name = Name},
                       vhost = VHost, type = Type}) ->
     #{<<"readable_name">> => rabbit_data_coercion:to_binary(rabbit_misc:rs(QName)),
       <<"name">> => Name,
       <<"virtual_host">> => VHost,
       <<"type">> => Type}.

-spec to_printable(rabbit_types:r(queue), atom() | binary()) -> #{binary() => any()}.
to_printable(QName = #resource{name = Name, virtual_host = VHost}, Type) ->
    _ = rabbit_queue_type:discover(Type),
     #{<<"readable_name">> => rabbit_data_coercion:to_binary(rabbit_misc:rs(QName)),
       <<"name">> => Name,
       <<"virtual_host">> => VHost,
       <<"type">> => Type}.

% private

macros() ->
    io:format(
      "-define(is_~ts(Q), is_record(Q, amqqueue, ~b)).~n~n",
      [?record_version, record_info(size, amqqueue)]),
    %% The field number starts at 2 because the first element is the
    %% record name.
    macros(record_info(fields, amqqueue), 2).

macros([Field | Rest], I) ->
    io:format(
      "-define(~s_field_~ts(Q), element(~b, Q)).~n",
      [?record_version, Field, I]),
    macros(Rest, I + 1);
macros([], _) ->
    ok.

ensure_type_compat(classic) ->
    ?amqqueue_v1_type;
ensure_type_compat(Type) ->
    Type.
