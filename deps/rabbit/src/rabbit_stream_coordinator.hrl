
-define(STREAM_COORDINATOR_STARTUP, {stream_coordinator_startup, self()}).
-define(TICK_TIMEOUT, 30000).
-define(RESTART_TIMEOUT, 1000).
-define(PHASE_RETRY_TIMEOUT, 10000).
-define(CMD_TIMEOUT, 30000).
-define(RA_SYSTEM, coordination).
-define(RPC_TIMEOUT, 1000).

-type stream_id() :: string().
-type stream() :: #{conf := osiris:config(),
                    atom() => term()}.
-type monitor_role() :: member | listener.
-type queue_ref() :: rabbit_types:r(queue).
-type tail() :: {osiris:epoch(), osiris:offset()} | empty.

-record(member,
        {state = {down, 0} :: {down, osiris:epoch()}
                              | {stopped, osiris:epoch(), tail()}
                              | {ready, osiris:epoch()}
                              %% when a replica disconnects
                              | {running | disconnected, osiris:epoch(), pid()}
                              | deleted,
         role :: {writer | replica, osiris:epoch()},
         preferred = false :: term(),
         %% the currently running action, if any
         current :: undefined |
                    {updating |
                     stopping |
                     starting |
                     deleting, ra:index()} |
                    {sleeping, nodeup | non_neg_integer()},
         %% record the "current" config used
         conf :: undefined | osiris:config(),
         target = running :: running | stopped | deleted}).

%% member lifecycle:
%% down(epoch) ->
%% stopped(epoch, tail) ->
%% ready(epoch+1) ->
%% running(epoch+1) | disconnected(epoch+1) ->
%% deleted

-type from() :: {pid(), reference()}.

-record(stream, {id :: stream_id(),
                 epoch = 0 :: osiris:epoch(),
                 queue_ref :: queue_ref(),
                 conf :: osiris:config(),
                 nodes :: [node()],
                 members = #{} :: #{node() := #member{}},
                 listeners = #{} :: #{pid() | %% v0 & v1
                                      {pid(), leader | member} %% v2
                                      := LeaderPid :: pid()} |
                                     {node(), LocalPid :: pid()},
                 reply_to :: undefined | from(),
                 mnesia = {updated, 0} :: {updated | updating, osiris:epoch()},
                 target = running :: running | deleted
                }).

-record(rabbit_stream_coordinator,
        {streams = #{} :: #{stream_id() => #stream{}},
         monitors = #{} :: #{pid() => {stream_id() | %% v0 & v1
                                       #{stream_id() => ok}, %% v2
                                       monitor_role()} |
                             sac},
         %% not used as of v2
         listeners = #{} :: undefined | #{stream_id() =>
                                          #{pid() := queue_ref()}},
         single_active_consumer = undefined :: undefined |
                                               rabbit_stream_sac_coordinator_v4:state() |
                                               rabbit_stream_sac_coordinator:state(),
         %% future extensibility
         reserved_2}).
