
-define(STREAM_COORDINATOR_STARTUP, {stream_coordinator_startup, self()}).
-define(TICK_TIMEOUT, 30000).
-define(RESTART_TIMEOUT, 1000).
-define(PHASE_RETRY_TIMEOUT, 10000).
-define(CMD_TIMEOUT, 30000).
-define(RA_SYSTEM, coordination).

-type stream_id() :: string().
-type stream() :: #{conf := osiris:config(),
                    atom() => term()}.
-type monitor_role() :: member | listener.
-type queue_ref() :: rabbit_types:r(queue).
-type tail() :: {osiris:offset(), osiris:epoch()} | empty.

-record(member,
        {state = {down, 0} :: {down, osiris:epoch()}
                              | {stopped, osiris:epoch(), tail()}
                              | {ready, osiris:epoch()}
                              %% when a replica disconnects
                              | {running | disconnected, osiris:epoch(), pid()}
                              | deleted,
         role :: {writer | replica, osiris:epoch()},
         node :: node(),
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

%% member lifecycle
%% down -> stopped(tail) -> running | disconnected -> deleted
%%
%% split the handling of incoming events (down, success | fail of operations)
%% and the actioning of current state (i.e. member A is down but the cluster target
%% is `up` - start a current action to turn member A -> running

-type from() :: {pid(), reference()}.

-record(stream, {id :: stream_id(),
                 epoch = 0 :: osiris:epoch(),
                 queue_ref :: queue_ref(),
                 conf :: osiris:config(),
                 nodes :: [node()],
                 members = #{} :: #{node() := #member{}},
                 listeners = #{} :: #{pid() := LeaderPid :: pid()},
                 reply_to :: undefined | from(),
                 mnesia = {updated, 0} :: {updated | updating, osiris:epoch()},
                 target = running :: running | deleted
                }).

-record(?MODULE, {streams = #{} :: #{stream_id() => #stream{}},
                  monitors = #{} :: #{pid() => {stream_id(), monitor_role()}},
                  listeners = #{} :: #{stream_id() =>
                                       #{pid() := queue_ref()}},
                  %% future extensibility
                  reserved_1,
                  reserved_2}).
