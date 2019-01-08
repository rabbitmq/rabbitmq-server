%% @doc Abstraction for a process to use to interact with queues and queue like
%% entities of different types inside RabbitMQ
-module(rabbit_queue_type).

-export([
         handle_down/3
        ]).

%% This module handles the confirm tracking logic as well as monitor
%% tracking. A process using this abstraction should forwad all 'DOWN' messages
%% for monitors it hasn't created for itself.

-record(?MODULE, {monitors = #{} :: #{reference() => queue_id()},
                  %% messages in flight between this process and the queue(s)
                  pending_in = #{} :: #{seq_num() => {msg(), [queue_id()]}},
                  %% message nums in flight between here and the external client
                  pending_out = #{} :: #{out_tag() => queue_id()},
                  queues = #{} :: #{queue_id() => queue_state()}
                 }).

-opaque state() :: #?MODULE{}.

-type msg() :: term().

%% the session state maintained by each channel for each queue
%% each queue type will have it's own state
-type queue_state() :: term().

-type out_tag() :: non_neg_integer().
-type seq_num() :: non_neg_integer().

-type queue_id() :: reference().
%% unique identifier for a queue interaction session. Queue type specific.
%% Effectively unique per process that interacts with the queue.

%% Sometimes we need to set up process monitors for a specific queue type
%% that won't be tagged with a queue_id() - to be able to efficiently route
%% DOWN notifications and other such messages to the correct queue_id() we
%% need the channel process to maintain a map of monitor references to queue_ids()
%% Hence: to do we need a means of a queue type impl requesting that the channel
%% (or whatever process) sets up monitors on their behalf. Thus we need an
%% effects system.
-type queue_type_action() ::
    {monitor, pid(), queue_id()} |
    {demonitor, pid(), queue_id()} |
    % update the state of published messages,
    % - not sure queues would ever spontaneously release messages
    % so only accepted or rejected is supported
    {msg_state_update, accepted | rejected, [seq_num()]} |
    {deliveries, deliveries()}.

-type actions() :: [queue_type_action()].

-type deliveries() :: [{out_tag(), term()}].
%% placeholder to represent deliviers received from a queue

-type credit_def() :: {simple_prefetch, non_neg_integer()} | credited.

-type receive_args() :: #{credit := credit_def(),
                          atom() => term}.

-type queue_def() :: term().
%% place holder for some means of configuring the queue - possibly
%% an amqqueue record

-type queue_name() :: rabbit_misc:r(queue).

-export_type([
              state/0
              ]).

-callback init(queue_def()) ->
    {queue_state(), actions()}.

%% @doc handle an incoming info message from a queue type implementation
%% the actual message is opaque to the process and should be delived wrapped
%% as a cast as `{$queue_info, queue_id(), Info}'
%% In addition to the updated queue state and the actions it needs to return
%% a list of sequence numbers as settled|rejected actions that have been
%% confirmed to have been delivered
%% successfully. The sequence numbers are provided in in/4
-callback handle_info(queue_id(), queue_state(), Info :: term()) ->
    {queue_state(), actions()}.

%% input to the abstracted queue
-callback in(queue_id(), queue_state(),
             SeqNo :: seq_num(), Msg :: term()) ->
    {queue_state(), actions()}.

%% handle a monitor down
-callback handle_down(queue_id(), queue_state(),
                      reference(), Reason :: term()) ->
    {queue_state(), actions()}.

%% setup a receiver (consumer / subscriber / stream etc) that automatically
%% receives messages. The receive args can be partly queue type specific.
%% E.g. a "stream" queue could take an optional position argument to
%% specificy where in the log to begin streaming from.
-callback begin_receive(queue_id(), queue_state(),
                        Tag :: term(), Args :: receive_args()) ->
    {queue_state(), actions()}.

%% end a receive using the Tag
-callback end_receive(queue_id(), queue_state(), Tag :: term()) ->
    {queue_state(), actions()}.

%% updates the message state for received messages
%% indicate if they are considered settled by the receiver
-callback update_msg_state(queue_id(),
                           queue_state(),
                           OutTags :: [out_tag()],
                           accepted | rejected | released,
                           Settled :: boolean()) ->
    {queue_state(), actions()}.

%% get a single message from the queue type
-callback get(queue_id(), queue_state(), Args :: #{}) ->
    {queue_state(), actions(), Msg :: term() | empty}.


%% API

-spec init() -> state().
init() ->
    #?MODULE{}.

-spec in([queue_name()], seq_num(), term(), state()) ->
    {state(), actions()}.
in(QueueNames, SeqNum, Delivery, State) ->
    %% 1. lookup queue_ids() for the queues
    %% 2. foreach queue pass to `in' callback and aggregate actions
    {State, []}.

-spec handle_down(MonitorRef :: reference(),
                  Reason :: term(), State :: state()) ->
    {state(), actions()}.
handle_down(_MonitorRef, _Reason, State) ->
    % Module:handle_down(
    {State, []}.


-spec handle_info(queue_name(), state()) ->
    {state(), actions()}.
handle_info(_QueueName, State) ->
    %% find the state for the queue name - infos should always exists anything
    %% else is an irrecoverable error
    %% dispatch to the implementation handle_info/3 and update the state
    %% process any `settle' actions
    {State, []}.


%% channels always monitor the queues they interact with and maintain a map
%% of pid to queue name
%% This is so that they can clean up stats as well as take variou


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
