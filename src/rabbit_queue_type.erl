%% @doc Abstraction for a process to use to interact with queues and queue like
%% entities of different types inside RabbitMQ
-module(rabbit_queue_type).

-export([
         init/1,
         in/4,
         handle_queue_info/3,
         handle_down/3,
         begin_receive/4,

         to_map/1
        ]).

%% This module handles the confirm tracking logic as well as monitor
%% tracking. A process using this abstraction should forwad all 'DOWN' messages
%% for monitors it hasn't created for itself.

%% any fields that never or very rarely change go in here
-record(static, {queue_lookup_fun :: fun((queue_name()) -> queue_def())
                                         }).

-record(queue, {module :: module(),
                name :: queue_name(),
                state :: queue_state()}).

-record(?MODULE, {config :: #static{},
                  monitors = #{} :: #{reference() => queue_id()},
                  %% messages in flight between this process and the queue(s)
                  % pending_in = #{} :: #{seq_num() => {msg(), [queue_id()]}},
                  pending_in = dtree:empty() :: dtree:dtree(),
                  %% message nums in flight between here and the external client
                  pending_out = #{} :: #{out_tag() => queue_id()},
                  %% reverse lookup of queue id
                  queue_names = #{} :: #{queue_name() => queue_id()},
                  queues = #{} :: #{queue_id() => #queue{}}
                 }).

-opaque state() :: #?MODULE{}.

%% the session state maintained by each channel for each queue
%% each queue type will have it's own state
-type queue_state() :: term().

-type out_tag() :: non_neg_integer().
-type seq_num() :: non_neg_integer().

% -type seq_state() ::
%       %% no routes accepted or rejected
%       pending |
%       %% Terminal. all routes accepted
%       accepted |
%       %% at least one route accepted, none rejected
%       mandatory |
%       %% Terminal. All rejected
%       rejected |
%       %% at lest one rejected, none accepted
%       pending_rejected |
%       %% Terminal. at least one accepted and rejected. could have pending entries
%       mandatory_rejected |
%       %% Terminal. all routes invalidated, none accepted nor rejected
%       invalidated.

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

-type credit_def() :: {simple_prefetch, non_neg_integer()} |
                      credited |
                      none.

-type receive_meta() :: #{credit := credit_def(),
                          acting_user := binary(),
                          atom() => term}.

-type receive_tag() :: binary().

-type queue_def() :: term().
%% place holder for some means of configuring the queue - possibly
%% an amqqueue record

-type queue_name() :: rabbit_misc:r(queue).

-export_type([
              state/0
              ]).

-callback init(queue_def()) ->
    {queue_state(), actions()}.

%% input to the abstracted queue
%% TODO: need a way to aggregate queue details for every node (for use with
%% delegate).
-callback in(queue_id(), queue_state(),
             SeqNo :: seq_num(), Msg :: term()) ->
    {queue_state(), actions()}.

%% @doc handle an incoming info message from a queue type implementation
%% the actual message is opaque to the process and should be delived wrapped
%% as a cast as `{$queue_info, queue_id(), Info}'
%% In addition to the updated queue state and the actions it needs to return
%% a list of sequence numbers as settled|rejected actions that have been
%% confirmed to have been delivered
%% successfully. The sequence numbers are provided in in/4
-callback handle_queue_info(queue_id(), queue_state(), Info :: term()) ->
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
                        Tag :: term(), Args :: receive_meta()) ->
    {queue_state(), actions()}.

%% end a receive using the Tag
-callback end_receive(queue_id(), queue_state(), Tag :: term()) ->
    {queue_state(), actions()}.

%% updates the message state for received messages
%% indicate if they are considered settled by the receiver
%% TODO: need to support a variety of ways to update the message stae
%% AMQP 0.9.1: single tag or multiple (up to)
%% AMQP 1.0: range {first, last}
%% MQTT 3.1.1: appears to only PUBACK one at a time
%% STOMP: 1.2 also acks one at a time
%% KAFKA: Polls in batches, range or sequence would probably do
%% CoAP: may randomise sequence number - seqs may not be sequential
-callback update_msg_state(queue_id(),
                           queue_state(),
                           OutTags :: [out_tag()],
                           accepted | rejected | released,
                           Settled :: boolean()) ->
    {queue_state(), actions()}.

% %% get a single message from the queue type
% -callback get(queue_id(), queue_state(), Args :: #{}) ->
%     {queue_state(), actions(), Msg :: term() | empty}.


%% API

-spec init(map()) -> state().
init(#{queue_lookup_fun := Fun}) ->
    #?MODULE{config = #static{queue_lookup_fun = Fun}}.

-spec in([queue_name()], seq_num() | undefined, Delivery :: term(), state()) ->
    {state(), actions()}.
in(Destinations, SeqNum, Delivery,
   #?MODULE{config = #static{queue_lookup_fun = Fun},
            pending_in = Pend0} = State0) ->
    %% * lookup queue_ids() for the queues and initialise if not found
    {QIds, State} = lists:foldl(
                      fun(Qn, {QIds, #?MODULE{queue_names = QNames,
                                              queues = Qs} = S0}) ->
                        case QNames of
                            #{Qn := Qid} ->
                                {[Qid | QIds], S0};
                            _ ->
                                Qid = make_ref(),
                                Q = #{module := Mod} = Fun(Qn),
                                {QState, []} = Mod:init(Q),
                                Qq = #queue{module = Mod,
                                            name = Qn,
                                            state = QState},
                                {[Qid | QIds],
                                 S0#?MODULE{queue_names = QNames#{Qn => Qid},
                                            queues = Qs#{Qid => Qq}}}
                        end
                     end, {[], State0}, Destinations),

    %% dispatch to each queue type implementation
    {Queues, Actions} = lists:foldl(
                          fun(Qid, {Qus, As}) ->
                                  #queue{module = Mod} = Q = maps:get(Qid, Qus),
                                  {Q2, A} = Mod:in(Qid, Q, SeqNum, Delivery),
                                  {Qus#{Qid => Q2}, A ++ As}
                          end, {State#?MODULE.queues, []}, QIds),

    %% TODO: how to aggregate network calls for classic queues (delegate)
    %% TODO: also credit flow???
    Pend = record_pending(SeqNum, QIds, Delivery, Pend0),
    {State#?MODULE{pending_in = Pend,
                   queues = Queues}, Actions}.

record_pending(undefined, _QIds, _Delivery, Pend) ->
    Pend;
record_pending(Seq, QIds, Delivery, Pend)
  when is_integer(Seq) ->
    dtree:insert(Seq, QIds, Delivery, Pend).




-spec handle_down(MonitorRef :: reference(),
                  Reason :: term(), State :: state()) ->
    {state(), actions()}.
handle_down(_MonitorRef, _Reason, State) ->
    % Module:handle_down(
    {State, []}.


-spec handle_queue_info(queue_id(), term(), state()) ->
    {state(), actions()}.
handle_queue_info(QueueId, Info, #?MODULE{queues = Queues} = State) ->
    %% find the state for the queue name - infos should always exists anything
    %% else is an irrecoverable error
    #queue{module = Mod,
           state = Qs0} = Q = maps:get(QueueId, Queues),
    {Qs, Actions0} = Mod:handle_queue_info(QueueId, Info, Qs0),
    %% dispatch to the implementation handle_queue_info/3 and update the state
    %% process any `settle' actions
    %% handle actions
    {Actions, Pend} =
        lists:foldl(fun({msg_state_update, DeliveryState, Seqs},
                        {Acc, P}) ->
                            case dtree:take(Seqs, QueueId, P) of
                                {[], P1} ->
                                    {Acc, P1};
                                {Completed, P1} ->
                                    CompletedSeqs = [S || {S, _} <- Completed],
                                    Evt = {msg_state_update, DeliveryState,
                                           CompletedSeqs},
                                    {[Evt | Acc], P1}
                            end
                    end, {[], State#?MODULE.pending_in}, Actions0),
    {State#?MODULE{queues = Queues#{QueueId => Q#queue{state = Qs}},
                   pending_in = Pend}, lists:reverse(Actions)}.


-spec begin_receive(queue_name(), state(),
                    receive_tag(), receive_meta()) ->
    {ok, state(), actions()} | {error, duplicate}.
begin_receive(_QName, State, _Tag, _Args) ->
    % Module:handle_down(
    {ok, State, []}.

to_map(State) ->
    #{monitors => State#?MODULE.monitors,
      pending_in => State#?MODULE.pending_in,
      pending_out => State#?MODULE.pending_out,
      queue_names => State#?MODULE.queue_names,
      queues => State#?MODULE.queues}.


%% channels always monitor the queues they interact with and maintain a map
%% of pid to queue name
%% This is so that they can clean up stats as well as take variou


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup_meck_impl(Mod) ->
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, init, fun (Q) -> {Q, []} end),
    meck:expect(Mod, in,
                fun (_QId, Qs, _Seq, _Msg) ->
                        {Qs, []}
                end),
    meck:expect(Mod, handle_queue_info,
                fun (_QId, {applied, Seqs}, Qs) when is_list(Seqs) ->
                        %% simply record as accepted
                        {Qs, [{msg_state_update, accepted, Seqs}]};
                    (_QId, {rejected, Seqs}, Qs) when is_list(Seqs) ->
                        {Qs, [{msg_state_update, rejected, Seqs}]}
                end),
    ok.

in_msg_is_accepted_test() ->
    QName = make_queue_name(?FUNCTION_NAME),
    setup_meck_impl(?FUNCTION_NAME),
    LookupFun = fun(Name) ->
                        #{module => ?FUNCTION_NAME,
                          name => Name}
                end,
    Qs0 = rabbit_queue_type:init(#{queue_lookup_fun => LookupFun}),

    {Qs1, []} = rabbit_queue_type:in([QName], 1, some_delivery, Qs0),

    %% extract the assigned queue id
    #?MODULE{queues = Queues, pending_in = P1} = Qs1,
    [QId] = maps:keys(Queues),
    ?assertEqual(1, dtree:size(P1)),

    %% this is when the channel can send confirms for example
    {Qs2, [{msg_state_update, accepted, [1]}]} =
        rabbit_queue_type:handle_queue_info(QId, {applied, [1]}, Qs1),

    {Qs2, [{msg_state_update, rejected, [1]}]} =
        rabbit_queue_type:handle_queue_info(QId, {rejected, [1]}, Qs1),
    %% no pending should remain inside the state after the queue has accepted
    %% the message
    ?assertEqual(0, dtree:size(Qs2#?MODULE.pending_in)),
    ok.

untracked_in_test() ->
    QName = make_queue_name(?FUNCTION_NAME),
    setup_meck_impl(?FUNCTION_NAME),
    LookupFun = fun(Name) ->
                        #{module => ?FUNCTION_NAME,
                          name => Name}
                end,
    Qs0 = rabbit_queue_type:init(#{queue_lookup_fun => LookupFun}),

    {Qs1, []} = rabbit_queue_type:in([QName], undefined, some_delivery, Qs0),
    #?MODULE{queues = Queues, pending_in = P1} = Qs1,
    [_] = maps:keys(Queues),
    ?assertEqual(0, dtree:size(P1)),
    ok.

in_msg_multi_queue_is_accepted_test() ->
    QName = make_queue_name(?FUNCTION_NAME),
    QAlt = make_queue_name(alt_queue_name),
    setup_meck_impl(?FUNCTION_NAME),
    LookupFun = fun(Name) ->
                        #{module => ?FUNCTION_NAME,
                          name => Name}
                end,
    Qs0 = rabbit_queue_type:init(#{queue_lookup_fun => LookupFun}),

    {#?MODULE{queue_names = #{QName := QId1,
                              QAlt := QId2}} = Qs1,
     []} = rabbit_queue_type:in([QName, QAlt], 1, some_delivery, Qs0),

    %% no msg_state_update should be issued for the first one
    {Qs2, []} =
        rabbit_queue_type:handle_queue_info(QId1, {applied, [1]}, Qs1),
    {_, [{msg_state_update, accepted, [1]}]} =
        rabbit_queue_type:handle_queue_info(QId2, {applied, [1]}, Qs2),
    ?assert(meck:called(?FUNCTION_NAME, in, ['_', '_', '_', '_'])),
    ?assertEqual(2, meck:num_calls(?FUNCTION_NAME, in, '_')),
    meck:unload(),
    ok.

begin_end_receive_test() ->
    QName = make_queue_name(?FUNCTION_NAME),
    setup_meck_impl(?FUNCTION_NAME),
    LookupFun = fun(Name) ->
                        #{module => ?FUNCTION_NAME,
                          name => Name}
                end,
    Qs0 = rabbit_queue_type:init(#{queue_lookup_fun => LookupFun}),
    Tag = <<"my-tag">>,
    Args = #{credit => {simple_prefetch, 10}},
    {ok, Qs1, []} = rabbit_queue_type:begin_receive(QName, Qs0, Tag, Args),
    %% Assert new queue state was set up and Mod:begin_receive was called
    ?assert(meck:called(?FUNCTION_NAME, begin_receive, ['_', '_', '_', '_'])),
    {error, duplicate} = rabbit_queue_type:begin_receive(QName, Qs0, Tag, Args),
    {ok, _, []} = rabbit_queue_type:end_receive(QName, Qs1, Tag),
    ?assert(meck:called(?FUNCTION_NAME, end_receive, ['_', '_', '_'])),
    %% TODO:if there are no publishes and no other receiver tags does this
    %% end the queue session?
    %% TODO: handle calls to being_receive with tame tag (should error)
    ok.

make_queue_name(Name) when is_atom(Name) ->
    rabbit_misc:r("/", queue, atom_to_binary(Name, utf8)).

-endif.
