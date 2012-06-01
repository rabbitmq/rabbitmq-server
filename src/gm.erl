%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(gm).

%% Guaranteed Multicast
%% ====================
%%
%% This module provides the ability to create named groups of
%% processes to which members can be dynamically added and removed,
%% and for messages to be broadcast within the group that are
%% guaranteed to reach all members of the group during the lifetime of
%% the message. The lifetime of a message is defined as being, at a
%% minimum, the time from which the message is first sent to any
%% member of the group, up until the time at which it is known by the
%% member who published the message that the message has reached all
%% group members.
%%
%% The guarantee given is that provided a message, once sent, makes it
%% to members who do not all leave the group, the message will
%% continue to propagate to all group members.
%%
%% Another way of stating the guarantee is that if member P publishes
%% messages m and m', then for all members P', if P' is a member of
%% the group prior to the publication of m, and P' receives m', then
%% P' will receive m.
%%
%% Note that only local-ordering is enforced: i.e. if member P sends
%% message m and then message m', then for-all members P', if P'
%% receives m and m', then they will receive m' after m. Causality
%% ordering is _not_ enforced. I.e. if member P receives message m
%% and as a result publishes message m', there is no guarantee that
%% other members P' will receive m before m'.
%%
%%
%% API Use
%% -------
%%
%% Mnesia must be started. Use the idempotent create_tables/0 function
%% to create the tables required.
%%
%% start_link/3
%% Provide the group name, the callback module name, and any arguments
%% you wish to be passed into the callback module's functions. The
%% joined/2 function will be called when we have joined the group,
%% with the arguments passed to start_link and a list of the current
%% members of the group. See the callbacks specs and the comments
%% below for further details of the callback functions.
%%
%% leave/1
%% Provide the Pid. Removes the Pid from the group. The callback
%% terminate/2 function will be called.
%%
%% broadcast/2
%% Provide the Pid and a Message. The message will be sent to all
%% members of the group as per the guarantees given above. This is a
%% cast and the function call will return immediately. There is no
%% guarantee that the message will reach any member of the group.
%%
%% confirmed_broadcast/2
%% Provide the Pid and a Message. As per broadcast/2 except that this
%% is a call, not a cast, and only returns 'ok' once the Message has
%% reached every member of the group. Do not call
%% confirmed_broadcast/2 directly from the callback module otherwise
%% you will deadlock the entire group.
%%
%% group_members/1
%% Provide the Pid. Returns a list of the current group members.
%%
%%
%% Implementation Overview
%% -----------------------
%%
%% One possible means of implementation would be a fan-out from the
%% sender to every member of the group. This would require that the
%% group is fully connected, and, in the event that the original
%% sender of the message disappears from the group before the message
%% has made it to every member of the group, raises questions as to
%% who is responsible for sending on the message to new group members.
%% In particular, the issue is with [ Pid ! Msg || Pid <- Members ] -
%% if the sender dies part way through, who is responsible for
%% ensuring that the remaining Members receive the Msg? In the event
%% that within the group, messages sent are broadcast from a subset of
%% the members, the fan-out arrangement has the potential to
%% substantially impact the CPU and network workload of such members,
%% as such members would have to accommodate the cost of sending each
%% message to every group member.
%%
%% Instead, if the members of the group are arranged in a chain, then
%% it becomes easier to reason about who within the group has received
%% each message and who has not. It eases issues of responsibility: in
%% the event of a group member disappearing, the nearest upstream
%% member of the chain is responsible for ensuring that messages
%% continue to propagate down the chain. It also results in equal
%% distribution of sending and receiving workload, even if all
%% messages are being sent from just a single group member. This
%% configuration has the further advantage that it is not necessary
%% for every group member to know of every other group member, and
%% even that a group member does not have to be accessible from all
%% other group members.
%%
%% Performance is kept high by permitting pipelining and all
%% communication between joined group members is asynchronous. In the
%% chain A -> B -> C -> D, if A sends a message to the group, it will
%% not directly contact C or D. However, it must know that D receives
%% the message (in addition to B and C) before it can consider the
%% message fully sent. A simplistic implementation would require that
%% D replies to C, C replies to B and B then replies to A. This would
%% result in a propagation delay of twice the length of the chain. It
%% would also require, in the event of the failure of C, that D knows
%% to directly contact B and issue the necessary replies. Instead, the
%% chain forms a ring: D sends the message on to A: D does not
%% distinguish A as the sender, merely as the next member (downstream)
%% within the chain (which has now become a ring). When A receives
%% from D messages that A sent, it knows that all members have
%% received the message. However, the message is not dead yet: if C
%% died as B was sending to C, then B would need to detect the death
%% of C and forward the message on to D instead: thus every node has
%% to remember every message published until it is told that it can
%% forget about the message. This is essential not just for dealing
%% with failure of members, but also for the addition of new members.
%%
%% Thus once A receives the message back again, it then sends to B an
%% acknowledgement for the message, indicating that B can now forget
%% about the message. B does so, and forwards the ack to C. C forgets
%% the message, and forwards the ack to D, which forgets the message
%% and finally forwards the ack back to A. At this point, A takes no
%% further action: the message and its acknowledgement have made it to
%% every member of the group. The message is now dead, and any new
%% member joining the group at this point will not receive the
%% message.
%%
%% We therefore have two roles:
%%
%% 1. The sender, who upon receiving their own messages back, must
%% then send out acknowledgements, and upon receiving their own
%% acknowledgements back perform no further action.
%%
%% 2. The other group members who upon receiving messages and
%% acknowledgements must update their own internal state accordingly
%% (the sending member must also do this in order to be able to
%% accommodate failures), and forwards messages on to their downstream
%% neighbours.
%%
%%
%% Implementation: It gets trickier
%% --------------------------------
%%
%% Chain A -> B -> C -> D
%%
%% A publishes a message which B receives. A now dies. B and D will
%% detect the death of A, and will link up, thus the chain is now B ->
%% C -> D. B forwards A's message on to C, who forwards it to D, who
%% forwards it to B. Thus B is now responsible for A's messages - both
%% publications and acknowledgements that were in flight at the point
%% at which A died. Even worse is that this is transitive: after B
%% forwards A's message to C, B dies as well. Now C is not only
%% responsible for B's in-flight messages, but is also responsible for
%% A's in-flight messages.
%%
%% Lemma 1: A member can only determine which dead members they have
%% inherited responsibility for if there is a total ordering on the
%% conflicting additions and subtractions of members from the group.
%%
%% Consider the simultaneous death of B and addition of B' that
%% transitions a chain from A -> B -> C to A -> B' -> C. Either B' or
%% C is responsible for in-flight messages from B. It is easy to
%% ensure that at least one of them thinks they have inherited B, but
%% if we do not ensure that exactly one of them inherits B, then we
%% could have B' converting publishes to acks, which then will crash C
%% as C does not believe it has issued acks for those messages.
%%
%% More complex scenarios are easy to concoct: A -> B -> C -> D -> E
%% becoming A -> C' -> E. Who has inherited which of B, C and D?
%%
%% However, for non-conflicting membership changes, only a partial
%% ordering is required. For example, A -> B -> C becoming A -> A' ->
%% B. The addition of A', between A and B can have no conflicts with
%% the death of C: it is clear that A has inherited C's messages.
%%
%% For ease of implementation, we adopt the simple solution, of
%% imposing a total order on all membership changes.
%%
%% On the death of a member, it is ensured the dead member's
%% neighbours become aware of the death, and the upstream neighbour
%% now sends to its new downstream neighbour its state, including the
%% messages pending acknowledgement. The downstream neighbour can then
%% use this to calculate which publishes and acknowledgements it has
%% missed out on, due to the death of its old upstream. Thus the
%% downstream can catch up, and continues the propagation of messages
%% through the group.
%%
%% Lemma 2: When a member is joining, it must synchronously
%% communicate with its upstream member in order to receive its
%% starting state atomically with its addition to the group.
%%
%% New members must start with the same state as their nearest
%% upstream neighbour. This ensures that it is not surprised by
%% acknowledgements they are sent, and that should their downstream
%% neighbour die, they are able to send the correct state to their new
%% downstream neighbour to ensure it can catch up. Thus in the
%% transition A -> B -> C becomes A -> A' -> B -> C becomes A -> A' ->
%% C, A' must start with the state of A, so that it can send C the
%% correct state when B dies, allowing C to detect any missed
%% messages.
%%
%% If A' starts by adding itself to the group membership, A could then
%% die, without A' having received the necessary state from A. This
%% would leave A' responsible for in-flight messages from A, but
%% having the least knowledge of all, of those messages. Thus A' must
%% start by synchronously calling A, which then immediately sends A'
%% back its state. A then adds A' to the group. If A dies at this
%% point then A' will be able to see this (as A' will fail to appear
%% in the group membership), and thus A' will ignore the state it
%% receives from A, and will simply repeat the process, trying to now
%% join downstream from some other member. This ensures that should
%% the upstream die as soon as the new member has been joined, the new
%% member is guaranteed to receive the correct state, allowing it to
%% correctly process messages inherited due to the death of its
%% upstream neighbour.
%%
%% The canonical definition of the group membership is held by a
%% distributed database. Whilst this allows the total ordering of
%% changes to be achieved, it is nevertheless undesirable to have to
%% query this database for the current view, upon receiving each
%% message. Instead, we wish for members to be able to cache a view of
%% the group membership, which then requires a cache invalidation
%% mechanism. Each member maintains its own view of the group
%% membership. Thus when the group's membership changes, members may
%% need to become aware of such changes in order to be able to
%% accurately process messages they receive. Because of the
%% requirement of a total ordering of conflicting membership changes,
%% it is not possible to use the guaranteed broadcast mechanism to
%% communicate these changes: to achieve the necessary ordering, it
%% would be necessary for such messages to be published by exactly one
%% member, which can not be guaranteed given that such a member could
%% die.
%%
%% The total ordering we enforce on membership changes gives rise to a
%% view version number: every change to the membership creates a
%% different view, and the total ordering permits a simple
%% monotonically increasing view version number.
%%
%% Lemma 3: If a message is sent from a member that holds view version
%% N, it can be correctly processed by any member receiving the
%% message with a view version >= N.
%%
%% Initially, let us suppose that each view contains the ordering of
%% every member that was ever part of the group. Dead members are
%% marked as such. Thus we have a ring of members, some of which are
%% dead, and are thus inherited by the nearest alive downstream
%% member.
%%
%% In the chain A -> B -> C, all three members initially have view
%% version 1, which reflects reality. B publishes a message, which is
%% forward by C to A. B now dies, which A notices very quickly. Thus A
%% updates the view, creating version 2. It now forwards B's
%% publication, sending that message to its new downstream neighbour,
%% C. This happens before C is aware of the death of B. C must become
%% aware of the view change before it interprets the message its
%% received, otherwise it will fail to learn of the death of B, and
%% thus will not realise it has inherited B's messages (and will
%% likely crash).
%%
%% Thus very simply, we have that each subsequent view contains more
%% information than the preceding view.
%%
%% However, to avoid the views growing indefinitely, we need to be
%% able to delete members which have died _and_ for which no messages
%% are in-flight. This requires that upon inheriting a dead member, we
%% know the last publication sent by the dead member (this is easy: we
%% inherit a member because we are the nearest downstream member which
%% implies that we know at least as much than everyone else about the
%% publications of the dead member), and we know the earliest message
%% for which the acknowledgement is still in flight.
%%
%% In the chain A -> B -> C, when B dies, A will send to C its state
%% (as C is the new downstream from A), allowing C to calculate which
%% messages it has missed out on (described above). At this point, C
%% also inherits B's messages. If that state from A also includes the
%% last message published by B for which an acknowledgement has been
%% seen, then C knows exactly which further acknowledgements it must
%% receive (also including issuing acknowledgements for publications
%% still in-flight that it receives), after which it is known there
%% are no more messages in flight for B, thus all evidence that B was
%% ever part of the group can be safely removed from the canonical
%% group membership.
%%
%% Thus, for every message that a member sends, it includes with that
%% message its view version. When a member receives a message it will
%% update its view from the canonical copy, should its view be older
%% than the view version included in the message it has received.
%%
%% The state held by each member therefore includes the messages from
%% each publisher pending acknowledgement, the last publication seen
%% from that publisher, and the last acknowledgement from that
%% publisher. In the case of the member's own publications or
%% inherited members, this last acknowledgement seen state indicates
%% the last acknowledgement retired, rather than sent.
%%
%%
%% Proof sketch
%% ------------
%%
%% We need to prove that with the provided operational semantics, we
%% can never reach a state that is not well formed from a well-formed
%% starting state.
%%
%% Operational semantics (small step): straight-forward message
%% sending, process monitoring, state updates.
%%
%% Well formed state: dead members inherited by exactly one non-dead
%% member; for every entry in anyone's pending-acks, either (the
%% publication of the message is in-flight downstream from the member
%% and upstream from the publisher) or (the acknowledgement of the
%% message is in-flight downstream from the publisher and upstream
%% from the member).
%%
%% Proof by induction on the applicable operational semantics.
%%
%%
%% Related work
%% ------------
%%
%% The ring configuration and double traversal of messages around the
%% ring is similar (though developed independently) to the LCR
%% protocol by [Levy 2008]. However, LCR differs in several
%% ways. Firstly, by using vector clocks, it enforces a total order of
%% message delivery, which is unnecessary for our purposes. More
%% significantly, it is built on top of a "group communication system"
%% which performs the group management functions, taking
%% responsibility away from the protocol as to how to cope with safely
%% adding and removing members. When membership changes do occur, the
%% protocol stipulates that every member must perform communication
%% with every other member of the group, to ensure all outstanding
%% deliveries complete, before the entire group transitions to the new
%% view. This, in total, requires two sets of all-to-all synchronous
%% communications.
%%
%% This is not only rather inefficient, but also does not explain what
%% happens upon the failure of a member during this process. It does
%% though entirely avoid the need for inheritance of responsibility of
%% dead members that our protocol incorporates.
%%
%% In [Marandi et al 2010], a Paxos-based protocol is described. This
%% work explicitly focuses on the efficiency of communication. LCR
%% (and our protocol too) are more efficient, but at the cost of
%% higher latency. The Ring-Paxos protocol is itself built on top of
%% IP-multicast, which rules it out for many applications where
%% point-to-point communication is all that can be required. They also
%% have an excellent related work section which I really ought to
%% read...
%%
%%
%% [Levy 2008] The Complexity of Reliable Distributed Storage, 2008.
%% [Marandi et al 2010] Ring Paxos: A High-Throughput Atomic Broadcast
%% Protocol


-behaviour(gen_server2).

-export([create_tables/0, start_link/3, leave/1, broadcast/2,
         confirmed_broadcast/2, group_members/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, prioritise_info/2]).

-ifndef(use_specs).
-export([behaviour_info/1]).
-endif.

-export([table_definitions/0]).

-define(GROUP_TABLE, gm_group).
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).
-define(BROADCAST_TIMER, 25).
-define(VERSION_START, 0).
-define(SETS, ordsets).
-define(DICT, orddict).

-record(state,
        { self,
          left,
          right,
          group_name,
          module,
          view,
          pub_count,
          members_state,
          callback_args,
          confirms,
          broadcast_buffer,
          broadcast_timer
        }).

-record(gm_group, { name, version, members }).

-record(view_member, { id, aliases, left, right }).

-record(member, { pending_ack, last_pub, last_ack }).

-define(TABLE, {?GROUP_TABLE, [{record_name, gm_group},
                               {attributes, record_info(fields, gm_group)}]}).
-define(TABLE_MATCH, {match, #gm_group { _ = '_' }}).

-define(TAG, '$gm').

-ifdef(use_specs).

-export_type([group_name/0]).

-type(group_name() :: any()).

-spec(create_tables/0 :: () -> 'ok' | {'aborted', any()}).
-spec(start_link/3 :: (group_name(), atom(), any()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(leave/1 :: (pid()) -> 'ok').
-spec(broadcast/2 :: (pid(), any()) -> 'ok').
-spec(confirmed_broadcast/2 :: (pid(), any()) -> 'ok').
-spec(group_members/1 :: (pid()) -> [pid()]).

%% The joined, members_changed and handle_msg callbacks can all return
%% any of the following terms:
%%
%% 'ok' - the callback function returns normally
%%
%% {'stop', Reason} - the callback indicates the member should stop
%% with reason Reason and should leave the group.
%%
%% {'become', Module, Args} - the callback indicates that the callback
%% module should be changed to Module and that the callback functions
%% should now be passed the arguments Args. This allows the callback
%% module to be dynamically changed.

%% Called when we've successfully joined the group. Supplied with Args
%% provided in start_link, plus current group members.
-callback joined(Args :: term(), Members :: [pid()]) ->
    ok | {stop, Reason :: term()} | {become, Module :: atom(), Args :: any()}.

%% Supplied with Args provided in start_link, the list of new members
%% and the list of members previously known to us that have since
%% died. Note that if a member joins and dies very quickly, it's
%% possible that we will never see that member appear in either births
%% or deaths. However we are guaranteed that (1) we will see a member
%% joining either in the births here, or in the members passed to
%% joined/2 before receiving any messages from it; and (2) we will not
%% see members die that we have not seen born (or supplied in the
%% members to joined/2).
-callback members_changed(Args :: term(), Births :: [pid()],
                          Deaths :: [pid()]) ->
    ok | {stop, Reason :: term()} | {become, Module :: atom(), Args :: any()}.

%% Supplied with Args provided in start_link, the sender, and the
%% message. This does get called for messages injected by this member,
%% however, in such cases, there is no special significance of this
%% invocation: it does not indicate that the message has made it to
%% any other members, let alone all other members.
-callback handle_msg(Args :: term(), From :: pid(), Message :: term()) ->
    ok | {stop, Reason :: term()} | {become, Module :: atom(), Args :: any()}.

%% Called on gm member termination as per rules in gen_server, with
%% the Args provided in start_link plus the termination Reason.
-callback terminate(Args :: term(), Reason :: term()) ->
    ok | term().

-else.

behaviour_info(callbacks) ->
    [{joined, 2}, {members_changed, 3}, {handle_msg, 3}, {terminate, 2}];
behaviour_info(_Other) ->
    undefined.

-endif.

create_tables() ->
    create_tables([?TABLE]).

create_tables([]) ->
    ok;
create_tables([{Table, Attributes} | Tables]) ->
    case mnesia:create_table(Table, Attributes) of
        {atomic, ok}                          -> create_tables(Tables);
        {aborted, {already_exists, gm_group}} -> create_tables(Tables);
        Err                                   -> Err
    end.

table_definitions() ->
    {Name, Attributes} = ?TABLE,
    [{Name, [?TABLE_MATCH | Attributes]}].

start_link(GroupName, Module, Args) ->
    gen_server2:start_link(?MODULE, [GroupName, Module, Args], []).

leave(Server) ->
    gen_server2:cast(Server, leave).

broadcast(Server, Msg) ->
    gen_server2:cast(Server, {broadcast, Msg}).

confirmed_broadcast(Server, Msg) ->
    gen_server2:call(Server, {confirmed_broadcast, Msg}, infinity).

group_members(Server) ->
    gen_server2:call(Server, group_members, infinity).


init([GroupName, Module, Args]) ->
    {MegaSecs, Secs, MicroSecs} = now(),
    random:seed(MegaSecs, Secs, MicroSecs),
    Self = make_member(GroupName),
    gen_server2:cast(self(), join),
    {ok, #state { self             = Self,
                  left             = {Self, undefined},
                  right            = {Self, undefined},
                  group_name       = GroupName,
                  module           = Module,
                  view             = undefined,
                  pub_count        = -1,
                  members_state    = undefined,
                  callback_args    = Args,
                  confirms         = queue:new(),
                  broadcast_buffer = [],
                  broadcast_timer  = undefined }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.


handle_call({confirmed_broadcast, _Msg}, _From,
            State = #state { members_state = undefined }) ->
    reply(not_joined, State);

handle_call({confirmed_broadcast, Msg}, _From,
            State = #state { self          = Self,
                             right         = {Self, undefined},
                             module        = Module,
                             callback_args = Args }) ->
    handle_callback_result({Module:handle_msg(Args, get_pid(Self), Msg),
                           ok, State});

handle_call({confirmed_broadcast, Msg}, From, State) ->
    internal_broadcast(Msg, From, State);

handle_call(group_members, _From,
            State = #state { members_state = undefined }) ->
    reply(not_joined, State);

handle_call(group_members, _From, State = #state { view = View }) ->
    reply(alive_view_members(View), State);

handle_call({add_on_right, _NewMember}, _From,
            State = #state { members_state = undefined }) ->
    reply(not_ready, State);

handle_call({add_on_right, NewMember}, _From,
            State = #state { self          = Self,
                             group_name    = GroupName,
                             view          = View,
                             members_state = MembersState,
                             module        = Module,
                             callback_args = Args }) ->
    {MembersState1, Group} =
      record_new_member_in_group(
        GroupName, Self, NewMember,
        fun (Group1) ->
                View1 = group_to_view(Group1),
                MembersState1 = remove_erased_members(MembersState, View1),
                ok = send_right(NewMember, View1,
                                {catchup, Self,
                                 prepare_members_state(MembersState1)}),
                MembersState1
        end),
    View2 = group_to_view(Group),
    State1 = check_neighbours(State #state { view          = View2,
                                             members_state = MembersState1 }),
    Result = callback_view_changed(Args, Module, View, View2),
    handle_callback_result({Result, {ok, Group}, State1}).


handle_cast({?TAG, ReqVer, Msg},
            State = #state { view          = View,
                             members_state = MembersState,
                             group_name    = GroupName,
                             module        = Module,
                             callback_args = Args }) ->
    {Result, State1} =
        case needs_view_update(ReqVer, View) of
            true  -> View1 = group_to_view(read_group(GroupName)),
                     MemberState1 = remove_erased_members(MembersState, View1),
                     {callback_view_changed(Args, Module, View, View1),
                      check_neighbours(
                        State #state { view          = View1,
                                       members_state = MemberState1 })};
            false -> {ok, State}
        end,
    handle_callback_result(
      if_callback_success(
        Result, fun handle_msg_true/3, fun handle_msg_false/3, Msg, State1));

handle_cast({broadcast, _Msg}, State = #state { members_state = undefined }) ->
    noreply(State);

handle_cast({broadcast, Msg},
            State = #state { self          = Self,
                             right         = {Self, undefined},
                             module        = Module,
                             callback_args = Args }) ->
    handle_callback_result({Module:handle_msg(Args, get_pid(Self), Msg),
                            State});

handle_cast({broadcast, Msg}, State) ->
    internal_broadcast(Msg, none, State);

handle_cast(join, State = #state { self          = Self,
                                   group_name    = GroupName,
                                   members_state = undefined,
                                   module        = Module,
                                   callback_args = Args }) ->
    View = join_group(Self, GroupName),
    MembersState =
        case alive_view_members(View) of
            [Self] -> blank_member_state();
            _      -> undefined
        end,
    State1 = check_neighbours(State #state { view          = View,
                                             members_state = MembersState }),
    handle_callback_result(
      {Module:joined(Args, get_pids(all_known_members(View))), State1});

handle_cast(leave, State) ->
    {stop, normal, State}.


handle_info(flush, State) ->
    noreply(
      flush_broadcast_buffer(State #state { broadcast_timer = undefined }));

handle_info({'DOWN', MRef, process, _Pid, _Reason},
            State = #state { self          = Self,
                             left          = Left,
                             right         = Right,
                             group_name    = GroupName,
                             view          = View,
                             module        = Module,
                             callback_args = Args,
                             confirms      = Confirms }) ->
    Member = case {Left, Right} of
                 {{Member1, MRef}, _} -> Member1;
                 {_, {Member1, MRef}} -> Member1;
                 _                    -> undefined
             end,
    case Member of
        undefined ->
            noreply(State);
        _ ->
            View1 =
                group_to_view(record_dead_member_in_group(Member, GroupName)),
            {Result, State2} =
                case alive_view_members(View1) of
                    [Self] ->
                        {Result1, State1} = maybe_erase_aliases(State, View1),
                        {Result1, State1 #state {
                            members_state = blank_member_state(),
                            confirms      = purge_confirms(Confirms) }};
                    _ ->
                        %% here we won't be pointing out any deaths:
                        %% the concern is that there maybe births
                        %% which we'd otherwise miss.
                        {callback_view_changed(Args, Module, View, View1),
                         check_neighbours(State #state { view = View1 })}
                end,
            handle_callback_result({Result, State2})
    end.


terminate(Reason, State = #state { module        = Module,
                                   callback_args = Args }) ->
    flush_broadcast_buffer(State),
    Module:terminate(Args, Reason).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

prioritise_info(flush, _State) ->
    1;
prioritise_info({'DOWN', _MRef, process, _Pid, _Reason},
                #state { members_state = MS }) when MS /= undefined ->
    1;
prioritise_info(_, _State) ->
    0.


handle_msg(check_neighbours, State) ->
    %% no-op - it's already been done by the calling handle_cast
    {ok, State};

handle_msg({catchup, Left, MembersStateLeft},
           State = #state { self          = Self,
                            left          = {Left, _MRefL},
                            right         = {Right, _MRefR},
                            view          = View,
                            members_state = undefined }) ->
    ok = send_right(Right, View, {catchup, Self, MembersStateLeft}),
    MembersStateLeft1 = build_members_state(MembersStateLeft),
    {ok, State #state { members_state = MembersStateLeft1 }};

handle_msg({catchup, Left, MembersStateLeft},
           State = #state { self = Self,
                            left = {Left, _MRefL},
                            view = View,
                            members_state = MembersState })
  when MembersState =/= undefined ->
    MembersStateLeft1 = build_members_state(MembersStateLeft),
    AllMembers = lists:usort(?DICT:fetch_keys(MembersState) ++
                                 ?DICT:fetch_keys(MembersStateLeft1)),
    {MembersState1, Activity} =
        lists:foldl(
          fun (Id, MembersStateActivity) ->
                  #member { pending_ack = PALeft, last_ack = LA } =
                      find_member_or_blank(Id, MembersStateLeft1),
                  with_member_acc(
                    fun (#member { pending_ack = PA } = Member, Activity1) ->
                            case is_member_alias(Id, Self, View) of
                                true ->
                                    {_AcksInFlight, Pubs, _PA1} =
                                        find_prefix_common_suffix(PALeft, PA),
                                    {Member #member { last_ack = LA },
                                     activity_cons(Id, pubs_from_queue(Pubs),
                                                   [], Activity1)};
                                false ->
                                    {Acks, _Common, Pubs} =
                                        find_prefix_common_suffix(PA, PALeft),
                                    {Member,
                                     activity_cons(Id, pubs_from_queue(Pubs),
                                                   acks_from_queue(Acks),
                                                   Activity1)}
                            end
                    end, Id, MembersStateActivity)
          end, {MembersState, activity_nil()}, AllMembers),
    handle_msg({activity, Left, activity_finalise(Activity)},
               State #state { members_state = MembersState1 });

handle_msg({catchup, _NotLeft, _MembersState}, State) ->
    {ok, State};

handle_msg({activity, Left, Activity},
           State = #state { self          = Self,
                            left          = {Left, _MRefL},
                            view          = View,
                            members_state = MembersState,
                            confirms      = Confirms })
  when MembersState =/= undefined ->
    {MembersState1, {Confirms1, Activity1}} =
        lists:foldl(
          fun ({Id, Pubs, Acks}, MembersStateConfirmsActivity) ->
                  with_member_acc(
                    fun (Member = #member { pending_ack = PA,
                                            last_pub    = LP,
                                            last_ack    = LA },
                         {Confirms2, Activity2}) ->
                            case is_member_alias(Id, Self, View) of
                                true ->
                                    {ToAck, PA1} =
                                        find_common(queue_from_pubs(Pubs), PA,
                                                    queue:new()),
                                    LA1 = last_ack(Acks, LA),
                                    AckNums = acks_from_queue(ToAck),
                                    Confirms3 = maybe_confirm(
                                                  Self, Id, Confirms2, AckNums),
                                    {Member #member { pending_ack = PA1,
                                                      last_ack    = LA1 },
                                     {Confirms3,
                                      activity_cons(
                                        Id, [], AckNums, Activity2)}};
                                false ->
                                    PA1 = apply_acks(Acks, join_pubs(PA, Pubs)),
                                    LA1 = last_ack(Acks, LA),
                                    LP1 = last_pub(Pubs, LP),
                                    {Member #member { pending_ack = PA1,
                                                      last_pub    = LP1,
                                                      last_ack    = LA1 },
                                     {Confirms2,
                                      activity_cons(Id, Pubs, Acks, Activity2)}}
                            end
                    end, Id, MembersStateConfirmsActivity)
          end, {MembersState, {Confirms, activity_nil()}}, Activity),
    State1 = State #state { members_state = MembersState1,
                            confirms      = Confirms1 },
    Activity3 = activity_finalise(Activity1),
    ok = maybe_send_activity(Activity3, State1),
    {Result, State2} = maybe_erase_aliases(State1, View),
    if_callback_success(
      Result, fun activity_true/3, fun activity_false/3, Activity3, State2);

handle_msg({activity, _NotLeft, _Activity}, State) ->
    {ok, State}.


noreply(State) ->
    {noreply, ensure_broadcast_timer(State), hibernate}.

reply(Reply, State) ->
    {reply, Reply, ensure_broadcast_timer(State), hibernate}.

ensure_broadcast_timer(State = #state { broadcast_buffer = [],
                                        broadcast_timer  = undefined }) ->
    State;
ensure_broadcast_timer(State = #state { broadcast_buffer = [],
                                        broadcast_timer  = TRef }) ->
    erlang:cancel_timer(TRef),
    State #state { broadcast_timer = undefined };
ensure_broadcast_timer(State = #state { broadcast_timer = undefined }) ->
    TRef = erlang:send_after(?BROADCAST_TIMER, self(), flush),
    State #state { broadcast_timer = TRef };
ensure_broadcast_timer(State) ->
    State.

internal_broadcast(Msg, From, State = #state { self             = Self,
                                               pub_count        = PubCount,
                                               module           = Module,
                                               confirms         = Confirms,
                                               callback_args    = Args,
                                               broadcast_buffer = Buffer }) ->
    PubCount1 = PubCount + 1,
    Result = Module:handle_msg(Args, get_pid(Self), Msg),
    Buffer1 = [{PubCount1, Msg} | Buffer],
    Confirms1 = case From of
                    none -> Confirms;
                    _    -> queue:in({PubCount1, From}, Confirms)
                end,
    State1 = State #state { pub_count        = PubCount1,
                            confirms         = Confirms1,
                            broadcast_buffer = Buffer1 },
    case From =/= none of
        true ->
            handle_callback_result({Result, flush_broadcast_buffer(State1)});
        false ->
            handle_callback_result(
              {Result, State1 #state { broadcast_buffer = Buffer1 }})
    end.

flush_broadcast_buffer(State = #state { broadcast_buffer = [] }) ->
    State;
flush_broadcast_buffer(State = #state { self             = Self,
                                        members_state    = MembersState,
                                        broadcast_buffer = Buffer,
                                        pub_count        = PubCount }) ->
    [{PubCount, _Msg}|_] = Buffer, %% ASSERTION match on PubCount
    Pubs = lists:reverse(Buffer),
    Activity = activity_cons(Self, Pubs, [], activity_nil()),
    ok = maybe_send_activity(activity_finalise(Activity), State),
    MembersState1 = with_member(
                      fun (Member = #member { pending_ack = PA }) ->
                              PA1 = queue:join(PA, queue:from_list(Pubs)),
                              Member #member { pending_ack = PA1,
                                               last_pub = PubCount }
                      end, Self, MembersState),
    State #state { members_state    = MembersState1,
                   broadcast_buffer = [] }.


%% ---------------------------------------------------------------------------
%% View construction and inspection
%% ---------------------------------------------------------------------------

needs_view_update(ReqVer, {Ver, _View}) ->
    Ver < ReqVer.

view_version({Ver, _View}) ->
    Ver.

is_member_alive({dead, _Member}) -> false;
is_member_alive(_)               -> true.

is_member_alias(Self, Self, _View) ->
    true;
is_member_alias(Member, Self, View) ->
    ?SETS:is_element(Member,
                     ((fetch_view_member(Self, View)) #view_member.aliases)).

dead_member_id({dead, Member}) -> Member.

store_view_member(VMember = #view_member { id = Id }, {Ver, View}) ->
    {Ver, ?DICT:store(Id, VMember, View)}.

with_view_member(Fun, View, Id) ->
    store_view_member(Fun(fetch_view_member(Id, View)), View).

fetch_view_member(Id, {_Ver, View}) ->
    ?DICT:fetch(Id, View).

find_view_member(Id, {_Ver, View}) ->
    ?DICT:find(Id, View).

blank_view(Ver) ->
    {Ver, ?DICT:new()}.

alive_view_members({_Ver, View}) ->
    ?DICT:fetch_keys(View).

all_known_members({_Ver, View}) ->
    ?DICT:fold(
       fun (Member, #view_member { aliases = Aliases }, Acc) ->
               ?SETS:to_list(Aliases) ++ [Member | Acc]
       end, [], View).

group_to_view(#gm_group { members = Members, version = Ver }) ->
    Alive = lists:filter(fun is_member_alive/1, Members),
    [_|_] = Alive, %% ASSERTION - can't have all dead members
    add_aliases(link_view(Alive ++ Alive ++ Alive, blank_view(Ver)), Members).

link_view([Left, Middle, Right | Rest], View) ->
    case find_view_member(Middle, View) of
        error ->
            link_view(
              [Middle, Right | Rest],
              store_view_member(#view_member { id      = Middle,
                                               aliases = ?SETS:new(),
                                               left    = Left,
                                               right   = Right }, View));
        {ok, _} ->
            View
    end;
link_view(_, View) ->
    View.

add_aliases(View, Members) ->
    Members1 = ensure_alive_suffix(Members),
    {EmptyDeadSet, View1} =
        lists:foldl(
          fun (Member, {DeadAcc, ViewAcc}) ->
                  case is_member_alive(Member) of
                      true ->
                          {?SETS:new(),
                           with_view_member(
                             fun (VMember =
                                      #view_member { aliases = Aliases }) ->
                                     VMember #view_member {
                                       aliases = ?SETS:union(Aliases, DeadAcc) }
                             end, ViewAcc, Member)};
                      false ->
                          {?SETS:add_element(dead_member_id(Member), DeadAcc),
                           ViewAcc}
                  end
          end, {?SETS:new(), View}, Members1),
    0 = ?SETS:size(EmptyDeadSet), %% ASSERTION
    View1.

ensure_alive_suffix(Members) ->
    queue:to_list(ensure_alive_suffix1(queue:from_list(Members))).

ensure_alive_suffix1(MembersQ) ->
    {{value, Member}, MembersQ1} = queue:out_r(MembersQ),
    case is_member_alive(Member) of
        true  -> MembersQ;
        false -> ensure_alive_suffix1(queue:in_r(Member, MembersQ1))
    end.


%% ---------------------------------------------------------------------------
%% View modification
%% ---------------------------------------------------------------------------

join_group(Self, GroupName) ->
    join_group(Self, GroupName, read_group(GroupName)).

join_group(Self, GroupName, {error, not_found}) ->
    join_group(Self, GroupName, prune_or_create_group(Self, GroupName));
join_group(Self, _GroupName, #gm_group { members = [Self] } = Group) ->
    group_to_view(Group);
join_group(Self, GroupName, #gm_group { members = Members } = Group) ->
    case lists:member(Self, Members) of
        true ->
            group_to_view(Group);
        false ->
            case lists:filter(fun is_member_alive/1, Members) of
                [] ->
                    join_group(Self, GroupName,
                               prune_or_create_group(Self, GroupName));
                Alive ->
                    Left = lists:nth(random:uniform(length(Alive)), Alive),
                    Handler =
                        fun () ->
                                join_group(
                                  Self, GroupName,
                                  record_dead_member_in_group(Left, GroupName))
                        end,
                    try
                        case gen_server2:call(
                               get_pid(Left), {add_on_right, Self}, infinity) of
                            {ok, Group1} -> group_to_view(Group1);
                            not_ready    -> join_group(Self, GroupName)
                        end
                    catch
                        exit:{R, _}
                          when R =:= noproc; R =:= normal; R =:= shutdown ->
                            Handler();
                        exit:{{R, _}, _}
                          when R =:= nodedown; R =:= shutdown ->
                            Handler()
                    end
            end
    end.

read_group(GroupName) ->
    case mnesia:dirty_read(?GROUP_TABLE, GroupName) of
        []      -> {error, not_found};
        [Group] -> Group
    end.

prune_or_create_group(Self, GroupName) ->
    {atomic, Group} =
        mnesia:sync_transaction(
          fun () -> GroupNew = #gm_group { name    = GroupName,
                                           members = [Self],
                                           version = ?VERSION_START },
                    case mnesia:read({?GROUP_TABLE, GroupName}) of
                        [] ->
                            mnesia:write(GroupNew),
                            GroupNew;
                        [Group1 = #gm_group { members = Members }] ->
                            case lists:any(fun is_member_alive/1, Members) of
                                true  -> Group1;
                                false -> mnesia:write(GroupNew),
                                         GroupNew
                            end
                    end
          end),
    Group.

record_dead_member_in_group(Member, GroupName) ->
    {atomic, Group} =
        mnesia:sync_transaction(
          fun () -> [Group1 = #gm_group { members = Members, version = Ver }] =
                        mnesia:read({?GROUP_TABLE, GroupName}),
                    case lists:splitwith(
                           fun (Member1) -> Member1 =/= Member end, Members) of
                        {_Members1, []} -> %% not found - already recorded dead
                            Group1;
                        {Members1, [Member | Members2]} ->
                            Members3 = Members1 ++ [{dead, Member} | Members2],
                            Group2 = Group1 #gm_group { members = Members3,
                                                        version = Ver + 1 },
                            mnesia:write(Group2),
                            Group2
                    end
          end),
    Group.

record_new_member_in_group(GroupName, Left, NewMember, Fun) ->
    {atomic, {Result, Group}} =
        mnesia:sync_transaction(
          fun () ->
                  [#gm_group { members = Members, version = Ver } = Group1] =
                      mnesia:read({?GROUP_TABLE, GroupName}),
                  {Prefix, [Left | Suffix]} =
                      lists:splitwith(fun (M) -> M =/= Left end, Members),
                  Members1 = Prefix ++ [Left, NewMember | Suffix],
                  Group2 = Group1 #gm_group { members = Members1,
                                              version = Ver + 1 },
                  Result = Fun(Group2),
                  mnesia:write(Group2),
                  {Result, Group2}
          end),
    {Result, Group}.

erase_members_in_group(Members, GroupName) ->
    DeadMembers = [{dead, Id} || Id <- Members],
    {atomic, Group} =
        mnesia:sync_transaction(
          fun () ->
                  [Group1 = #gm_group { members = [_|_] = Members1,
                                        version = Ver }] =
                      mnesia:read({?GROUP_TABLE, GroupName}),
                  case Members1 -- DeadMembers of
                      Members1 -> Group1;
                      Members2 -> Group2 =
                                      Group1 #gm_group { members = Members2,
                                                         version = Ver + 1 },
                                  mnesia:write(Group2),
                                  Group2
                  end
          end),
    Group.

maybe_erase_aliases(State = #state { self          = Self,
                                     group_name    = GroupName,
                                     view          = View0,
                                     members_state = MembersState,
                                     module        = Module,
                                     callback_args = Args }, View) ->
    #view_member { aliases = Aliases } = fetch_view_member(Self, View),
    {Erasable, MembersState1}
        = ?SETS:fold(
             fun (Id, {ErasableAcc, MembersStateAcc} = Acc) ->
                     #member { last_pub = LP, last_ack = LA } =
                         find_member_or_blank(Id, MembersState),
                     case can_erase_view_member(Self, Id, LA, LP) of
                         true  -> {[Id | ErasableAcc],
                                   erase_member(Id, MembersStateAcc)};
                         false -> Acc
                     end
             end, {[], MembersState}, Aliases),
    State1 = State #state { members_state = MembersState1 },
    case Erasable of
        [] -> {ok, State1 #state { view = View }};
        _  -> View1 = group_to_view(
                        erase_members_in_group(Erasable, GroupName)),
              {callback_view_changed(Args, Module, View0, View1),
               check_neighbours(State1 #state { view = View1 })}
    end.

can_erase_view_member(Self, Self, _LA, _LP) -> false;
can_erase_view_member(_Self, _Id,   N,   N) -> true;
can_erase_view_member(_Self, _Id, _LA, _LP) -> false.


%% ---------------------------------------------------------------------------
%% View monitoring and maintanence
%% ---------------------------------------------------------------------------

ensure_neighbour(_Ver, Self, {Self, undefined}, Self) ->
    {Self, undefined};
ensure_neighbour(Ver, Self, {Self, undefined}, RealNeighbour) ->
    ok = gen_server2:cast(get_pid(RealNeighbour),
                          {?TAG, Ver, check_neighbours}),
    {RealNeighbour, maybe_monitor(RealNeighbour, Self)};
ensure_neighbour(_Ver, _Self, {RealNeighbour, MRef}, RealNeighbour) ->
    {RealNeighbour, MRef};
ensure_neighbour(Ver, Self, {RealNeighbour, MRef}, Neighbour) ->
    true = erlang:demonitor(MRef),
    Msg = {?TAG, Ver, check_neighbours},
    ok = gen_server2:cast(get_pid(RealNeighbour), Msg),
    ok = case Neighbour of
             Self -> ok;
             _    -> gen_server2:cast(get_pid(Neighbour), Msg)
         end,
    {Neighbour, maybe_monitor(Neighbour, Self)}.

maybe_monitor(Self, Self) ->
    undefined;
maybe_monitor(Other, _Self) ->
    erlang:monitor(process, get_pid(Other)).

check_neighbours(State = #state { self             = Self,
                                  left             = Left,
                                  right            = Right,
                                  view             = View,
                                  broadcast_buffer = Buffer }) ->
    #view_member { left = VLeft, right = VRight }
        = fetch_view_member(Self, View),
    Ver = view_version(View),
    Left1 = ensure_neighbour(Ver, Self, Left, VLeft),
    Right1 = ensure_neighbour(Ver, Self, Right, VRight),
    Buffer1 = case Right1 of
                  {Self, undefined} -> [];
                  _                 -> Buffer
              end,
    State1 = State #state { left = Left1, right = Right1,
                            broadcast_buffer = Buffer1 },
    ok = maybe_send_catchup(Right, State1),
    State1.

maybe_send_catchup(Right, #state { right = Right }) ->
    ok;
maybe_send_catchup(_Right, #state { self  = Self,
                                    right = {Self, undefined} }) ->
    ok;
maybe_send_catchup(_Right, #state { members_state = undefined }) ->
    ok;
maybe_send_catchup(_Right, #state { self          = Self,
                                    right         = {Right, _MRef},
                                    view          = View,
                                    members_state = MembersState }) ->
    send_right(Right, View,
               {catchup, Self, prepare_members_state(MembersState)}).


%% ---------------------------------------------------------------------------
%% Catch_up delta detection
%% ---------------------------------------------------------------------------

find_prefix_common_suffix(A, B) ->
    {Prefix, A1} = find_prefix(A, B, queue:new()),
    {Common, Suffix} = find_common(A1, B, queue:new()),
    {Prefix, Common, Suffix}.

%% Returns the elements of A that occur before the first element of B,
%% plus the remainder of A.
find_prefix(A, B, Prefix) ->
    case {queue:out(A), queue:out(B)} of
        {{{value, Val}, _A1}, {{value, Val}, _B1}} ->
            {Prefix, A};
        {{empty, A1}, {{value, _A}, _B1}} ->
            {Prefix, A1};
        {{{value, {NumA, _MsgA} = Val}, A1},
         {{value, {NumB, _MsgB}}, _B1}} when NumA < NumB ->
            find_prefix(A1, B, queue:in(Val, Prefix));
        {_, {empty, _B1}} ->
            {A, Prefix} %% Prefix well be empty here
    end.

%% A should be a prefix of B. Returns the commonality plus the
%% remainder of B.
find_common(A, B, Common) ->
    case {queue:out(A), queue:out(B)} of
        {{{value, Val}, A1}, {{value, Val}, B1}} ->
            find_common(A1, B1, queue:in(Val, Common));
        {{empty, _A}, _} ->
            {Common, B}
    end.


%% ---------------------------------------------------------------------------
%% Members helpers
%% ---------------------------------------------------------------------------

with_member(Fun, Id, MembersState) ->
    store_member(
      Id, Fun(find_member_or_blank(Id, MembersState)), MembersState).

with_member_acc(Fun, Id, {MembersState, Acc}) ->
    {MemberState, Acc1} = Fun(find_member_or_blank(Id, MembersState), Acc),
    {store_member(Id, MemberState, MembersState), Acc1}.

find_member_or_blank(Id, MembersState) ->
    case ?DICT:find(Id, MembersState) of
        {ok, Result} -> Result;
        error        -> blank_member()
    end.

erase_member(Id, MembersState) ->
    ?DICT:erase(Id, MembersState).

blank_member() ->
    #member { pending_ack = queue:new(), last_pub = -1, last_ack = -1 }.

blank_member_state() ->
    ?DICT:new().

store_member(Id, MemberState, MembersState) ->
    ?DICT:store(Id, MemberState, MembersState).

prepare_members_state(MembersState) ->
    ?DICT:to_list(MembersState).

build_members_state(MembersStateList) ->
    ?DICT:from_list(MembersStateList).

make_member(GroupName) ->
   {case read_group(GroupName) of
        #gm_group { version = Version } -> Version;
        {error, not_found}              -> ?VERSION_START
    end, self()}.

remove_erased_members(MembersState, View) ->
    lists:foldl(fun (Id, MembersState1) ->
                    store_member(Id, find_member_or_blank(Id, MembersState),
                                 MembersState1)
                end, blank_member_state(), all_known_members(View)).

get_pid({_Version, Pid}) -> Pid.

get_pids(Ids) -> [Pid || {_Version, Pid} <- Ids].

%% ---------------------------------------------------------------------------
%% Activity assembly
%% ---------------------------------------------------------------------------

activity_nil() ->
    queue:new().

activity_cons(_Id, [], [], Tail) ->
    Tail;
activity_cons(Sender, Pubs, Acks, Tail) ->
    queue:in({Sender, Pubs, Acks}, Tail).

activity_finalise(Activity) ->
    queue:to_list(Activity).

maybe_send_activity([], _State) ->
    ok;
maybe_send_activity(Activity, #state { self  = Self,
                                       right = {Right, _MRefR},
                                       view  = View }) ->
    send_right(Right, View, {activity, Self, Activity}).

send_right(Right, View, Msg) ->
    ok = gen_server2:cast(get_pid(Right), {?TAG, view_version(View), Msg}).

callback(Args, Module, Activity) ->
    Result =
      lists:foldl(
        fun ({Id, Pubs, _Acks}, {Args1, Module1, ok}) ->
                lists:foldl(fun ({_PubNum, Pub}, Acc = {Args2, Module2, ok}) ->
                                    case Module2:handle_msg(
                                           Args2, get_pid(Id), Pub) of
                                        ok ->
                                            Acc;
                                        {become, Module3, Args3} ->
                                            {Args3, Module3, ok};
                                        {stop, _Reason} = Error ->
                                            Error
                                    end;
                                (_, Error = {stop, _Reason}) ->
                                    Error
                            end, {Args1, Module1, ok}, Pubs);
            (_, Error = {stop, _Reason}) ->
                Error
        end, {Args, Module, ok}, Activity),
    case Result of
        {Args, Module, ok}      -> ok;
        {Args1, Module1, ok}    -> {become, Module1, Args1};
        {stop, _Reason} = Error -> Error
    end.

callback_view_changed(Args, Module, OldView, NewView) ->
    OldMembers = all_known_members(OldView),
    NewMembers = all_known_members(NewView),
    Births = NewMembers -- OldMembers,
    Deaths = OldMembers -- NewMembers,
    case {Births, Deaths} of
        {[], []} -> ok;
        _        -> Module:members_changed(Args, get_pids(Births),
                                                 get_pids(Deaths))
    end.

handle_callback_result({Result, State}) ->
    if_callback_success(
      Result, fun no_reply_true/3, fun no_reply_false/3, undefined, State);
handle_callback_result({Result, Reply, State}) ->
    if_callback_success(
      Result, fun reply_true/3, fun reply_false/3, Reply, State).

no_reply_true (_Result,        _Undefined, State) -> noreply(State).
no_reply_false({stop, Reason}, _Undefined, State) -> {stop, Reason, State}.

reply_true (_Result,        Reply, State) -> reply(Reply, State).
reply_false({stop, Reason}, Reply, State) -> {stop, Reason, Reply, State}.

handle_msg_true (_Result, Msg, State) -> handle_msg(Msg, State).
handle_msg_false(Result, _Msg, State) -> {Result, State}.

activity_true(_Result, Activity, State = #state { module        = Module,
                                                  callback_args = Args }) ->
    {callback(Args, Module, Activity), State}.
activity_false(Result, _Activity, State) ->
    {Result, State}.

if_callback_success(ok, True, _False, Arg, State) ->
    True(ok, Arg, State);
if_callback_success(
  {become, Module, Args} = Result, True, _False, Arg, State) ->
    True(Result, Arg, State #state { module        = Module,
                                     callback_args = Args });
if_callback_success({stop, _Reason} = Result, _True, False, Arg, State) ->
    False(Result, Arg, State).

maybe_confirm(_Self, _Id, Confirms, []) ->
    Confirms;
maybe_confirm(Self, Self, Confirms, [PubNum | PubNums]) ->
    case queue:out(Confirms) of
        {empty, _Confirms} ->
            Confirms;
        {{value, {PubNum, From}}, Confirms1} ->
            gen_server2:reply(From, ok),
            maybe_confirm(Self, Self, Confirms1, PubNums);
        {{value, {PubNum1, _From}}, _Confirms} when PubNum1 > PubNum ->
            maybe_confirm(Self, Self, Confirms, PubNums)
    end;
maybe_confirm(_Self, _Id, Confirms, _PubNums) ->
    Confirms.

purge_confirms(Confirms) ->
    [gen_server2:reply(From, ok) || {_PubNum, From} <- queue:to_list(Confirms)],
    queue:new().


%% ---------------------------------------------------------------------------
%% Msg transformation
%% ---------------------------------------------------------------------------

acks_from_queue(Q) ->
    [PubNum || {PubNum, _Msg} <- queue:to_list(Q)].

pubs_from_queue(Q) ->
    queue:to_list(Q).

queue_from_pubs(Pubs) ->
    queue:from_list(Pubs).

apply_acks([], Pubs) ->
    Pubs;
apply_acks(List, Pubs) ->
    {_, Pubs1} = queue:split(length(List), Pubs),
    Pubs1.

join_pubs(Q, [])   -> Q;
join_pubs(Q, Pubs) -> queue:join(Q, queue_from_pubs(Pubs)).

last_ack([], LA) ->
    LA;
last_ack(List, LA) ->
    LA1 = lists:last(List),
    true = LA1 > LA, %% ASSERTION
    LA1.

last_pub([], LP) ->
    LP;
last_pub(List, LP) ->
    {PubNum, _Msg} = lists:last(List),
    true = PubNum > LP, %% ASSERTION
    PubNum.
