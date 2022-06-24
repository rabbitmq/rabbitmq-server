-module(rabbit_virtual_queue).
-behaviour(rabbit_queue_type).

-include("amqqueue.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(STATE, ?MODULE).
-record(?STATE, {pid :: undefined | pid(), %% the current master pid
                 consumer_tag :: term(),
                 key :: binary(),
                 qref :: term()}).


-opaque state() :: #?STATE{}.

-export_type([state/0]).

-export([
         is_enabled/0,
         declare/2,
         delete/4,
         is_recoverable/1,
         recover/2,
         purge/1,
         policy_changed/1,
         stat/1,
         init/1,
         close/1,
         update/2,
         consume/3,
         cancel/5,
         handle_event/2,
         deliver/2,
         settle/4,
         credit/4,
         dequeue/4,
         info/2,
         state_info/1,
         capabilities/0,
         notify_decorators/1
         ]).

-export([
         is_virtual/1,
         create_amqqueue/1
        ]).


-spec is_virtual(binary() | rabbit_amqqueue:name()) -> boolean().
is_virtual(<<"amq.rabbitmq.reply-to.", _/binary>>) ->
    true;
is_virtual(#resource{name = Name}) ->
    is_virtual(Name);
is_virtual(_) ->
    false.

create_amqqueue(#resource{name = <<"amq.rabbitmq.reply-to">>,
                          virtual_host = VHost}) ->
    {Key, Suffix} = rabbit_direct_reply_to:compute_key_and_suffix_v2(self()),
    Name = <<"amq.rabbitmq.reply-to.", Suffix/binary>>,
    QName = #resource{name = Name,
                      virtual_host = VHost,
                      kind = queue},
    Q = amqqueue:new(QName,
                     self(),
                     false,
                     false,
                     none,
                     [],
                     VHost,
                     #{user => <<"internal">>},
                     ?MODULE),
    TS = amqqueue:get_type_state(Q),
    amqqueue:set_type_state(Q, TS#{key => Key,
                                   suffix => Suffix});
create_amqqueue(#resource{name = Name,
                          virtual_host = VHost,
                          kind = queue} = QName) ->
    {Pid, Key} = pid_and_key_from_name(Name),
    Q = amqqueue:new(QName,
                     Pid,
                     false,
                     false,
                     none,
                     [],
                     VHost,
                     #{user => <<"internal">>},
                     ?MODULE),
    TS = amqqueue:get_type_state(Q),
    amqqueue:set_type_state(Q, TS#{key => Key}).

pid_and_key_from_name(<<"amq.rabbitmq.reply-to.", EncodedBin/binary>>) ->
    case rabbit_direct_reply_to:decode_reply_to_v2(
           EncodedBin, rabbit_nodes:all_running_with_hashes()) of
        {ok, Pid, Key} ->
            {Pid, Key};
        {error, _} ->
            {ok, Pid, Key} = rabbit_direct_reply_to:decode_reply_to_v1(EncodedBin),
            {Pid, Key}
    end.

notify_decorators(_) ->
    ok.

is_enabled() -> true.

declare(Q, _Node) ->
    {new, Q}.

delete(_Q, _IfUnused, _IfEmpty, _ActingUser) ->
    {ok, 0}.

is_recoverable(_Q) ->
    false.

recover(_VHost, _Queues) ->
    {[], []}.

-spec policy_changed(amqqueue:amqqueue()) -> ok.
policy_changed(_Q) ->
    ok.

stat(_Q) ->
    {ok, 0, 0}.

-spec init(amqqueue:amqqueue()) ->
    {ok, state()}.
init(Q) ->
    QName = amqqueue:get_name(Q),
    #{key := Key} = amqqueue:get_type_state(Q),
    {ok, #?STATE{pid = amqqueue:get_pid(Q),
                 key = Key,
                 qref = QName}}.

-spec close(state()) -> ok.
close(_State) ->
    ok.

-spec update(amqqueue:amqqueue(), state()) -> state().
update(_Q, #?STATE{} = State) ->
    State.

consume(_Q, #{consumer_tag := CTag}, State) ->
    {ok, State#?MODULE{consumer_tag = CTag}, []}.

cancel(_Q, _ConsumerTag, _OkMsg, _ActingUser, State) ->
    {ok, State}.

-spec settle(rabbit_queue_type:settle_op(), rabbit_types:ctag(),
             [non_neg_integer()], state()) ->
    {state(), rabbit_queue_type:actions()}.
settle(_Op, _CTag, _MsgIds, State) ->
    {State, []}.

credit(_CTag, _Credit, _Drain, State) ->
    {State, []}.

handle_event({down, Pid, _Info}, #?STATE{pid = Pid} = _State0) ->
    eol;
handle_event({deliver_reply, Key, #delivery{message = Message}},
             #?STATE{qref = QName,
                     consumer_tag = CTag,
                     key = Key} = State) ->
    Msg = {QName, self(), 0, false, Message},
    {ok, State, [{deliver, CTag, false, [Msg]}]}.


-spec deliver([{amqqueue:amqqueue(), state()}], Delivery :: term()) ->
    {[{amqqueue:amqqueue(), state()}], rabbit_queue_type:actions()}.
deliver(Qs0, #delivery{} = Delivery) ->
    Actions = [begin
                   Pid = amqqueue:get_pid(Q),
                   #{key := Key} = amqqueue:get_type_state(Q),
                   Evt =  {queue_event, QRef, {deliver_reply, Key, Delivery}},
                   gen_server2:cast(Pid, Evt),
                   {monitor, process, Pid}
               end || {Q, #?MODULE{qref = QRef}} <- Qs0],
    {Qs0, Actions}.


-spec dequeue(NoAck :: boolean(), LimiterPid :: pid(),
              rabbit_types:ctag(), state()) ->
    {ok, Count :: non_neg_integer(), rabbit_amqqueue:qmsg(), state()} |
    {empty, state()}.
dequeue(_NoAck, _LimiterPid, _CTag, State) ->
    {empty, State}.

-spec state_info(state()) -> #{atom() := term()}.
state_info(_State) ->
    #{}.

%% general queue info
-spec info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(_Q, _Items) ->
    [].

-spec purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()}.
purge(Q) when ?is_amqqueue(Q) ->
    {ok, 0}.

%% internal-ish

capabilities() ->
    #{unsupported_policies => [%% Stream policies
                               <<"max-age">>, <<"stream-max-segment-size-bytes">>,
                               <<"queue-leader-locator">>, <<"initial-cluster-size">>,
                               %% Quorum policies
                               <<"delivery-limit">>, <<"dead-letter-strategy">>],
      queue_arguments => [<<"x-expires">>, <<"x-message-ttl">>, <<"x-dead-letter-exchange">>,
                          <<"x-dead-letter-routing-key">>, <<"x-max-length">>,
                          <<"x-max-length-bytes">>, <<"x-max-priority">>,
                          <<"x-overflow">>, <<"x-queue-mode">>, <<"x-queue-version">>,
                          <<"x-single-active-consumer">>, <<"x-queue-type">>,
                          <<"x-queue-master-locator">>],
      consumer_arguments => [<<"x-cancel-on-ha-failover">>,
                             <<"x-priority">>, <<"x-credit">>],
      server_named => true}.

