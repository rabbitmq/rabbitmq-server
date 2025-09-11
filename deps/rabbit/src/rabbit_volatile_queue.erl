%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This queue type is volatile:
%% * Queue metadata is not stored in the metadata store.
%% * Messages in this queue are effectively transient and delivered at most once.
%% * Messages are not buffered.
%% * Messages are dropped immediately if consumer ran out of link credit.
-module(rabbit_volatile_queue).
-behaviour(rabbit_queue_type).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([new/1,
         new_name/0,
         is/1,
         key_from_name/1,
         pid_from_name/2,
         exists/1,
         ff_enabled/0,
         local_cast/2,
         local_call/2]).

%% rabbit_queue_type callbacks
-export([declare/2,
         supports_stateful_delivery/0,
         deliver/3,
         credit/6,
         init/1,
         close/1,
         update/2,
         consume/3,
         cancel/3,
         handle_event/3,
         is_enabled/0,
         is_compatible/3,
         is_recoverable/1,
         purge/1,
         policy_changed/1,
         stat/1,
         format/2,
         capabilities/0,
         notify_decorators/1,
         stop/1,
         list_with_minimum_quorum/0,
         drain/1,
         revive/0,
         queue_vm_stats_sups/0,
         queue_vm_ets/0,
         delete/4,
         recover/2,
         settle/5,
         credit_v1/5,
         dequeue/5,
         state_info/1,
         info/2,
         policy_apply_to_name/0
        ]).

-define(STATE, ?MODULE).
-record(?STATE, {
           name :: rabbit_amqqueue:name(),
           ctag :: undefined | rabbit_types:ctag(),
           delivery_count :: undefined | rabbit_queue_type:delivery_count(),
           credit :: undefined | rabbit_queue_type:credit(),
           dropped = 0 :: non_neg_integer()
          }).

-opaque state() :: #?STATE{}.

-export_type([state/0]).

-define(PREFIX, "amq.rabbitmq.reply-to.").
-define(CP_DOT, cp_dot).

-spec new(rabbit_amqqueue:name()) ->
    amqqueue:amqqueue() | error.
new(#resource{virtual_host = Vhost,
              name = <<"amq.rabbitmq.reply-to">>} = Name) ->
    new0(Name, self(), Vhost);
new(#resource{virtual_host = Vhost,
              name = NameBin} = Name) ->
    case pid_from_name(NameBin, nodes_with_hashes()) of
        {ok, Pid} when is_pid(Pid) ->
            new0(Name, Pid, Vhost);
        _ ->
            error
    end.

new0(Name, Pid, Vhost) ->
    amqqueue:new(Name, Pid, false, true, none, [], Vhost, #{}, ?MODULE).

-spec is(rabbit_misc:resource_name()) ->
    boolean().
is(<<?PREFIX, _/binary>>) ->
    true;
is(Name) when is_binary(Name) ->
    false.

init(Q) ->
    {ok, #?STATE{name = amqqueue:get_name(Q)}}.

consume(_Q, Spec, State) ->
    #{no_ack := true,
      consumer_tag := Ctag,
      mode := Mode} = Spec,
    {DeliveryCount, Credit} = case Mode of
                                  {credited, InitialDC} ->
                                      {InitialDC, 0};
                                  {simple_prefetch, 0} ->
                                      {undefined, undefined}
                              end,
    {ok, State#?STATE{ctag = Ctag,
                      delivery_count = DeliveryCount,
                      credit = Credit}}.

declare(Q, _Node) ->
    #resource{name = NameBin} = Name = amqqueue:get_name(Q),
    case NameBin of
        <<"amq.rabbitmq.reply-to">> ->
            {existing, Q};
        _ ->
            case exists(Name) of
                true ->
                    {existing, Q};
                false ->
                    {absent, Q, stopped}
            end
    end.

-spec exists(rabbit_amqqueue:name()) -> boolean().
exists(#resource{kind = queue,
                 name = QNameBin} = QName) ->
    case pid_from_name(QNameBin, nodes_with_hashes()) of
        {ok, Pid} when is_pid(Pid) ->
            case ff_enabled() of
                true ->
                    Request = {has_state, QName, ?MODULE},
                    MFA = {?MODULE, local_call, [Request]},
                    try delegate:invoke(Pid, MFA)
                    catch _:_ -> false
                    end;
                false ->
                    case key_from_name(QNameBin) of
                        {ok, Key} ->
                            Msg = {declare_fast_reply_to, Key},
                            try gen_server:call(Pid, Msg, infinity) of
                                exists -> true;
                                _ -> false
                            catch exit:_ -> false
                            end;
                        error ->
                            false
                    end
            end;
        _ ->
            false
    end.

supports_stateful_delivery() ->
    false.

deliver(Qs, Msg, #{correlation := Corr})
  when Corr =/= undefined ->
    Corrs = [Corr],
    Actions = lists:map(fun({Q, stateless}) ->
                                deliver0(Q, Msg),
                                {settled, amqqueue:get_name(Q), Corrs}
                        end, Qs),
    {[], Actions};
deliver(Qs, Msg, #{}) ->
    lists:foreach(fun({Q, stateless}) ->
                          deliver0(Q, Msg)
                  end, Qs),
    {[], []}.

deliver0(Q, Msg) ->
    QName = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    case ff_enabled() of
        true ->
            Request = {queue_event, QName, {deliver, Msg}},
            MFA = {?MODULE, local_cast, [Request]},
            delegate:invoke_no_result(QPid, MFA);
        false ->
            case key_from_name(QName#resource.name) of
                {ok, Key} ->
                    MFA = {rabbit_channel, deliver_reply_local, [Key, Msg]},
                    delegate:invoke_no_result(QPid, MFA);
                error ->
                    ok
            end
    end.

-spec local_cast(pid(), term()) -> ok.
local_cast(Pid, Request) ->
    %% Ensure clients can't send a message to an arbitrary process and kill it.
    case is_local(Pid) of
        true -> gen_server:cast(Pid, Request);
        false -> ok
    end.

-spec local_call(pid(), term()) -> term().
local_call(Pid, Request) ->
    %% Ensure clients can't send a message to an arbitrary process and kill it.
    case is_local(Pid) of
        true -> gen_server:call(Pid, Request);
        false -> exit({unknown_pid, Pid})
    end.

is_local(Pid) ->
    rabbit_amqp_session:is_local(Pid) orelse
    pg_local:in_group(rabbit_channels, Pid).

handle_event(QName, {deliver, Msg}, #?STATE{name = QName,
                                            ctag = Ctag,
                                            credit = undefined} = State) ->
    {ok, State, deliver_actions(QName, Ctag, Msg)};
handle_event(QName, {deliver, Msg}, #?STATE{name = QName,
                                            ctag = Ctag,
                                            delivery_count = DeliveryCount,
                                            credit = Credit} = State0)
  when Credit > 0 ->
    State = State0#?STATE{delivery_count = serial_number:add(DeliveryCount, 1),
                          credit = Credit - 1},
    {ok, State, deliver_actions(QName, Ctag, Msg)};
handle_event(QName, {deliver, _Msg}, #?STATE{name = QName,
                                             dropped = Dropped} = State) ->
    rabbit_global_counters:messages_dead_lettered(maxlen, ?MODULE, disabled, 1),
    {ok, State#?STATE{dropped = Dropped + 1}, []}.

deliver_actions(QName, Ctag, Mc) ->
    Msgs = [{QName, self(), undefined, _Redelivered = false, Mc}],
    [{deliver, Ctag, _AckRequired = false, Msgs}].

credit(_QName, CTag, DeliveryCountRcv, LinkCreditRcv, Drain,
       #?STATE{delivery_count = DeliveryCountSnd} = State) ->
    LinkCreditSnd = amqp10_util:link_credit_snd(
                      DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd),
    {DeliveryCount, Credit} = case Drain of
                                  true ->
                                      {serial_number:add(DeliveryCountSnd, LinkCreditSnd), 0};
                                  false ->
                                      {DeliveryCountSnd, LinkCreditSnd}
                              end,
    {State#?STATE{delivery_count = DeliveryCount,
                  credit = Credit},
     [{credit_reply, CTag, DeliveryCount, Credit, _Available = 0, Drain}]}.

close(#?STATE{}) ->
    ok.

update(_, #?STATE{} = State) ->
    State.

cancel(_, _, #?STATE{} = State) ->
    {ok, State}.

is_enabled() ->
    true.

ff_enabled() ->
    rabbit_feature_flags:is_enabled('rabbitmq_4.2.0').

is_compatible(_, _, _) ->
    true.

is_recoverable(_) ->
    false.

purge(_) ->
    {ok, 0}.

policy_changed(_) ->
    ok.

notify_decorators(_) ->
    ok.

stat(_) ->
    {ok, 0, 1}.

format(_, _) ->
    [].

capabilities() ->
    #{unsupported_policies => [],
      queue_arguments => [],
      consumer_arguments => [],
      amqp_capabilities => [],
      server_named => false,
      rebalance_module => undefined,
      can_redeliver => false ,
      is_replicable => false}.

stop(_) ->
    ok.

list_with_minimum_quorum() ->
    [].

drain(_) ->
    ok.

revive() ->
    ok.

queue_vm_stats_sups() ->
    {[], []}.

queue_vm_ets() ->
    {[], []}.

delete(_, _, _, _) ->
    {ok, 0}.

recover(_, _) ->
    {[], []}.

settle(_, _, _, _, #?STATE{} = State) ->
    {State, []}.

credit_v1(_, _, _, _, #?STATE{} = State) ->
    {State, []}.

dequeue(_, _, _, _, #?STATE{name = Name}) ->
    {protocol_error, not_implemented,
     "basic.get not supported by volatile ~ts",
     [rabbit_misc:rs(Name)]}.

state_info(#?STATE{}) ->
    #{}.

info(_, _) ->
    [].

policy_apply_to_name() ->
    <<>>.

-spec new_name() ->
    rabbit_misc:resource_name().
new_name() ->
    EncodedPid = encode_pid(self()),
    EncodedKey = base64:encode(rabbit_guid:gen()),
    <<?PREFIX, EncodedPid/binary, ".", EncodedKey/binary>>.

%% This pid encoding function produces values that are of mostly fixed size
%% regardless of the node name length.
encode_pid(Pid) ->
    PidParts0 = #{node := Node} = rabbit_pid_codec:decompose(Pid),
    %% Note: we hash the entire node name. This is sufficient for our needs of shortening node name
    %% in the TTB-encoded pid, and helps avoid doing the node name split for every single cluster member
    %% in rabbit_nodes:all_running_with_hashes/0.
    %%
    %% We also use a synthetic node prefix because the hash alone will be sufficient to
    NodeHash = erlang:phash2(Node),
    PidParts = maps:update(node,
                           rabbit_nodes_common:make("reply", integer_to_list(NodeHash)),
                           PidParts0),
    base64:encode(rabbit_pid_codec:recompose_to_binary(PidParts)).

-spec pid_from_name(rabbit_misc:resource_name(),
                    #{non_neg_integer() => node()}) ->
    {ok, pid()} | error.
pid_from_name(<<?PREFIX, Bin/binary>>, CandidateNodes) ->
    Cp = case persistent_term:get(?CP_DOT, undefined) of
             undefined ->
                 P = binary:compile_pattern(<<".">>),
                 persistent_term:put(?CP_DOT, P),
                 P;
             P ->
                 P
         end,
    try
        [PidBase64, _KeyBase64] = binary:split(Bin, Cp),
        PidBin = base64:decode(PidBase64),
        PidParts0 = #{node := ShortenedNodename} = rabbit_pid_codec:decompose_from_binary(PidBin),
        {_, NodeHash} = rabbit_nodes_common:parts(ShortenedNodename),
        case maps:get(list_to_integer(NodeHash), CandidateNodes, undefined) of
            undefined ->
                error;
            Candidate ->
                PidParts = maps:update(node, Candidate, PidParts0),
                {ok, rabbit_pid_codec:recompose(PidParts)}
        end
    catch error:_ -> error
    end;
pid_from_name(_, _) ->
    error.

%% Returns the base 64 encoded key.
-spec key_from_name(rabbit_misc:resource_name()) ->
    {ok, binary()} | error.
key_from_name(<<?PREFIX, Suffix/binary>>) ->
    case binary:split(Suffix, <<".">>) of
        [_Pid, Key] ->
            {ok, Key};
        _ ->
            error
    end;
key_from_name(_) ->
    error.

nodes_with_hashes() ->
    #{erlang:phash2(Node) => Node || Node <- rabbit_nodes:list_members()}.
