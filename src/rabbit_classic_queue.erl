-module(rabbit_classic_queue).

-export([
         init/1,
         begin_receive/4
        ]).

-record(?MODULE, {pid :: pid()}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

init(_QDef) ->
    {#?MODULE{}, []}.

begin_receive(_QId, State, ConsumerTag,
              #{acting_user := ActingUser} = Meta) ->
    Args = maps:get(consumer_args, Meta, []),
    %% TODO: validate consume arguments
    {Prefetch, NoAck} =
        case Meta of
            #{credit := {simple_prefetch, P}} ->
                {P, false};
            #{credit := none} ->
                {0, true}
        end,

    ExclusiveConsume = false,
    QPid = State#?MODULE.pid,
    ChPid = self(),
    LimiterPid = undefined,
    LimiterActive = false,
    OkMsg = undefined,
    case delegate:invoke(QPid,
                         {gen_server2, call,
                          [{basic_consume, NoAck, ChPid, LimiterPid,
                            LimiterActive,
                            Prefetch, ConsumerTag,
                            ExclusiveConsume,
                            Args, OkMsg, ActingUser}, infinity]}) of
        ok ->
            {ok, State, []};
        Err ->
            Err
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
