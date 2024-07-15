-module(rabbit_fifo_q).

-include("rabbit_fifo.hrl").
-export([
         new/0,
         in/3,
         out/1,
         get/1,
         len/1,
         from_lqueue/1,
         get_lowest_index/1
        ]).

-define(WEIGHT, 2).
-define(NON_EMPTY, {_, [_|_]}).
-define(EMPTY, {[], []}).

%% a weighted priority queue with only two priorities

-record(?MODULE, {hi = ?EMPTY :: {list(msg()), list(msg())},
                  lo = ?EMPTY :: {list(msg()), list(msg())},
                  len = 0 :: non_neg_integer(),
                  dequeue_counter = 0 :: non_neg_integer()}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec new() -> state().
new() ->
    #?MODULE{}.

-spec in(hi | lo, msg(), state()) -> state().
in(hi, Item, #?MODULE{hi = Hi, len = Len} = State) ->
    State#?MODULE{hi = in(Item, Hi),
                  len = Len + 1};
in(lo, Item, #?MODULE{lo = Lo, len = Len} = State) ->
    State#?MODULE{lo = in(Item, Lo),
                  len = Len + 1}.

-spec out(state()) ->
    empty | {msg(), state()}.
out(#?MODULE{len = 0}) ->
    empty;
out(#?MODULE{hi = Hi0,
             lo = Lo0,
             len = Len,
             dequeue_counter = C0} = State) ->
    C = case C0 of
            ?WEIGHT ->
                0;
            _ ->
                C0 + 1
        end,
    case next(State) of
        {hi, Msg} ->
            {Msg, State#?MODULE{hi = drop(Hi0),
                                dequeue_counter = C,
                                len = Len - 1}};
        {lo, Msg} ->
            {Msg, State#?MODULE{lo = drop(Lo0),
                                dequeue_counter = C,
                                len = Len - 1}}
    end.

-spec get(state()) -> empty | msg().
get(#?MODULE{len = 0}) ->
    empty;
get(#?MODULE{} = State) ->
    {_, Msg} = next(State),
    Msg.

-spec len(state()) -> non_neg_integer().
len(#?MODULE{len = Len}) ->
    Len.

-spec from_lqueue(lqueue:lqueue(msg())) -> state().
from_lqueue(LQ) ->
    lqueue:fold(fun (Item, Acc) ->
                        in(lo, Item, Acc)
                end, new(), LQ).

-spec get_lowest_index(state()) -> undefined | ra:index().
get_lowest_index(#?MODULE{len = 0}) ->
    undefined;
get_lowest_index(#?MODULE{hi = Hi, lo = Lo}) ->
    case peek(Hi) of
        empty ->
            ?MSG(LoIdx, _) = peek(Lo),
            LoIdx;
        ?MSG(HiIdx, _) ->
            case peek(Lo) of
                ?MSG(LoIdx, _) ->
                    min(HiIdx, LoIdx);
                empty ->
                    HiIdx
            end
    end.

%% internals

next(#?MODULE{hi = ?NON_EMPTY = Hi,
              lo = ?NON_EMPTY = Lo,
              dequeue_counter = ?WEIGHT}) ->
    ?MSG(HiIdx, _) = HiMsg = peek(Hi),
    ?MSG(LoIdx, _) = LoMsg = peek(Lo),
    %% always favour hi priority messages when it is safe to do so,
    %% i.e. the index is lower than the next index for the lo queue
    case HiIdx < LoIdx of
        true ->
            {hi, HiMsg};
        false ->
            {lo, LoMsg}
    end;
next(#?MODULE{hi = ?NON_EMPTY = Hi}) ->
    {hi, peek(Hi)};
next(#?MODULE{lo = Lo}) ->
    {lo, peek(Lo)}.

%% invariant, if the queue is non empty so is the Out (right) list.
in(X, ?EMPTY) ->
    {[], [X]};
in(X, {In, Out}) ->
    {[X | In], Out}.

peek(?EMPTY) ->
    empty;
peek({_, [H | _]}) ->
    H.

drop({In, [_]}) ->
    %% the last Out one
    {[], lists:reverse(In)};
drop({In, [_ | Out]}) ->
    {In, Out}.
