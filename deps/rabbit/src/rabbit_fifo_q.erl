-module(rabbit_fifo_q).

-include("rabbit_fifo.hrl").
-export([
         new/0,
         in/3,
         out/1,
         get/1,
         len/1,
         from_lqueue/1,
         get_lowest_index/1,
         overview/1
        ]).

-define(WEIGHT, 2).
-define(NON_EMPTY, {_, [_|_]}).
-define(EMPTY, {[], []}).

%% a weighted priority queue with only two priorities

-record(?MODULE, {hi = ?EMPTY :: {list(msg()), list(msg())}, %% high
                  no = ?EMPTY :: {list(msg()), list(msg())}, %% normal
                  len = 0 :: non_neg_integer(),
                  dequeue_counter = 0 :: non_neg_integer()}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec new() -> state().
new() ->
    #?MODULE{}.

-spec in(hi | no, msg(), state()) -> state().
in(hi, Item, #?MODULE{hi = Hi, len = Len} = State) ->
    State#?MODULE{hi = in(Item, Hi),
                  len = Len + 1};
in(no, Item, #?MODULE{no = No, len = Len} = State) ->
    State#?MODULE{no = in(Item, No),
                  len = Len + 1}.

-spec out(state()) ->
    empty | {msg(), state()}.
out(#?MODULE{len = 0}) ->
    empty;
out(#?MODULE{hi = Hi0,
             no = No0,
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
        {no, Msg} ->
            {Msg, State#?MODULE{no = drop(No0),
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
                        in(no, Item, Acc)
                end, new(), LQ).

-spec get_lowest_index(state()) -> undefined | ra:index().
get_lowest_index(#?MODULE{len = 0}) ->
    undefined;
get_lowest_index(#?MODULE{hi = Hi, no = No}) ->
    case peek(Hi) of
        empty ->
            ?MSG(NoIdx, _) = peek(No),
            NoIdx;
        ?MSG(HiIdx, _) ->
            case peek(No) of
                ?MSG(NoIdx, _) ->
                    min(HiIdx, NoIdx);
                empty ->
                    HiIdx
            end
    end.

-spec overview(state()) ->
    #{len := non_neg_integer(),
      num_hi := non_neg_integer(),
      num_no := non_neg_integer(),
      lowest_index := ra:index()}.
overview(#?MODULE{len = Len,
                  hi = {Hi1, Hi2},
                  no = _} = State) ->
    %% TODO: this could be very slow with large backlogs,
    %% consider keeping a separate counter for 'hi', 'no' messages
    NumHi = length(Hi1) + length(Hi2),
    #{len => Len,
      num_hi => NumHi,
      num_no => Len - NumHi,
      lowest_index => get_lowest_index(State)}.

%% internals

next(#?MODULE{hi = ?NON_EMPTY = Hi,
              no = ?NON_EMPTY = No,
              dequeue_counter = ?WEIGHT}) ->
    ?MSG(HiIdx, _) = HiMsg = peek(Hi),
    ?MSG(NoIdx, _) = NoMsg = peek(No),
    %% always favour hi priority messages when it is safe to do so,
    %% i.e. the index is lower than the next index for the 'no' queue
    case HiIdx < NoIdx of
        true ->
            {hi, HiMsg};
        false ->
            {no, NoMsg}
    end;
next(#?MODULE{hi = ?NON_EMPTY = Hi}) ->
    {hi, peek(Hi)};
next(#?MODULE{no = No}) ->
    {no, peek(No)}.

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
