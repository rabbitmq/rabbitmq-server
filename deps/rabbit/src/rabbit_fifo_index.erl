-module(rabbit_fifo_index).

-export([
         empty/0,
         exists/2,
         append/2,
         delete/2,
         size/1,
         smallest/1,
         map/2,
         normalize/1
        ]).

-compile({no_auto_import, [size/1]}).

%% the empty atom is a lot smaller (4 bytes) than e.g. `undefined` (13 bytes).
%% This matters as the data map gets persisted as part of the snapshot
-define(NIL, []).

-record(?MODULE, {data = #{} :: #{integer() => ?NIL},
                  smallest :: undefined | non_neg_integer(),
                  largest :: undefined | non_neg_integer()
                 }).


-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec empty() -> state().
empty() ->
    #?MODULE{}.

-spec exists(integer(), state()) -> boolean().
exists(Key, #?MODULE{data = Data}) ->
    maps:is_key(Key, Data).

% only integer keys are supported
-spec append(integer(), state()) -> state().
append(Key,
       #?MODULE{data = Data,
                smallest = Smallest,
                largest = Largest} = State)
  when Key > Largest orelse
       Largest =:= undefined ->
    State#?MODULE{data = maps:put(Key, ?NIL, Data),
                  smallest = ra_lib:default(Smallest, Key),
                  largest = Key};
append(Key,
       #?MODULE{data = Data,
                largest = Largest,
                smallest = Smallest} = State) ->
    State#?MODULE{data = maps:put(Key, ?NIL, Data),
                  smallest = min(Key, ra_lib:default(Smallest, Key)),
                  largest = max(Key, ra_lib:default(Largest, Key))
                  }.

-spec delete(Index :: integer(), state()) -> state().
delete(Smallest, #?MODULE{data = Data0,
                          largest = Largest,
                          smallest = Smallest} = State) ->
    Data = maps:remove(Smallest, Data0),
    case find_next(Smallest + 1, Largest, Data) of
        undefined ->
            State#?MODULE{data = Data,
                          smallest = undefined,
                          largest = undefined};
        Next ->
            State#?MODULE{data = Data, smallest = Next}
    end;
delete(Key, #?MODULE{data = Data} = State) ->
    State#?MODULE{data = maps:remove(Key, Data)}.

-spec size(state()) -> non_neg_integer().
size(#?MODULE{data = Data}) ->
    maps:size(Data).

-spec smallest(state()) -> undefined | integer().
smallest(#?MODULE{smallest = Smallest}) ->
    Smallest.


-spec map(fun(), state()) -> state().
map(F, #?MODULE{data = Data} = State) ->
    State#?MODULE{data = maps:map(F, Data)}.


%% internal

find_next(Next, Last, _Map) when Next > Last ->
    undefined;
find_next(Next, Last, Map) ->
    case Map of
        #{Next := _} ->
            Next;
        _ ->
            % in degenerate cases the range here could be very large
            % and hence this could be very slow
            % the typical case should ideally be better
            % assuming fifo-ish deletion of entries
            find_next(Next+1, Last, Map)
    end.

-spec normalize(state()) -> state().
normalize(State) ->
    State#?MODULE{largest = undefined}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

append_test() ->
    S0 = empty(),
    false = exists(99, S0),
    undefined = smallest(S0),
    0 = size(S0),
    S1 = append(1, S0),
    false = exists(99, S1),
    true = exists(1, S1),
    1 = size(S1),
    1 = smallest(S1),
    S2 = append(2, S1),
    true = exists(2, S2),
    2 = size(S2),
    1 = smallest(S2),
    S3 = delete(1, S2),
    2 = smallest(S3),
    1 = size(S3),
    S5 = delete(2, S3),
    undefined = smallest(S5),
    0 = size(S0),
    ok.

-endif.
