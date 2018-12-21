-module(rabbit_fifo_index).

-export([
         empty/0,
         fetch/2,
         append/3,
         return/3,
         delete/2,
         size/1,
         smallest/1,
         next_key_after/2,
         map/2
        ]).

-include_lib("ra/include/ra.hrl").
-compile({no_auto_import, [size/1]}).

-record(?MODULE, {data = #{} :: #{integer() => term()},
                  smallest :: undefined | non_neg_integer(),
                  largest :: undefined | non_neg_integer()
                 }).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec empty() -> state().
empty() ->
    #?MODULE{}.

-spec fetch(integer(), state()) -> undefined | term().
fetch(Key, #?MODULE{data = Data}) ->
    maps:get(Key, Data, undefined).

% only integer keys are supported
-spec append(integer(), term(), state()) -> state().
append(Key, Value,
       #?MODULE{data = Data,
              smallest = Smallest,
              largest = Largest} = State)
  when Key > Largest orelse Largest =:= undefined ->
    State#?MODULE{data = maps:put(Key, Value, Data),
                smallest = ra_lib:default(Smallest, Key),
                largest = Key}.

-spec return(integer(), term(), state()) -> state().
return(Key, Value, #?MODULE{data = Data, smallest = Smallest} = State)
  when is_integer(Key) andalso Key < Smallest ->
    % TODO: this could potentially result in very large gaps which would
    % result in poor performance of smallest/1
    % We could try to persist a linked list of "smallests" to make it quicker
    % to skip from one to the other - needs measurement
    State#?MODULE{data = maps:put(Key, Value, Data),
                smallest = Key};
return(Key, Value, #?MODULE{data = Data} = State)
  when is_integer(Key) ->
    State#?MODULE{data = maps:put(Key, Value, Data)}.

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

-spec smallest(state()) -> undefined | {integer(), term()}.
smallest(#?MODULE{smallest = undefined}) ->
    undefined;
smallest(#?MODULE{smallest = Smallest, data = Data}) ->
    {Smallest, maps:get(Smallest, Data)}.


-spec next_key_after(non_neg_integer(), state()) -> undefined | integer().
next_key_after(_Idx, #?MODULE{smallest = undefined}) ->
    % map must be empty
    undefined;
next_key_after(Idx, #?MODULE{smallest = Smallest,
                           largest = Largest})
  when Idx+1 < Smallest orelse Idx+1 > Largest ->
    undefined;
next_key_after(Idx, #?MODULE{data = Data} = State) ->
    Next = Idx+1,
    case maps:is_key(Next, Data) of
        true ->
            Next;
        false ->
            next_key_after(Next, State)
    end.

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
            % the typical case should idealy be better
            % assuming fifo-ish deletion of entries
            find_next(Next+1, Last, Map)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

append_test() ->
    S0 = empty(),
    undefined = fetch(99, S0),
    undefined = smallest(S0),
    0 = size(S0),
    S1 = append(1, one, S0),
    undefined = fetch(99, S1),
    one = fetch(1, S1),
    1 = size(S1),
    {1, one} = smallest(S1),
    S2 = append(2, two, S1),
    two = fetch(2, S2),
    2 = size(S2),
    {1, one} = smallest(S2),
    S3 = delete(1, S2),
    {2, two} = smallest(S3),
    1 = size(S3),
    S4 = return(1, one, S3),
    one = fetch(1, S4),
    2 = size(S4),
    {1, one} = smallest(S4),
    S5 = delete(2, delete(1, S4)),
    undefined = smallest(S5),
    0 = size(S0),
    ok.

next_after_test() ->
    S = append(3, three,
               append(2, two,
                      append(1, one,
                             empty()))),
    1 = next_key_after(0, S),
    2 = next_key_after(1, S),
    3 = next_key_after(2, S),
    undefined = next_key_after(3, S),
    undefined = next_key_after(4, S),
    ok.

-endif.
