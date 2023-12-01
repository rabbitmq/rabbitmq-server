-module(hinoloq).

-export([
         new/0,
         in/3,
         out/1,
         len/1,
         peek_all/1,
         overview/1
         ]).

%% a simple weighted priority queue with only two priorities

-type int() :: non_neg_integer().
-type lqueue() :: {int(), queue:queue()}.
-type priority() :: high | normal | low.

-record(?MODULE, {items = #{} :: #{priority() => lqueue()},
                  weights = {3,2,1} :: {int(), int(), int()},
                  state = {high, 3} :: {high | normal | low, int()}
                 }).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec new() -> state().
new() ->
    #?MODULE{}.

-spec in(priority(), term(), state()) -> state().
in(high, Item, #?MODULE{items = #{high := {Cnt, Q}} = Items} = State) ->
    State#?MODULE{items = Items#{high => {Cnt + 1, queue:in(Item, Q)}}};
in(normal, Item, #?MODULE{items = #{normal := {Cnt, Q}} = Items} = State) ->
    State#?MODULE{items = Items#{normal => {Cnt + 1, queue:in(Item, Q)}}};
in(low, Item, #?MODULE{items = #{low := {Cnt, Q}} = Items} = State) ->
    State#?MODULE{items = Items#{low => {Cnt + 1, queue:in(Item, Q)}}};
in(Priority, Item, #?MODULE{items = Items} = State) ->
    Q = queue:in(Item, queue:new()),
    State#?MODULE{items = Items#{Priority => {1, Q}}}.

dequeue(#?MODULE{items = Items0,
                 state = {Cur, Rem}} = State)
  when Rem > 0 andalso is_map_key(Cur, Items0) ->
    {Cnt, Q0} = maps:get(Cur, Items0),
    {{value, Item}, Q} = queue:out(Q0),
    Items = case Cnt of
                1 ->
                    maps:remove(Cur, Items0);
                _ ->
                    Items0#{Cur => {Cnt-1, Q}}
            end,
    {Item, State#?MODULE{items = Items,
                         state = {Cur, Rem - 1}}};
dequeue(#?MODULE{items = Items,
                 weights = {H, N, L},
                 state = {Cur, _}} = State) ->
    % current state is either out of credits or items
    Next = case Cur of
               high when is_map_key(normal, Items) ->
                   {normal, N};
               high when is_map_key(low, Items) ->
                   {low, L};
               normal when is_map_key(low, Items) ->
                   {low, L};
               low when is_map_key(high, Items) ->
                   {high, H};
               low when is_map_key(normal, Items) ->
                   {normal, N};
               _ ->
                   {high, H}
           end,
    dequeue(State#?MODULE{state = Next}).


out(#?MODULE{items = Items}) when map_size(Items) == 0 ->
    empty;
out(State0) ->
    dequeue(State0).

len(#?MODULE{items = Items}) ->
    maps:fold(fun (_, {C, _}, Acc) ->
                      Acc + C
              end, 0, Items).

peek_all(#?MODULE{items = Items}) ->
    maps:fold(
      fun (_K, {C, Q}, Acc) when C > 0 ->
              [queue:get(Q) | Acc];
          (_, _, Acc) ->
              Acc
      end, [], Items).

overview(#?MODULE{items = Items}) ->
    maps:map(fun (_, {C, _}) -> C end, Items).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basics_test() ->
    Q1 = in_(new(), [
                     {high, hi1},
                     {low, lo1},
                     {high, hi2},
                     {normal, no1},
                     {low, lo2},
                     {high, hi3},
                     {high, hi4}
                    ]),
    ?assertEqual(7, len(Q1)),
    {{value, hi1}, Q2} = out(Q1),
    {{value, hi2}, Q3} = out(Q2),
    {{value, hi3}, Q4} = out(Q3),
    {{value, no1}, Q5} = out(Q4),
    {{value, lo1}, Q6} = out(Q5),
    {{value, hi4}, Q7} = out(Q6),
    {{value, lo2}, Q8} = out(Q7),
    empty = out(Q8),
ok.

high_prio_test() ->
    Q000 = in_(new(), [{low, lo1},
                       {normal, no1},
                       {low, lo2},
                       {low, lo3},
                       {high, hi1},
                       {high, hi2},
                       {high, hi3}
                      ]),

    {{value, hi1}, Q00} = out(Q000),
    {{value, hi2}, Q0} = out(Q00),
    {{value, hi3}, Q1} = out(Q0),
    {{value, no1}, Q2} = out(Q1),
    {{value, lo1}, Q3} = out(Q2),
    Q4 = in(normal, no2, Q3),
    {{value, no2}, Q5} = out(Q4),
    Q6 = in(high, hi2, Q5),
    {{value, lo2}, Q7} = out(Q6),
    {{value, hi2}, Q8} = out(Q7),
    ok.

in_(Q, Ops) ->
    lists:foldl(
      fun ({P, I}, Q) ->
              in(P, I, Q)
      end, Q, Ops).

-endif.
