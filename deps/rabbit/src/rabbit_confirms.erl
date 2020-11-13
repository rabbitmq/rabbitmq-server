-module(rabbit_confirms).

-compile({no_auto_import, [size/1]}).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([init/0,
         insert/4,
         confirm/3,
         reject/2,

         remove_queue/2,

         smallest/1,
         size/1,
         is_empty/1]).

-type seq_no() :: non_neg_integer().
-type queue_name() :: rabbit_amqqueue:name().
-type exchange_name() :: rabbit_exchange:name().

-record(?MODULE, {smallest  :: undefined | seq_no(),
                  unconfirmed = #{} :: #{seq_no() =>
                                         {exchange_name(),
                                          #{queue_name() => ok}}}
                  }).

-type mx() :: {seq_no(), exchange_name()}.

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

-spec init() -> state().
init() ->
    #?MODULE{}.

-spec insert(seq_no(), [queue_name()], exchange_name(), state()) ->
    state().
insert(SeqNo, QNames, #resource{kind = exchange} = XName,
       #?MODULE{smallest = S0,
                unconfirmed = U0} = State)
  when is_integer(SeqNo)
       andalso is_list(QNames)
       andalso is_map_key(SeqNo, U0) == false ->
    U = U0#{SeqNo => {XName, maps:from_list([{Q, ok} || Q <- QNames])}},
    S = case S0 of
            undefined -> SeqNo;
            _ -> S0
        end,
    State#?MODULE{smallest = S,
                  unconfirmed = U}.

-spec confirm([seq_no()], queue_name(), state()) ->
    {[mx()], state()}.
confirm(SeqNos, QName, #?MODULE{smallest = Smallest0,
                                unconfirmed = U0} = State)
  when is_list(SeqNos) ->
    {Confirmed, U} = lists:foldr(
                       fun (SeqNo, Acc) ->
                               confirm_one(SeqNo, QName, Acc)
                       end, {[], U0}, SeqNos),
    %% check if smallest is in Confirmed
    %% TODO: this can be optimised by checking in the preceeding foldr
    Smallest =
    case lists:any(fun ({S, _}) -> S == Smallest0 end, Confirmed) of
        true ->
            %% work out new smallest
            next_smallest(Smallest0, U);
        false ->
            Smallest0
    end,
    {Confirmed, State#?MODULE{smallest = Smallest,
                              unconfirmed = U}}.

-spec reject(seq_no(), state()) ->
    {ok, mx(), state()} | {error, not_found}.
reject(SeqNo, #?MODULE{smallest = Smallest0,
                       unconfirmed = U0} = State)
  when is_integer(SeqNo) ->
    case maps:take(SeqNo, U0) of
        {{XName, _QS}, U} ->
            Smallest = case SeqNo of
                           Smallest0 ->
                               %% need to scan as the smallest was removed
                               next_smallest(Smallest0, U);
                           _ ->
                               Smallest0
                       end,
            {ok, {SeqNo, XName}, State#?MODULE{unconfirmed = U,
                                               smallest = Smallest}};
        error ->
            {error, not_found}
    end.

%% idempotent
-spec remove_queue(queue_name(), state()) ->
    {[mx()], state()}.
remove_queue(QName, #?MODULE{unconfirmed = U} = State) ->
    SeqNos = maps:fold(
               fun (SeqNo, {_XName, QS0}, Acc) ->
                       case maps:is_key(QName, QS0) of
                           true ->
                               [SeqNo | Acc];
                           false ->
                               Acc
                       end
               end, [], U),
    confirm(lists:sort(SeqNos), QName,State).

-spec smallest(state()) -> seq_no() | undefined.
smallest(#?MODULE{smallest = Smallest}) ->
    Smallest.

-spec size(state()) -> non_neg_integer().
size(#?MODULE{unconfirmed = U}) ->
    maps:size(U).

-spec is_empty(state()) -> boolean().
is_empty(State) ->
    size(State) == 0.

%% INTERNAL

confirm_one(SeqNo, QName, {Acc, U0}) ->
    case maps:take(SeqNo, U0) of
        {{XName, QS}, U1}
          when is_map_key(QName, QS)
               andalso map_size(QS) == 1 ->
            %% last queue confirm
            {[{SeqNo, XName} | Acc], U1};
        {{XName, QS}, U1} ->
            {Acc, U1#{SeqNo => {XName, maps:remove(QName, QS)}}};
        error ->
            {Acc, U0}
    end.

next_smallest(_S, U) when map_size(U) == 0 ->
    undefined;
next_smallest(S, U) when is_map_key(S, U) ->
    S;
next_smallest(S, U) ->
    %% TODO: this is potentially infinitely recursive if called incorrectly
    next_smallest(S+1, U).



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
