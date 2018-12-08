%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_open).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2, info/1, info/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([init_db/0]).

-rabbit_boot_step({?MODULE, [ {description, "exchange type x-open : registry"}
  ,{mfa,      {rabbit_registry, register, [exchange, <<"x-open">>, ?MODULE]}}
  ,{cleanup,  {rabbit_registry, unregister, [exchange, <<"x-open">>]}}
  ,{requires, rabbit_registry}
  ,{enables,  kernel_ready}
  ]}).
-rabbit_boot_step({rabbit_exchange_type_open_mnesia, [ {description, "exchange type x-open : mnesia"}
  ,{mfa,      {?MODULE, init_db, []}}
  ,{requires, database}
  ,{enables,  external_infrastructure}
  ]}).

description() ->
    [{description, <<"Rabbit extension : x-open exchange, route like you need">>}].

-define(RECORD, open_bindings).
-define(RECKEY, exchange_name).
-define(RECVALUE, bindings).
-record(?RECORD, {?RECKEY, ?RECVALUE}).
-define(TABLE, rabbit_open_bindings).

init_db() ->
    mnesia:create_table(?TABLE, [{record_name, ?RECORD},
                                 {attributes, record_info(fields, ?RECORD)},
                                 {type, ordered_set}]),
    mnesia:add_table_copy(?TABLE, node(), ram_copies),
    mnesia:wait_for_tables([?TABLE], 30000),
    ok.


-define(ONE_CHAR_AT_LEAST, _/utf8, _/binary).

info(_X) -> [].
info(_X, _) -> [].


serialise_events() -> false.

%
% This one is really heavy, we must find something neater
% The NIF way seems to be ok for that!
%
save_datetimes() ->
    TS = erlang:timestamp(),
% Local date time
    {YMD={Y,Mo,D},{H,Mi,S}} = calendar:now_to_local_time(TS),
    {_,W} = calendar:iso_week_number(YMD),
    Dw = calendar:day_of_the_week(YMD),
    DateAsString = lists:flatten(io_lib:format("~4..0w~2..0w~2..0w ~2..0w ~w ~2..0w~2..0w~2..0w", [Y,Mo, D, W, Dw, H, Mi, S])),
    put(xopen_dtl, DateAsString),
% Universal date time taken from before
    {UYMD={UY,UMo,UD},{UH,UMi,US}} = calendar:now_to_universal_time(TS),
    {_,UW} = calendar:iso_week_number(UYMD),
    UDw = calendar:day_of_the_week(UYMD),
    UDateAsString = lists:flatten(io_lib:format("~4..0w~2..0w~2..0w ~2..0w ~w ~2..0w~2..0w~2..0w", [UY,UMo, UD, UW, UDw, UH, UMi, US])),
    put(xopen_dtu, UDateAsString).

% Maybe we could use ets:tab2list here ?..... I don't know.
% And yes, maybe there is a cleaner way to list queues..
save_queues() ->
    AllQueues = mnesia:dirty_all_keys(rabbit_queue),
% We should drop amq.* queues also no ?! I don't see them here..
    AllVHQueues = [Q || Q = #resource{virtual_host = QueueVHost, kind = queue} <- AllQueues, QueueVHost == get(xopen_vhost)],
    put(xopen_allqs, AllVHQueues),
    AllVHQueues.

% Store msg ops once
save_msg_dops(Headers, VHost) ->
    FHeaders = flatten_table_msg(Headers, []),
    {DQAT, DEAT, DQREAT, DQNREAT} = pack_msg_ops(FHeaders, VHost),
    MsgDs = {ordsets:from_list(DQAT), ordsets:from_list(DEAT), DQREAT, DQNREAT},
    put(xopen_msg_ds, MsgDs),
    MsgDs.


%% Filter and flatten msg headers
flatten_table_msg([], Result) -> Result;
flatten_table_msg ([ {K = << "x-addq-ontrue" >>, array, Vs} | Tail ], Result) ->
        Res = [ { K, T, V } || {T = longstr, V = <<?ONE_CHAR_AT_LEAST>>} <- Vs ],
        flatten_table_msg (Tail, lists:append ([ Res , Result ]));
flatten_table_msg ([ {K = << "x-addq-ontrue" >>, T = longstr, V = <<?ONE_CHAR_AT_LEAST>>} | Tail ], Result) ->
        flatten_table_msg (Tail, [ {K, T, V} | Result ]);
flatten_table_msg ([ {K = << "x-adde-ontrue" >>, array, Vs} | Tail ], Result) ->
        Res = [ { K, T, V } || {T = longstr, V = <<?ONE_CHAR_AT_LEAST>>} <- Vs ],
        flatten_table_msg (Tail, lists:append ([ Res , Result ]));
flatten_table_msg ([ {K = << "x-adde-ontrue" >>, T = longstr, V = <<?ONE_CHAR_AT_LEAST>>} | Tail ], Result) ->
        flatten_table_msg (Tail, [ {K, T, V} | Result ]);
flatten_table_msg ([ {K = << "x-addqre-ontrue" >>, T = longstr, V = <<?ONE_CHAR_AT_LEAST>>} | Tail ], Result) ->
        flatten_table_msg (Tail, [ {K, T, V} | Result ]);
flatten_table_msg ([ {K = << "x-addq!re-ontrue" >>, T = longstr, V = <<?ONE_CHAR_AT_LEAST>>} | Tail ], Result) ->
        flatten_table_msg (Tail, [ {K, T, V} | Result ]);
flatten_table_msg ([ _ | Tail ], Result) ->
        flatten_table_msg (Tail, Result).

% Group msg ops
pack_msg_ops(Args, VHost) -> pack_msg_ops(Args, VHost, [],[],nil,nil).

pack_msg_ops([], _, DQAT, DEAT, DQREAT, DQNREAT) ->
    {DQAT, DEAT, DQREAT, DQNREAT};
pack_msg_ops([ {<<"x-addq-ontrue">>, _, V} | Tail ], VHost, DQAT, DEAT, DQREAT, DQNREAT) ->
    pack_msg_ops(Tail, VHost, [rabbit_misc:r(VHost, queue, V) | DQAT], DEAT, DQREAT, DQNREAT);
pack_msg_ops([ {<<"x-adde-ontrue">>, _, V} | Tail ], VHost, DQAT, DEAT, DQREAT, DQNREAT) ->
    pack_msg_ops(Tail, VHost, DQAT, [rabbit_misc:r(VHost, exchange, V) | DEAT], DQREAT, DQNREAT);
pack_msg_ops([ {<<"x-addqre-ontrue">>, _, V} | Tail ], VHost, DQAT, DEAT, _, DQNREAT) ->
    pack_msg_ops(Tail, VHost, DQAT, DEAT, V, DQNREAT);
pack_msg_ops([ {<<"x-addq!re-ontrue">>, _, V} | Tail ], VHost, DQAT, DEAT, DQREAT, _) ->
    pack_msg_ops(Tail, VHost, DQAT, DEAT, DQREAT, V).



route(#exchange{name = #resource{virtual_host = VHost} = Name},
      #delivery{message = #basic_message{content = Content, routing_keys = [RK | _]}}) ->
    put(xopen_vhost, VHost),
    erase(xopen_dtl),
    erase(xopen_allqs),
    erase(xopen_msg_ds),
    Headers = case (Content#content.properties)#'P_basic'.headers of
                  undefined -> [];
                  H         -> rabbit_misc:sort_field_table(H)
              end,
    CurrentOrderedBindings = case ets:lookup(?TABLE, Name) of
        [] -> [];
        [#?RECORD{?RECVALUE = Bs}] -> Bs
    end,
    get_routes({RK, Headers}, CurrentOrderedBindings, 0, ordsets:new()).


is_match(BindingType, MatchRk, RK, Args, Headers, nil) ->
    case BindingType of
        all -> is_match_rk(BindingType, MatchRk, RK) andalso is_match_hkv(BindingType, Args, Headers);
        any -> is_match_rk(BindingType, MatchRk, RK) orelse is_match_hkv(BindingType, Args, Headers)
    end;
is_match(BindingType, MatchRk, RK, Args, Headers, MatchDt) ->
    case get(xopen_dtl) of
        undefined -> save_datetimes();
        _ -> ok
    end,
    case BindingType of
        all -> is_match_dt(BindingType, MatchDt) andalso is_match_rk(BindingType, MatchRk, RK) andalso is_match_hkv(BindingType, Args, Headers);
        any -> is_match_rk(BindingType, MatchRk, RK) orelse is_match_hkv(BindingType, Args, Headers) orelse is_match_dt(BindingType, MatchDt)
    end.


% No more bindings; return dests
get_routes(_, [], _, ResDests) -> ordsets:to_list(ResDests);
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk, MatchDt}, << 0 >>, _} | T ], _, ResDests) ->
    case is_match(BindingType, MatchRk, RK, Args, Headers, MatchDt) of
        true -> get_routes(Data, T, 0, ordsets:add_element(Dest, ResDests));
           _ -> get_routes(Data, T, 0, ResDests)
    end;
% Simplest like direct's exchange type
get_routes(Data={RK, Headers}, [ {_, BindingType, _, {Args, MatchRk, MatchDt}, << 128 >>, _} | T ], _, ResDests) ->
    case is_match(BindingType, MatchRk, RK, Args, Headers, MatchDt) of
        true -> get_routes(Data, T, 0, ordsets:add_element(rabbit_misc:r(get(xopen_vhost), queue, RK), ResDests));
           _ -> get_routes(Data, T, 0, ResDests)
    end;
% Jump to the next binding satisfying the last goto operator
get_routes(Data, [ {Order, _, _, _, _, _, _} | T ], GotoOrder, ResDests) when GotoOrder > Order ->
    get_routes(Data, T, GotoOrder, ResDests);
% At this point, main dest is rewritten for next cases
get_routes(Data = {RK, _}, [ {Order, BindingType, _, MatchOps, << 128 >>, Other, BindingId} | T ], GotoOrder, ResDests) ->
    NewDest = rabbit_misc:r(get(xopen_vhost), queue, RK),
    get_routes(Data, [{Order, BindingType, NewDest, MatchOps, << 0 >>, Other, BindingId} | T], GotoOrder, ResDests);
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk, MatchDt}, _, {GOT, GOF, StopOperators}, _} | T ], _, ResDests) ->
    case {is_match(BindingType, MatchRk, RK, Args, Headers, MatchDt), StopOperators} of
        {true,{1,_}}  -> ordsets:add_element(Dest, ResDests);
        {false,{_,1}} -> ResDests;
        {true,_}      -> get_routes(Data, T, GOT, ordsets:add_element(Dest, ResDests));
        {false,_}     -> get_routes(Data, T, GOF, ResDests)
    end;
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk, MatchDt}, _, {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, nil, nil, nil, nil, nil, nil, nil, nil, <<0, 0>>}, _} | T ], _, ResDests) ->
    case {is_match(BindingType, MatchRk, RK, Args, Headers, MatchDt), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:union(DAT, ordsets:add_element(Dest, ResDests)), DDT);
        {false,{_,1}} -> ordsets:subtract(ordsets:union(DAF, ResDests), DDF);
        {true,_}      -> get_routes(Data, T, GOT, ordsets:subtract(ordsets:union(DAT, ordsets:add_element(Dest, ResDests)), DDT));
        {false,_}     -> get_routes(Data, T, GOF, ordsets:subtract(ordsets:union(DAF, ResDests), DDF))
    end;
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk, MatchDt}, _, {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, nil, nil, nil, nil, nil, nil, nil, nil, <<BindDest:8, 0>>}, _} | T ], _, ResDests) ->
    {MsgDQAT, MsgDEAT, _, _} = case get(xopen_msg_ds) of
        undefined -> save_msg_dops(Headers, get(xopen_vhost));
        V -> V
    end,
    MsgDQAT2 = case (BindDest bsr 7) band 1 of
        1 -> MsgDQAT;
        0 -> ordsets:from_list([])
    end,
    MsgDEAT2 = case (BindDest bsr 5) band 1 of
        1 -> MsgDEAT;
        0 -> ordsets:from_list([])
    end,
    case {is_match(BindingType, MatchRk, RK, Args, Headers, MatchDt), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:union([DAT, MsgDQAT2, MsgDEAT2, ordsets:add_element(Dest, ResDests)]), DDT);
        {false,{_,1}} -> ordsets:subtract(ordsets:union([DAF, ResDests]), DDF);
        {true,_}      -> get_routes(Data, T, GOT, ordsets:subtract(ordsets:union([DAT, MsgDQAT2, MsgDEAT2, ordsets:add_element(Dest, ResDests)]), DDT));
        {false,_}     -> get_routes(Data, T, GOF, ordsets:subtract(ordsets:union([DAF, ResDests]), DDF))
    end;
get_routes(Data={RK, Headers}, [ {_, BindingType, Dest, {Args, MatchRk, MatchDt}, _, {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE, <<BindDest:8, BindREDest:8>>}, _} | T ], _, ResDests) ->
    AllVHQueues = case get(xopen_allqs) of
        undefined -> save_queues();
        Res1 -> Res1
    end,
    {MsgDQAT, MsgDEAT, MsgDQREAT, MsgDQNREAT} = case get(xopen_msg_ds) of
        undefined -> save_msg_dops(Headers, get(xopen_vhost));
        Res2 -> Res2
    end,
    MsgDQAT2 = case (BindDest bsr 7) band 1 of
        1 -> MsgDQAT;
        0 -> ordsets:from_list([])
    end,
    MsgDEAT2 = case (BindDest bsr 5) band 1 of
        1 -> MsgDEAT;
        0 -> ordsets:from_list([])
    end,

    MsgDQREAT2 = case ((BindREDest bsr 7) band 1 == 1 andalso MsgDQREAT /= nil) of
        false -> ordsets:from_list([]);
        true -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MsgDQREAT, [ report_errors, {capture, none} ]) == match])
    end,
    MsgDQNREAT2 = case ((BindREDest bsr 6) band 1 == 1 andalso MsgDQNREAT /= nil) of
        false -> ordsets:from_list([]);
        true -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, MsgDQNREAT, [ report_errors, {capture, none} ]) /= match])
    end,

    DATREsult = case DATRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATRE, [ {capture, none} ]) == match])
    end,
    DAFREsult = case DAFRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DAFRE, [ {capture, none} ]) == match])
    end,
    DDTREsult = case DDTRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName, kind=queue} <- ResDests, re:run(QueueName, DDTRE, [ {capture, none} ]) == match])
    end,
    DDFREsult = case DDFRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName, kind=queue} <- ResDests, re:run(QueueName, DDFRE, [ {capture, none} ]) == match])
    end,
    DATNREsult = case DATNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DATNRE, [ {capture, none} ]) /= match])
    end,
    DAFNREsult = case DAFNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName} <- AllVHQueues, re:run(QueueName, DAFNRE, [ {capture, none} ]) /= match])
    end,
    DDTNREsult = case DDTNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName, kind=queue} <- ResDests, re:run(QueueName, DDTNRE, [ {capture, none} ]) /= match])
    end,
    DDFNREsult = case DDFNRE of
        nil -> ordsets:from_list([]);
        _ -> ordsets:from_list([Q || Q = #resource{name = QueueName, kind=queue} <- ResDests, re:run(QueueName, DDFNRE, [ {capture, none} ]) /= match])
    end,
    case {is_match(BindingType, MatchRk, RK, Args, Headers, MatchDt), StopOperators} of
        {true,{1,_}}  -> ordsets:subtract(ordsets:add_element(Dest, ordsets:union([DAT,DATREsult,DATNREsult,MsgDQAT2,MsgDEAT2,MsgDQREAT2,MsgDQNREAT2,ResDests])), ordsets:union([DDT,DDTREsult,DDTNREsult]));
        {false,{_,1}} -> ordsets:subtract(ordsets:union([DAF,DAFREsult,DAFNREsult,ResDests]), ordsets:union([DDF,DDFREsult,DDFNREsult]));
        {true,_}      -> get_routes(Data, T, GOT, ordsets:subtract(ordsets:add_element(Dest, ordsets:union([DAT,DATREsult,DATNREsult,MsgDQAT2,MsgDEAT2,MsgDQREAT2,MsgDQNREAT2,ResDests])), ordsets:union([DDT,DDTREsult,DDTNREsult])));
        {false,_}     -> get_routes(Data, T, GOF, ordsets:subtract(ordsets:union([DAF,DAFREsult,DAFNREsult,ResDests]), ordsets:union([DDF,DDFREsult,DDFNREsult])))
    end.


is_match_hkv(all, Args, Headers) ->
    is_match_hkv_all(Args, Headers);
is_match_hkv(any, Args, Headers) ->
    is_match_hkv_any(Args, Headers).


validate_binding(_X, #binding{args = Args, key = << >>, destination = Dest}) ->
    Args2 = rebuild_args(Args, Dest),
    case rabbit_misc:table_lookup(Args2, <<"x-match">>) of
        {longstr, <<"all">>} -> validate_list_type_usage(all, Args2);
        {longstr, <<"any">>} -> validate_list_type_usage(any, Args2);
        {longstr, Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field value ~p; "
                                  "expected all or any", [Other]}};
        {Type,    Other}     -> {error,
                                 {binding_invalid,
                                  "Invalid x-match field type ~p (value ~p); "
                                  "expected longstr", [Type, Other]}};
        undefined            -> validate_list_type_usage(all, Args2)
    end;
validate_binding(_X, _) ->
    {error, {binding_invalid, "Invalid binding's routing key declaration in x-open exchange", []}}.


%% Binding's header keys of type 'list' must be validated
validate_list_type_usage(BindingType, Args) ->
    validate_list_type_usage(BindingType, Args, Args).

validate_list_type_usage(_, [], Args) -> validate_no_deep_lists(Args);
% Routing key ops
validate_list_type_usage(all, [ {<<"x-?rk=", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with routing key = operator with binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-?rk!=", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with routing key != operator with binding type 'any'", []}};

validate_list_type_usage(all, [ {<<"x-?hkv= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with = operator with binding type 'all'", []}};
validate_list_type_usage(any, [ {<<"x-?hkv!= ", _/binary>>, array, _} | _ ], _) ->
    {error, {binding_invalid, "Invalid use of list type with != operator with binding type 'any'", []}};
% Routing facilities
validate_list_type_usage(_, [ {<< RuleKey:9/binary, _/binary >>, array, _} | _], _) when RuleKey==<<"x-addqre-">> ; RuleKey==<<"x-delqre-">> ->
    {error, {binding_invalid, "Invalid use of list type with regex in routing facilities", []}};
validate_list_type_usage(_, [ {<< RuleKey:10/binary, _/binary >>, array, _} | _], _) when RuleKey==<<"x-addq!re-">> ; RuleKey==<<"x-delq!re-">> ->
    {error, {binding_invalid, "Invalid use of list type with regex in routing facilities", []}};

validate_list_type_usage(BindingType, [ {<< RuleKey/binary >>, array, _} | Tail ], Args) ->
    RKL = binary_to_list(RuleKey),
    MatchOperators = ["x-?hkv<", "x-?hkv>", "x-?dture", "x-?dtunre", "x-?dtlre", "x-?dtlnre"],
    case lists:filter(fun(S) -> lists:prefix(S, RKL) end, MatchOperators) of
        [] -> validate_list_type_usage(BindingType, Tail, Args);
        _ -> {error, {binding_invalid, "Invalid use of list type with < or > operators and datetime related", []}}
    end;
% Else go next
validate_list_type_usage(BindingType, [ _ | Tail ], Args) ->
    validate_list_type_usage(BindingType, Tail, Args).


%% Binding can't have array in array :
validate_no_deep_lists(Args) -> 
    validate_no_deep_lists(Args, [], Args).

validate_no_deep_lists([], [], Args) -> validate_operators(Args);
validate_no_deep_lists(_, [_ | _], _) ->
    {error, {binding_invalid, "Invalid use of list in list", []}};
validate_no_deep_lists([ {_, array, Vs} | Tail ], _, Args) ->
    ArrInArr = [ bad || {array, _} <- Vs ],
    validate_no_deep_lists(Tail, ArrInArr, Args); 
validate_no_deep_lists([ _ | Tail ], _, Args) ->
    validate_no_deep_lists(Tail, [], Args).



%% Binding is INvalidated if some rule does not match anything,
%%  so that we can't have some "unexpected" results
validate_operators(Args) ->
  FlattenedArgs = flatten_table(Args),
  case validate_operators2(FlattenedArgs) of
    ok -> validate_regexes(Args);
    Err -> Err
  end.

validate_operators2([]) -> ok;
% x-match have been checked first, so that we don't check type or value
validate_operators2([ {<<"x-match">>, _, _} | Tail ]) -> validate_operators2(Tail);
% Decimal type is a pain to compare with other numeric types's values, so this is forbidden.
validate_operators2([ {_, decimal, _} | _ ]) ->
    {error, {binding_invalid, "Decimal type is invalid in x-open", []}};
% Do not think that can't happen... it can ! :)
validate_operators2([ {<<>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's rule key can't be void", []}};

% Datettime match ops
validate_operators2([ {<<"x-?dture">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?dtunre">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?dtlre">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?dtlnre">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);

% Routing key ops
validate_operators2([ {<<"x-?rk=">>, longstr, <<_/binary>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?rk!=">>, longstr, <<_/binary>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?rkre">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?rk!re">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);

validate_operators2([ {<<"x-?hkex">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hknx">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);

% Dests ops (exchanges)
validate_operators2([ {<<"x-adde-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-adde-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-dele-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-dele-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
% Dests ops (queues)
validate_operators2([ {<<"x-addq-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addqre-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addq!re-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addq-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addqre-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-addq!re-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delq-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delqre-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delq!re-ontrue">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delq-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delqre-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-delq!re-onfalse">>, longstr, <<?ONE_CHAR_AT_LEAST>>} | Tail ]) -> validate_operators2(Tail);
% Dests ops (msg)
validate_operators2([ {<<"x-msg-addq-ontrue">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-msg-addqre-ontrue">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-msg-addq!re-ontrue">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-msg-adde-ontrue">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);

validate_operators2([ {<<"x-msg-destq-rk">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);

% Binding order
validate_operators2([ {<<"x-order">>, _, V} | Tail ]) when is_integer(V), V > 1999, V < 16000 -> validate_operators2(Tail);
validate_operators2([ {<<"x-order">>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's order must be an integer between 2000 and 15999", []}};

% Gotos
validate_operators2([ {<<"x-goto-ontrue">>, _, V} | Tail ]) when is_integer(V), V > 1999, V < 16000 -> validate_operators2(Tail);
validate_operators2([ {<<"x-goto-ontrue">>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's goto must be an integer between 2000 and 15999", []}};
validate_operators2([ {<<"x-goto-onfalse">>, _, V} | Tail ]) when is_integer(V), V > 1999, V < 16000 -> validate_operators2(Tail);
validate_operators2([ {<<"x-goto-onfalse">>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's goto must be an integer between 2000 and 15999", []}};

% Stops
validate_operators2([ {<<"x-stop-ontrue">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-stop-onfalse">>, longstr, <<>>} | Tail ]) -> validate_operators2(Tail);

% Operators hkv with < or > must be numeric only.
validate_operators2([ {<<"x-?hkv<= ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv<= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-?hkv< ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv< ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-?hkv>= ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv>= ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};
validate_operators2([ {<<"x-?hkv> ", ?ONE_CHAR_AT_LEAST>>, _, V} | Tail ]) when is_number(V) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv> ", ?ONE_CHAR_AT_LEAST>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Type's value of comparison's operators < and > must be numeric", []}};

validate_operators2([ {<<"x-?hkv= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkvre ", ?ONE_CHAR_AT_LEAST>>, longstr, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv!= ", ?ONE_CHAR_AT_LEAST>>, _, _} | Tail ]) -> validate_operators2(Tail);
validate_operators2([ {<<"x-?hkv!re ", ?ONE_CHAR_AT_LEAST>>, longstr, _} | Tail ]) -> validate_operators2(Tail);

validate_operators2([ {InvalidKey = <<"x-", _/binary>>, _, _} | _ ]) ->
    {error, {binding_invalid, "Binding's key ~p cannot start with 'x-' in x-open exchange; use new operators to match such keys", [InvalidKey]}};
validate_operators2([ _ | Tail ]) -> validate_operators2(Tail).


validate_regexes_item(RegexBin, Tail) ->
    case re:compile(RegexBin) of
        {ok, _} -> validate_regexes(Tail);
        _ -> {error, {binding_invalid, "Regex '~ts' is invalid", [RegexBin]}}
    end.

validate_regexes([]) -> ok;
validate_regexes([ {<< RuleKey:8/binary, _/binary >>, longstr, << RegexBin/binary >>} | Tail ]) when RuleKey==<<"x-?hkvre">> ; RuleKey==<<"x-addqre">> ; RuleKey==<<"x-delqre">> ->
    validate_regexes_item(RegexBin, Tail);
validate_regexes([ {<< RuleKey:9/binary, _/binary >>, longstr, << RegexBin/binary >>} | Tail ]) when RuleKey==<<"x-?hkv!re">> ; RuleKey==<<"x-addq!re">> ; RuleKey==<<"x-delq!re">> ->
    validate_regexes_item(RegexBin, Tail);
validate_regexes([ _ | Tail ]) ->
        validate_regexes(Tail).


%% By default the binding type is 'all'; and that's it :)
parse_x_match({longstr, <<"all">>}) -> all;
parse_x_match({longstr, <<"any">>}) -> any;
parse_x_match(_)                    -> all.


%
% Check datetime operators
%

is_match_dt(all, Rules) ->
    is_match_dt_all(Rules);
is_match_dt(any, Rules) ->
    is_match_dt_any(Rules).

% With 'all' binding type
% No (more) match to chek, return true
is_match_dt_all([]) -> true;
is_match_dt_all([ {dture, V} | Tail]) ->
    case re:run(get(xopen_dtu), V, [ {capture, none} ]) of
        match -> is_match_dt_all(Tail);
        _ -> false
    end;
is_match_dt_all([ {dtunre, V} | Tail]) ->
    case re:run(get(xopen_dtu), V, [ {capture, none} ]) of
        match -> false;
        _ -> is_match_dt_all(Tail)
    end;
is_match_dt_all([ {dtlre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> is_match_dt_all(Tail);
        _ -> false
    end;
is_match_dt_all([ {dtlnre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> false;
        _ -> is_match_dt_all(Tail)
    end.
% With 'any' binding type
is_match_dt_any([]) -> false;
is_match_dt_any([ {dture, V} | Tail]) ->
    case re:run(get(xopen_dtu), V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_dt_any(Tail)
    end;
is_match_dt_any([ {dtunre, V} | Tail]) ->
    case re:run(get(xopen_dtu), V, [ {capture, none} ]) of
        match -> is_match_dt_any(Tail);
        _ -> true
    end;
is_match_dt_any([ {dtlre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_dt_any(Tail)
    end;
is_match_dt_any([ {dtlnre, V} | Tail]) ->
    case re:run(get(xopen_dtl), V, [ {capture, none} ]) of
        match -> is_match_dt_any(Tail);
        _ -> true
    end.



%
% Check RK operators with current routing key from message
%

is_match_rk(all, Rules, RK) ->
    is_match_rk_all(Rules, RK);
is_match_rk(any, Rules, RK) ->
    is_match_rk_any(Rules, RK).

% With 'all' binding type
% No (more) match to chek, return true
is_match_rk_all([], _) -> true;
% Case RK must be equal
is_match_rk_all([ {rkeq, V} | Tail], RK) when V == RK ->
    is_match_rk_all(Tail, RK);
% Case RK must not be equal
is_match_rk_all([ {rkne, V} | Tail], RK) when V /= RK ->
    is_match_rk_all(Tail, RK);
% Case RK must match regex
is_match_rk_all([ {rkre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> is_match_rk_all(Tail, RK);
        _ -> false
    end;
% Case RK must not match regex
is_match_rk_all([ {rknre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> false;
        _ -> is_match_rk_all(Tail, RK)
    end;
% rkeq or rkne are false..
is_match_rk_all(_, _) ->
    false.

% With 'any' binding type
% No (more) match to chek, return false
is_match_rk_any([], _) -> false;
% Case RK must be equal
is_match_rk_any([ {rkeq, V} | _], RK) when V == RK -> true;
% Case RK must not be equal
is_match_rk_any([ {rkne, V} | _], RK) when V /= RK -> true;
% Case RK must match regex
is_match_rk_any([ {rkre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_rk_any(Tail, RK)
    end;
% Case RK must not match regex
is_match_rk_any([ {rknre, V} | Tail], RK) ->
    case re:run(RK, V, [ {capture, none} ]) of
        match -> is_match_rk_any(Tail, RK);
        _ -> true
    end;
% rkeq or rkne are false..
is_match_rk_any([ _ | Tail], RK) ->
    is_match_rk_any(Tail, RK).






%%
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%% REQUIRES BOTH PATTERN AND DATA TO BE SORTED ASCENDING BY KEY.
%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
%%

%% Binding type 'all' match

% No more match operator to check; return true
is_match_hkv_all([], _) -> true;

% Purge nx op on no data as all these are true
is_match_hkv_all([{_, nx, _} | BNext], []) ->
    is_match_hkv_all(BNext, []);

% No more message header but still match operator to check; return false
is_match_hkv_all(_, []) -> false;

% Current header key not in match operators; go next header with current match operator
is_match_hkv_all(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> is_match_hkv_all(BCur, HNext);
% Current binding key must not exist in data, go next binding
is_match_hkv_all([{BK, nx, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> is_match_hkv_all(BNext, HCur);
% Current match operator does not exist in message; return false
is_match_hkv_all([{BK, _, _} | _], [{HK, _, _} | _])
    when BK < HK -> false;
%
% From here, BK == HK (keys are the same)
%
% Current values must match and do match; ok go next
is_match_hkv_all([{_, eq, BV} | BNext], [{_, _, HV} | HNext])
    when BV == HV -> is_match_hkv_all(BNext, HNext);
% Current values must match but do not match; return false
is_match_hkv_all([{_, eq, _} | _], _) -> false;
% Key must not exist, return false
is_match_hkv_all([{_, nx, _} | _], _) -> false;
% Current header key must exist; ok go next
is_match_hkv_all([{_, ex, _} | BNext], [ _ | HNext]) ->
    is_match_hkv_all(BNext, HNext);
% <= < = != > >=
is_match_hkv_all([{_, ne, BV} | BNext], HCur = [{_, _, HV} | _])
    when BV /= HV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, ne, _} | _], _) -> false;

% Thanks to validation done upstream, gt/ge/lt/le are done only for numeric
is_match_hkv_all([{_, gt, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV > BV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, gt, _} | _], _) -> false;
is_match_hkv_all([{_, ge, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV >= BV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, ge, _} | _], _) -> false;
is_match_hkv_all([{_, lt, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV < BV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, lt, _} | _], _) -> false;
is_match_hkv_all([{_, le, BV} | BNext], HCur = [{_, _, HV} | _])
    when is_number(HV), HV =< BV -> is_match_hkv_all(BNext, HCur);
is_match_hkv_all([{_, le, _} | _], _) -> false;

% Regexes
is_match_hkv_all([{_, re, BV} | BNext], HCur = [{_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> is_match_hkv_all(BNext, HCur);
        _ -> false
    end;
% Message header value is not a string : regex returns always false :
is_match_hkv_all([{_, re, _} | _], _) -> false;
is_match_hkv_all([{_, nre, BV} | BNext], HCur = [{_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        nomatch -> is_match_hkv_all(BNext, HCur);
        _ -> false
    end;
% Message header value is not a string : regex returns always false :
is_match_hkv_all([{_, nre, _} | _], _) -> false.



%% Binding type 'any' match

% No more match operator to check; return false
is_match_hkv_any([], _) -> false;
% No more message header but still match operator to check; return false
is_match_hkv_any(_, []) -> false;
% Current header key not in match operators; go next header with current match operator
is_match_hkv_any(BCur = [{BK, _, _} | _], [{HK, _, _} | HNext])
    when BK > HK -> is_match_hkv_any(BCur, HNext);
% Current binding key must not exist in data, return true
is_match_hkv_any([{BK, nx, _} | _], [{HK, _, _} | _])
    when BK < HK -> true;
% Current binding key does not exist in message; go next binding
is_match_hkv_any([{BK, _, _} | BNext], HCur = [{HK, _, _} | _])
    when BK < HK -> is_match_hkv_any(BNext, HCur);
%
% From here, BK == HK
%
% Current values must match and do match; return true
is_match_hkv_any([{_, eq, BV} | _], [{_, _, HV} | _]) when BV == HV -> true;
% Current header key must exist; return true
is_match_hkv_any([{_, ex, _} | _], _) -> true;
is_match_hkv_any([{_, ne, BV} | _], [{_, _, HV} | _]) when HV /= BV -> true;
is_match_hkv_any([{_, gt, BV} | _], [{_, _, HV} | _]) when HV > BV -> true;
is_match_hkv_any([{_, ge, BV} | _], [{_, _, HV} | _]) when HV >= BV -> true;
is_match_hkv_any([{_, lt, BV} | _], [{_, _, HV} | _]) when HV < BV -> true;
is_match_hkv_any([{_, le, BV} | _], [{_, _, HV} | _]) when HV =< BV -> true;

% Regexes
is_match_hkv_any([{_, re, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> true;
        _ -> is_match_hkv_any(BNext, HCur)
    end;
is_match_hkv_any([{_, nre, BV} | BNext], HCur = [ {_, longstr, HV} | _]) ->
    case re:run(HV, BV, [ {capture, none} ]) of
        match -> is_match_hkv_any(BNext, HCur);
        _ -> true
    end;
% No match yet; go next
is_match_hkv_any([_ | BNext], HCur) ->
    is_match_hkv_any(BNext, HCur).


topic_amqp_to_re(TopicStr) -> topic_amqp_to_re([ $^ | erlang:binary_to_list(TopicStr)], 0).
topic_amqp_to_reci(TopicStr) -> topic_amqp_to_re([ "^(?i)" | erlang:binary_to_list(TopicStr)], 0).
topic_amqp_to_re(TopicStr, 0) ->
    NewTopicStr = string:replace(TopicStr, "*", "(\\w+)", all),
    topic_amqp_to_re(NewTopicStr, 1);
topic_amqp_to_re(TopicStr, 1) ->
    NewTopicStr = string:replace(TopicStr, "#.", "(\\w+.)*"),
    topic_amqp_to_re(NewTopicStr, 2);
topic_amqp_to_re(TopicStr, 2) ->
    NewTopicStr = string:replace(TopicStr, ".#", "(.\\w+)*"),
    topic_amqp_to_re(NewTopicStr, 3);
topic_amqp_to_re(TopicStr, 3) ->
    NewTopicStr = string:replace(TopicStr, ".", "\\.", all),
    topic_amqp_to_re(NewTopicStr, 4);
topic_amqp_to_re(TopicStr, 4) ->
    NewTopicStr = string:replace(TopicStr, "#", ".*"),
    topic_amqp_to_re(NewTopicStr, 5);
topic_amqp_to_re(TopicStr, 5) ->
    lists:flatten([TopicStr, "$"]).



% This fun transforms some rule by some other
rebuild_args(Args, Dest) -> rebuild_args(Args, [], Dest).

rebuild_args([], Ret, _) -> Ret;
rebuild_args([ {<<"x-dest-del">>, longstr, <<>>} | Tail ], Ret, Dest = #resource{kind = queue, name = RName} ) ->
    rebuild_args(Tail, [ {<<"x-delq-ontrue">>, longstr, RName} | Ret], Dest);
rebuild_args([ {<<"x-dest-del">>, longstr, <<>>} | Tail ], Ret, Dest = #resource{kind = exchange, name = RName} ) ->
    rebuild_args(Tail, [ {<<"x-dele-ontrue">>, longstr, RName} | Ret], Dest);
rebuild_args([ {<<"x-?rkta">>, longstr, Topic} | Tail ], Ret, Dest) ->
    ZZZ = topic_amqp_to_re(Topic),
    rebuild_args(Tail, [ {<<"x-?rkre">>, longstr, erlang:list_to_binary(ZZZ)} | Ret], Dest);
rebuild_args([ {<<"x-?rktaci">>, longstr, Topic} | Tail ], Ret, Dest) ->
    ZZZ = topic_amqp_to_reci(Topic),
    rebuild_args(Tail, [ {<<"x-?rkre">>, longstr, erlang:list_to_binary(ZZZ)} | Ret], Dest);
rebuild_args([ Op | Tail ], Ret, Dest) ->
    rebuild_args(Tail, [ Op | Ret], Dest).



get_match_hk_ops(BindingArgs) ->
    MatchOperators = get_match_hk_ops(BindingArgs, []),
    rabbit_misc:sort_field_table(MatchOperators).

% Get match operators
% We won't check types again as this has been done during validation..

% Get match operators based on headers
get_match_hk_ops([], Result) -> Result;
% Does a key exist ?
get_match_hk_ops([ {<<"x-?hkex">>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {V, ex, nil} | Res]);
% Does a key NOT exist ?
get_match_hk_ops([ {<<"x-?hknx">>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {V, nx, nil} | Res]);

% operators <= < = != > >=
get_match_hk_ops([ {<<"x-?hkv<= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, le, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv< ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, lt, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, eq, V} | Res]);
get_match_hk_ops([ {<<"x-?hkvre ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, re, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hkv!= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ne, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv!re ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, nre, binary_to_list(V)} | Res]);
get_match_hk_ops([ {<<"x-?hkv> ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, gt, V} | Res]);
get_match_hk_ops([ {<<"x-?hkv>= ", K/binary>>, _, V} | Tail ], Res) ->
    get_match_hk_ops (Tail, [ {K, ge, V} | Res]);

%% We should not found here another header beginning with x-? !!!

%% All others beginnig with x- are other operators
get_match_hk_ops([ {<<"x-", _/binary>>, _, _} | Tail ], Res) ->
    get_match_hk_ops (Tail, Res);
% And for all other cases, the match operator is 'eq'
get_match_hk_ops([ {K, _, V} | T ], Res) ->
    get_match_hk_ops (T, [ {K, eq, V} | Res]).


% Get match operators related to routing key
get_match_rk_ops([], Result) -> Result;

get_match_rk_ops([ {<<"x-?rk=">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkeq, V} | Res]);
get_match_rk_ops([ {<<"x-?rk!=">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkne, V} | Res]);
get_match_rk_ops([ {<<"x-?rkre">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rkre, V} | Res]);
get_match_rk_ops([ {<<"x-?rk!re">>, _, <<V/binary>>} | Tail ], Res) ->
    get_match_rk_ops(Tail, [ {rknre, V} | Res]);
get_match_rk_ops([ _ | Tail ], Result) ->
    get_match_rk_ops(Tail, Result).


% Validation is made upstream
get_binding_order(Args, Default) ->
    case rabbit_misc:table_lookup(Args, <<"x-order">>) of
        undefined     -> Default;
        {_, Order} -> Order
    end.

% Datetime match ops
get_match_dt_ops([], []) -> nil;
get_match_dt_ops([], Result) -> Result;
get_match_dt_ops([{<<"x-?dture">>, _, <<V/binary>>} | T], Res) ->
    get_match_dt_ops(T, [{dture, V} | Res]);
get_match_dt_ops([{<<"x-?dtunre">>, _, <<V/binary>>} | T], Res) ->
    get_match_dt_ops(T, [{dtunre, V} | Res]);
get_match_dt_ops([{<<"x-?dtlre">>, _, <<V/binary>>} | T], Res) ->
    get_match_dt_ops(T, [{dtlre, V} | Res]);
get_match_dt_ops([{<<"x-?dtlnre">>, _, <<V/binary>>} | T], Res) ->
    get_match_dt_ops(T, [{dtlnre, V} | Res]);
get_match_dt_ops([_ | T], R) ->
    get_match_dt_ops(T, R).

% Validation is made upstream
get_stop_operators([], Result) -> Result;
get_stop_operators([{<<"x-stop-ontrue">>, _, _} | T], {_, StopOnFalse}) ->
    get_stop_operators(T, {1, StopOnFalse});
get_stop_operators([{<<"x-stop-onfalse">>, _, _} | T], {StopOnTrue, _}) ->
    get_stop_operators(T, {StopOnTrue, 1});
get_stop_operators([_ | T], R) ->
    get_stop_operators(T, R).

% Validation is made upstream
get_goto_operators([], Result) -> Result;
get_goto_operators([{<<"x-goto-ontrue">>, _, N} | T], {_, GotoOnFalse}) ->
    get_goto_operators(T, {N, GotoOnFalse});
get_goto_operators([{<<"x-goto-onfalse">>, _, N} | T], {GotoOnTrue, _}) ->
    get_goto_operators(T, {GotoOnTrue, N});
get_goto_operators([_ | T], R) ->
    get_goto_operators(T, R).


%% DAT : Destinations to Add on True
%% DAF : Destinations to Add on False
%% DDT : Destinations to Del on True
%% DDF : Destinations to Del on False
% They are resource's names or regex
get_dests_operators(VHost, Args) ->
    OS = ordsets:new(),
    get_dests_operators(VHost, Args, {OS, OS, OS, OS}, {nil, nil, nil, nil, nil, nil, nil, nil}).

get_dests_operators(_, [], Dests, DestsRE) -> {Dests, DestsRE};
get_dests_operators(VHost, [{<<"x-addq-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {ordsets:add_element(R,DAT), DAF, DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-adde-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {ordsets:add_element(R,DAT), DAF, DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-addq-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, ordsets:add_element(R,DAF), DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-adde-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, ordsets:add_element(R,DAF), DDT, DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-delq-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, DAF, ordsets:add_element(R,DDT), DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-dele-ontrue">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, DAF, ordsets:add_element(R,DDT), DDF}, DestsRE);
get_dests_operators(VHost, [{<<"x-delq-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, queue, D),
    get_dests_operators(VHost, T, {DAT, DAF, DDT, ordsets:add_element(R,DDF)}, DestsRE);
get_dests_operators(VHost, [{<<"x-dele-onfalse">>, longstr, D} | T], {DAT,DAF,DDT,DDF}, DestsRE) ->
    R = rabbit_misc:r(VHost, exchange, D),
    get_dests_operators(VHost, T, {DAT, DAF, DDT, ordsets:add_element(R,DDF)}, DestsRE);
% Regex part
get_dests_operators(VHost, [{<<"x-addqre-ontrue">>, longstr, R} | T], Dests, {_,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {R, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addqre-onfalse">>, longstr, R} | T], Dests, {DATRE,_,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, R, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delqre-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,_,DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, R, DDFRE,DATNRE,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delqre-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,_,DATNRE,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, R,DATNRE,DAFNRE,DDTNRE,DDFNRE});

get_dests_operators(VHost, [{<<"x-addq!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,_,DAFNRE,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,R,DAFNRE,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-addq!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,_,DDTNRE,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,R,DDTNRE,DDFNRE});
get_dests_operators(VHost, [{<<"x-delq!re-ontrue">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,_,DDFNRE}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,R,DDFNRE});
get_dests_operators(VHost, [{<<"x-delq!re-onfalse">>, longstr, R} | T], Dests, {DATRE,DAFRE,DDTRE,DDFRE,DATNRE,DAFNRE,DDTNRE,_}) ->
    get_dests_operators(VHost, T, Dests, {DATRE, DAFRE, DDTRE, DDFRE,DATNRE,DAFNRE,DDTNRE,R});
get_dests_operators(VHost, [_ | T], Dests, DestsRE) ->
    get_dests_operators(VHost, T, Dests, DestsRE).



% Operators decided by the message (by the producer)
get_msg_ops([], Result) -> Result;
get_msg_ops([{<<"x-msg-destq-rk">>, _, _} | T], << D:8, DRE:8, Opt:8 >>) ->
    get_msg_ops(T, << D, DRE, (Opt + 128) >>);
get_msg_ops([{<<"x-msg-addq-ontrue">>, _, _} | T], << D:8, DRE:8, Opt:8 >>) ->
    get_msg_ops(T, << (D + 128), DRE, Opt >>);
get_msg_ops([{<<"x-msg-adde-ontrue">>, _, _} | T], << D:8, DRE:8, Opt:8 >>) ->
    get_msg_ops(T, << (D + 32), DRE, Opt >>);
get_msg_ops([{<<"x-msg-addqre-ontrue">>, _, _} | T], << D:8, DRE:8, Opt:8 >>) ->
    get_msg_ops(T, << D, (DRE + 128), Opt >>);
get_msg_ops([{<<"x-msg-addq!re-ontrue">>, _, _} | T], << D:8, DRE:8, Opt:8 >>) ->
    get_msg_ops(T, << D, (DRE + 64), Opt >>);
get_msg_ops([_ | T], Result) ->
    get_msg_ops(T, Result).


%% Flatten one level for list of values (array)
flatten_table(Args) ->
    flatten_table(Args, []).

flatten_table([], Result) -> Result;
flatten_table ([ {K, array, Vs} | Tail ], Result) ->
        Res = [ { K, T, V } || {T, V} <- Vs ],
        flatten_table (Tail, lists:append ([ Res , Result ]));
flatten_table ([ {K, T, V} | Tail ], Result) ->
        flatten_table (Tail, [ {K, T, V} | Result ]).


validate(_X) -> ok.
create(_Tx, _X) -> ok.

delete(transaction, #exchange{name = XName}, _) ->
    ok = mnesia:delete (?TABLE, XName, write);
delete(_, _, _) -> ok.

policy_changed(_X1, _X2) -> ok.


add_binding(transaction, #exchange{name = #resource{virtual_host = VHost} = XName}, BindingToAdd = #binding{destination = Dest, args = BindingArgs}) ->
% BindingId is used to track original binding definition so that it is used when deleting later
    BindingId = crypto:hash(md5, term_to_binary(BindingToAdd)),
% Let's doing that heavy lookup one time only
    BindingType = parse_x_match(rabbit_misc:table_lookup(BindingArgs, <<"x-match">>)),
    BindingOrder = get_binding_order(BindingArgs, 9000),
    {GOT, GOF} = get_goto_operators(BindingArgs, {0, 0}),
    StopOperators = get_stop_operators(BindingArgs, {0, 0}),
    << MsgDests:8, MsgREDests:8, MsgOpt:8 >> = get_msg_ops(BindingArgs, << 0, 0, 0 >>),
    FlattenedBindindArgs = flatten_table(rebuild_args(BindingArgs, Dest)),
    MatchHKOps = get_match_hk_ops(FlattenedBindindArgs),
    MatchRKOps = get_match_rk_ops(FlattenedBindindArgs, []),
    MatchDTOps = get_match_dt_ops(FlattenedBindindArgs, []),
    MatchOps = {MatchHKOps, MatchRKOps, MatchDTOps},
    {{DAT, DAF, DDT, DDF}, {DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE}} = get_dests_operators(VHost, FlattenedBindindArgs),
    CurrentOrderedBindings = case mnesia:read(?TABLE, XName, write) of
        [] -> [];
        [#?RECORD{?RECVALUE = E}] -> E
    end,
    NewBinding1 = {BindingOrder, BindingType, Dest, MatchOps, << MsgOpt:8 >>},
    NewBinding2 = case {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE, << MsgDests:8, MsgREDests:8 >>} of
        {0, 0, {0, 0}, [], [], [], [], nil, nil, nil, nil, nil, nil, nil, nil, <<0, 0>>} -> NewBinding1;
        {_, _, _, [], [], [], [], nil, nil, nil, nil, nil, nil, nil, nil, <<0, 0>>} -> erlang:append_element(NewBinding1, {GOT, GOF, StopOperators});
        _ -> erlang:append_element(NewBinding1, {GOT, GOF, StopOperators, DAT, DAF, DDT, DDF, DATRE, DAFRE, DDTRE, DDFRE, DATNRE, DAFNRE, DDTNRE, DDFNRE, << MsgDests:8, MsgREDests:8 >>})
    end,
    NewBinding = erlang:append_element(NewBinding2, BindingId),
    NewBindings = lists:keysort(1, [NewBinding | CurrentOrderedBindings]),
    NewRecord = #?RECORD{?RECKEY = XName, ?RECVALUE = NewBindings},
    ok = mnesia:write(?TABLE, NewRecord, write);
add_binding(_, _, _) ->
    ok.


remove_bindings(transaction, #exchange{name = XName}, BindingsToDelete) ->
    CurrentOrderedBindings = case mnesia:read(?TABLE, XName, write) of
        [] -> [];
        [#?RECORD{?RECVALUE = E}] -> E
    end,
    BindingIdsToDelete = [crypto:hash(md5, term_to_binary(B)) || B <- BindingsToDelete],
    NewOrderedBindings = remove_bindings_ids(BindingIdsToDelete, CurrentOrderedBindings, []),
    NewRecord = #?RECORD{?RECKEY = XName, ?RECVALUE = NewOrderedBindings},
    ok = mnesia:write(?TABLE, NewRecord, write);
remove_bindings(_, _, _) ->
    ok.

remove_bindings_ids(_, [], Res) -> Res;
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end;
remove_bindings_ids(BindingIdsToDelete, [Bind = {_,_,_,_,_,BId} | T], Res) ->
    case lists:member(BId, BindingIdsToDelete) of
        true -> remove_bindings_ids(BindingIdsToDelete, T, Res);
        _    -> remove_bindings_ids(BindingIdsToDelete, T, lists:append(Res, [Bind]))
    end.


assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

