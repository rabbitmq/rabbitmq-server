-module(auth_http_mock).

-export([init/2]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% CALLBACKS

init(Req = #{method := <<"GET">>}, Users) ->
    QsVals = cowboy_req:parse_qs(Req),
    Reply = authenticate(QsVals, Users),
    Req2 = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, Reply, Req),
    {ok, Req2, Users}.

%%% HELPERS

authenticate(QsVals, Users) ->
   Username = proplists:get_value(<<"username">>, QsVals),
   Password = proplists:get_value(<<"password">>, QsVals),
   case maps:get(Username, Users, undefined) of
       {MatchingPassword, Tags, ExpectedCredentials} when Password =:= MatchingPassword ->
            case lists:all(fun(C) -> proplists:is_defined(list_to_binary(rabbit_data_coercion:to_list(C)),QsVals) end, ExpectedCredentials) of
              true -> StringTags = lists:map(fun(T) -> io_lib:format("~ts", [T]) end, Tags),
                      <<"allow ", (list_to_binary(string:join(StringTags, " ")))/binary>>;
              false -> ct:log("Missing required attributes. Expected ~p, Found: ~p", [ExpectedCredentials, QsVals]),
                       <<"deny">>
            end;
        {_OtherPassword, _, _} ->
            <<"deny">>;
        undefined ->
            <<"deny">>
   end.
