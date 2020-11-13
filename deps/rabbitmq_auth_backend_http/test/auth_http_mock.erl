%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(auth_http_mock).

-export([init/2]).

%%% CALLBACKS

init(Req = #{method := <<"GET">>}, Users) ->
    QsVals = cowboy_req:parse_qs(Req),
    Reply = authenticate(proplists:get_value(<<"username">>, QsVals),
                         proplists:get_value(<<"password">>, QsVals),
                         Users),
    Req2 = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, Reply, Req),
    {ok, Req2, Users}.

%%% HELPERS

authenticate(Username, Password, Users) ->
   case maps:get(Username, Users, undefined) of
       {MatchingPassword, Tags} when Password =:= MatchingPassword ->
           StringTags = lists:map(fun(T) -> io_lib:format("~s", [T]) end, Tags),
           <<"allow ", (list_to_binary(string:join(StringTags, " ")))/binary>>;
        {_OtherPassword, _} ->
            <<"deny">>;
        undefined ->
            <<"deny">>
   end.
