%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_util).

-export([is_authorized/2, now_ms/0, http_date/0, vhost/1, vhost_exists/1]).
-export([bad_request/3, id/2, decode/2]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

is_authorized(ReqData, Context) ->
    Unauthorized = {"Basic realm=\"RabbitMQ Management Console\"",
                    ReqData, Context},
    case wrq:get_req_header("authorization", ReqData) of
        "Basic " ++ Base64 ->
            Str = base64:mime_decode_to_string(Base64),
            [User, Pass] = string:tokens(Str, ":"),
            case rabbit_access_control:lookup_user(list_to_binary(User)) of
                {ok, U}  -> case list_to_binary(Pass) == U#user.password of
                                true ->  {true, ReqData, Context};
                                false -> Unauthorized
                            end;
                {error, _} -> Unauthorized
            end;
        _ -> Unauthorized
    end.

now_ms() ->
    {MegaSecs, Secs, MicroSecs} = now(),
    trunc(MegaSecs*1000000000 + Secs*1000 + MicroSecs/1000).

http_date() ->
    httpd_util:rfc1123_date(erlang:universaltime()).

vhost(ReqData) ->
    VHost = id(vhost, ReqData),
    case vhost_exists(VHost) of
        true  -> VHost;
        false -> not_found
    end.

vhost_exists(VHostBin) ->
    lists:any(fun (E) -> E == VHostBin end,
              rabbit_access_control:list_vhosts()).

bad_request(Reason, ReqData, Context) ->
    Json = {struct, [{error, bad_request},
                     {reason, rabbit_mgmt_format:tuple(Reason)}]},
    ReqData1 = wrq:append_to_response_body(mochijson2:encode(Json), ReqData),
    {{halt, 400}, ReqData1, Context}.

id(Key, ReqData) ->
    {ok, Id} = dict:find(Key, wrq:path_info(ReqData)),
    list_to_binary(mochiweb_util:unquote(Id)).

decode(Keys, ReqData) ->
    Body = wrq:req_body(ReqData),
    {Res, Json} = try
                      {struct, J} = mochijson2:decode(Body),
                      {ok, J}
                  catch error:_ -> {error, not_json}
                  end,
    case Res of
        ok ->
            Results =
                [proplists:get_value(list_to_binary(K), Json) || K <- Keys],
            case lists:any(fun(E) -> E == undefined end, Results) of
                false -> Results;
                true  -> {error, key_missing}
            end;
        _  ->
            {Res, Json}
    end.
