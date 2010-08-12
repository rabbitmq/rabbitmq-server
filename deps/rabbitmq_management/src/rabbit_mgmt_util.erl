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
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_util).

-export([is_authorized/2, now_ms/0, http_date/0]).

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
