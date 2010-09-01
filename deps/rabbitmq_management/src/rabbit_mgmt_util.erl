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

%% TODO sort all this out; maybe there's scope for rabbit_mgmt_request?

-export([is_authorized/2, now_ms/0, vhost/1, vhost_exists/1]).
-export([bad_request/3, id/2, parse_bool/1]).
-export([with_decode/4, not_found/3, not_authorised/3, amqp_request/4]).
-export([all_or_one_vhost/2, with_decode_vhost/4, reply/3]).

-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

is_authorized(ReqData, Context) ->
    Unauthorized = {"Basic realm=\"RabbitMQ Management Console\"",
                    ReqData, Context},
    case wrq:get_req_header("authorization", ReqData) of
        "Basic " ++ Base64 ->
            Str = base64:mime_decode_to_string(Base64),
            [User, Pass] = [list_to_binary(S) || S <- string:tokens(Str, ":")],
            case rabbit_access_control:lookup_user(User) of
                {ok, U}  ->
                    case Pass == U#user.password of
                        true ->
                            {true, ReqData,
                             Context#context{username = User, password = Pass}};
                        false ->
                            Unauthorized
                    end;
                {error, _} ->
                    Unauthorized
            end;
        _ -> Unauthorized
    end.

now_ms() ->
    {MegaSecs, Secs, MicroSecs} = now(),
    trunc(MegaSecs*1000000000 + Secs*1000 + MicroSecs/1000).

http_date() ->
    httpd_util:rfc1123_date(erlang:universaltime()).

vhost(ReqData) ->
    case id(vhost, ReqData) of
        none ->
            none;
        VHost ->
            case vhost_exists(VHost) of
                true  -> VHost;
                false -> not_found
            end
    end.

vhost_exists(VHostBin) ->
    lists:any(fun (E) -> E == VHostBin end,
              rabbit_access_control:list_vhosts()).

reply(Facts, ReqData, Context) ->
    {mochijson2:encode(Facts), ReqData, Context}.

bad_request(Reason, ReqData, Context) ->
    halt_response(400, bad_request, Reason, ReqData, Context).

not_authorised(Reason, ReqData, Context) ->
    halt_response(401, not_authorised, Reason, ReqData, Context).

not_found(Reason, ReqData, Context) ->
    halt_response(404, not_found, Reason, ReqData, Context).

halt_response(Code, Type, Reason, ReqData, Context) ->
    Json = {struct, [{error, Type},
                     {reason, rabbit_mgmt_format:tuple(Reason)}]},
    ReqData1 = wrq:append_to_response_body(mochijson2:encode(Json), ReqData),
    {{halt, Code}, ReqData1, Context}.

id(exchange, ReqData) ->
    case id0(exchange, ReqData) of
        <<"amq.default">> -> <<"">>;
        Name              -> Name
    end;
id(Key, ReqData) ->
    id0(Key, ReqData).

id0(Key, ReqData) ->
    case dict:find(Key, wrq:path_info(ReqData)) of
        {ok, Id} ->
            list_to_binary(mochiweb_util:unquote(Id));
        error ->
            none
    end.

with_decode(Keys, ReqData, Context, Fun) ->
    case decode(Keys, ReqData) of
        {error, Reason} ->
            bad_request(Reason, ReqData, Context);
        Values ->
            try
                Fun(Values)
            catch throw:{error, Error} ->
                    bad_request(Error, ReqData, Context);
                  fb3efbwfbhf2bj throw:access_refused ->
                    not_authorised(not_authorised, ReqData, Context);
                  throw:{server_closed, Reason} ->
                    bad_request(list_to_binary(Reason), ReqData, Context)
            end
    end.

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
                [get_or_missing(list_to_binary(atom_to_list(K)), Json)
                 || K <- Keys],
            case lists:filter(fun({key_missing, _}) -> true;
                                 (_)                -> false
                              end, Results) of
                []      -> Results;
                Errors  -> {error, Errors}
            end;
        _  ->
            {Res, Json}
    end.

with_decode_vhost(Keys, ReqData, Context, Fun) ->
    case vhost(ReqData) of
        not_found ->
            not_found(vhost_not_found, ReqData, Context);
        VHost ->
            with_decode(Keys, ReqData, Context,
                        fun (Vals) -> Fun(VHost, Vals) end)
    end.

get_or_missing(K, L) ->
    case proplists:get_value(K, L) of
        undefined -> {key_missing, K};
        V         -> V
    end.

parse_bool(V) ->
    case V of
        <<"true">>  -> true;
        <<"false">> -> false;
        true        -> true;
        false       -> false;
        _           -> throw({error, {not_boolean, V}})
    end.

amqp_request(VHost, ReqData, Context, Method) ->
    try
        Params = #amqp_params{username = Context#context.username,
                              password = Context#context.password,
                              virtual_host = VHost},
        Conn = amqp_connection:start_direct(Params),
        Ch = amqp_connection:open_channel(Conn),
        amqp_channel:call(Ch, Method),
        amqp_channel:close(Ch),
        amqp_connection:close(Conn),
        {true, ReqData, Context}
    %% See bug 23187
    catch error:{badmatch,{error, #amqp_error{name = access_refused}}} ->
            not_authorised(not_authorised, ReqData, Context);
          error:{badmatch,{error, #amqp_error{name = {error, Error}}}} ->
            bad_request(Error, ReqData, Context);
          exit:{{server_initiated_close, ?NOT_FOUND, Reason}, _} ->
            not_found(list_to_binary(Reason), ReqData, Context);
          exit:{{server_initiated_close, _Code, Reason}, _} ->
            bad_request(list_to_binary(Reason), ReqData, Context)
    end.

all_or_one_vhost(ReqData, Fun) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        none ->
            lists:append([Fun(V) || V <- rabbit_access_control:list_vhosts()]);
        not_found ->
            vhost_not_found;
        VHost ->
            Fun(VHost)
    end.
