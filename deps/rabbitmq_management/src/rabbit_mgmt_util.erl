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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
-module(rabbit_mgmt_util).

%% TODO sort all this out; maybe there's scope for rabbit_mgmt_request?

-export([is_authorized/2, is_authorized_admin/2, vhost/1]).
-export([is_authorized_vhost/2, is_authorized/3, is_authorized_user/3]).
-export([bad_request/3, bad_request_exception/4, id/2, parse_bool/1,
         parse_int/1]).
-export([with_decode/4, not_found/3, amqp_request/4]).
-export([with_channel/4, with_channel/5]).
-export([props_to_method/2]).
-export([all_or_one_vhost/2, http_to_amqp/5, reply/3, filter_vhost/3]).
-export([filter_user/3, with_decode/5, decode/1, redirect/2, args/1]).
-export([reply_list/3, reply_list/4, sort_list/4, destination_type/1]).
-export([post_respond/1]).

-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(FRAMING, rabbit_framing_amqp_0_9_1).

%%--------------------------------------------------------------------

is_authorized(ReqData, Context) ->
    is_authorized(ReqData, Context, fun(_) -> true end).

is_authorized_admin(ReqData, Context) ->
    is_authorized(ReqData, Context,
                  fun(#user{is_admin = IsAdmin}) -> IsAdmin end).

is_authorized_vhost(ReqData, Context) ->
    is_authorized(
      ReqData, Context,
      fun(User) ->
              case vhost(ReqData) of
                  not_found -> true;
                  none      -> true;
                  V         -> lists:member(
                                 V,
                                 rabbit_access_control:list_vhosts(User, write))
              end
      end).

is_authorized_user(ReqData, Context, Item) ->
    is_authorized(
      ReqData, Context,
      fun(#user{username = Username, is_admin = IsAdmin}) ->
              IsAdmin orelse Username == proplists:get_value(user, Item)
      end).

is_authorized(ReqData, Context, Fun) ->
    %% The realm name is wrong, but it needs to match the context name
    %% of /mgmt/ to prevent some web ui users from being asked for
    %% creds twice.
    %% This will get fixed if / when we stop using rabbitmq-mochiweb.
    Unauthorized = {"Basic realm=\"Management: Web UI\"",
                    ReqData, Context},
    case rabbit_mochiweb_util:parse_auth_header(
           wrq:get_req_header("authorization", ReqData)) of
        [Username, Password] ->
            case rabbit_access_control:check_user_pass_login(Username,
                                                             Password) of
                {ok, User} ->
                    case Fun(User) of
                        true  -> {true, ReqData,
                                  Context#context{user     = User,
                                                  password = Password}};
                        false -> Unauthorized
                    end;
                {refused, _, _} ->
                    Unauthorized
            end;
        _ ->
            Unauthorized
    end.

vhost(ReqData) ->
    case id(vhost, ReqData) of
        none  -> none;
        VHost -> case rabbit_vhost:exists(VHost) of
                     true  -> VHost;
                     false -> not_found
                 end
    end.

destination_type(ReqData) ->
    case id(dtype, ReqData) of
        <<"e">> -> exchange;
        <<"q">> -> queue
    end.

reply(Facts, ReqData, Context) ->
    ReqData1 = wrq:set_resp_header("Cache-Control", "no-cache", ReqData),
    try
        {mochijson2:encode(Facts), ReqData1, Context}
    catch
        exit:{json_encode, E} ->
            Error = iolist_to_binary(
                      io_lib:format("JSON encode error: ~p", [E])),
            Reason = iolist_to_binary(
                       io_lib:format("While encoding:~n~p", [Facts])),
            internal_server_error(Error, Reason, ReqData1, Context)
    end.

reply_list(Facts, ReqData, Context) ->
    reply_list(Facts, ["vhost", "name"], ReqData, Context).

reply_list(Facts, DefaultSorts, ReqData, Context) ->
    reply(sort_list(Facts, DefaultSorts,
                    wrq:get_qs_value("sort", ReqData),
                    wrq:get_qs_value("sort_reverse", ReqData)),
          ReqData, Context).

sort_list(Facts, DefaultSorts, Sort, Reverse) ->
    SortList = case Sort of
               undefined -> DefaultSorts;
               Extra     -> [Extra | DefaultSorts]
           end,
    Sorted = lists:sort(fun(A, B) -> compare(A, B, SortList) end, Facts),
    case Reverse of
        "true" -> lists:reverse(Sorted);
        _      -> Sorted
    end.

compare(_A, _B, []) ->
    true;
compare(A, B, [Sort | Sorts]) ->
    A0 = get_dotted_value(Sort, A),
    B0 = get_dotted_value(Sort, B),
    case {A0, B0, A0 == B0} of
        %% Put "nothing" before everything else, in number terms it usually
        %% means 0.
        {_,         _,         true}  -> compare(A, B, Sorts);
        {undefined, _,         _}     -> true;
        {_,         undefined, _}     -> false;
        {_,         _,         false} -> A0 < B0
    end.

get_dotted_value(Key, Item) ->
    Keys = string:tokens(Key, "."),
    get_dotted_value0(Keys, Item).

get_dotted_value0([Key], Item) ->
    proplists:get_value(list_to_atom(Key), Item);
get_dotted_value0([Key | Keys], Item) ->
    SubItem = case proplists:get_value(list_to_atom(Key), Item) of
                  undefined -> [];
                  Other     -> Other
              end,
    get_dotted_value0(Keys, SubItem).

bad_request(Reason, ReqData, Context) ->
    halt_response(400, bad_request, Reason, ReqData, Context).

not_authorised(Reason, ReqData, Context) ->
    halt_response(401, not_authorised, Reason, ReqData, Context).

not_found(Reason, ReqData, Context) ->
    halt_response(404, not_found, Reason, ReqData, Context).

internal_server_error(Error, Reason, ReqData, Context) ->
    rabbit_log:error("~s~n~s~n", [Error, Reason]),
    halt_response(500, Error, Reason, ReqData, Context).

halt_response(Code, Type, Reason, ReqData, Context) ->
    Json = {struct, [{error, Type},
                     {reason, rabbit_mgmt_format:tuple(Reason)}]},
    ReqData1 = wrq:append_to_response_body(mochijson2:encode(Json), ReqData),
    {{halt, Code}, ReqData1, Context}.

id(Key, ReqData) when Key =:= exchange;
                      Key =:= source;
                      Key =:= destination ->
    case id0(Key, ReqData) of
        <<"amq.default">> -> <<"">>;
        Name              -> Name
    end;
id(Key, ReqData) ->
    id0(Key, ReqData).

id0(Key, ReqData) ->
    case dict:find(Key, wrq:path_info(ReqData)) of
        {ok, Id} -> list_to_binary(mochiweb_util:unquote(Id));
        error    -> none
    end.

with_decode(Keys, ReqData, Context, Fun) ->
    with_decode(Keys, wrq:req_body(ReqData), ReqData, Context, Fun).

with_decode(Keys, Body, ReqData, Context, Fun) ->
    case decode(Keys, Body) of
        {error, Reason}    -> bad_request(Reason, ReqData, Context);
        {ok, Values, JSON} -> try
                                  Fun(Values, JSON)
                              catch {error, Error} ->
                                      bad_request(Error, ReqData, Context)
                              end
    end.

decode(Keys, Body) ->
    case decode(Body) of
        {ok, J0} -> J = [{list_to_atom(binary_to_list(K)), V} || {K, V} <- J0],
                    Results = [get_or_missing(K, J) || K <- Keys],
                    case [E || E = {key_missing, _} <- Results] of
                        []      -> {ok, Results, J};
                        Errors  -> {error, Errors}
                    end;
        Else     -> Else
    end.

decode(Body) ->
    try
        {struct, J} = mochijson2:decode(Body),
        {ok, J}
    catch error:_ -> {error, not_json}
    end.

get_or_missing(K, L) ->
    case proplists:get_value(K, L) of
        undefined -> {key_missing, K};
        V         -> V
    end.

http_to_amqp(MethodName, ReqData, Context, Transformers, Extra) ->
    case vhost(ReqData) of
        not_found ->
            not_found(vhost_not_found, ReqData, Context);
        VHost ->
            case decode(wrq:req_body(ReqData)) of
                {ok, Props} ->
                    try
                        Node =
                            case proplists:get_value(<<"node">>, Props) of
                                undefined -> node();
                                N         -> rabbit_misc:makenode(
                                               binary_to_list(N))
                            end,
                        amqp_request(VHost, ReqData, Context, Node,
                                     props_to_method(
                                       MethodName, Props, Transformers, Extra))
                    catch {error, Error} ->
                            bad_request(Error, ReqData, Context)
                    end;
                {error, Reason} ->
                    bad_request(Reason, ReqData, Context)
            end
    end.

props_to_method(MethodName, Props, Transformers, Extra) ->
    Props1 = [{list_to_atom(binary_to_list(K)), V} || {K, V} <- Props],
    props_to_method(
      MethodName, rabbit_mgmt_format:format(Props1 ++ Extra, Transformers)).

props_to_method(MethodName, Props) ->
    Props1 = rabbit_mgmt_format:format(
               Props,
               [{fun (Args) -> [{arguments, args(Args)}] end, [arguments]}]),
    FieldNames = ?FRAMING:method_fieldnames(MethodName),
    {Res, _Idx} = lists:foldl(
                    fun (K, {R, Idx}) ->
                            NewR = case proplists:get_value(K, Props1) of
                                       undefined -> R;
                                       V         -> setelement(Idx, R, V)
                                   end,
                            {NewR, Idx + 1}
                    end, {?FRAMING:method_record(MethodName), 2},
                    FieldNames),
    Res.

parse_bool(<<"true">>)  -> true;
parse_bool(<<"false">>) -> false;
parse_bool(true)        -> true;
parse_bool(false)       -> false;
parse_bool(V)           -> throw({error, {not_boolean, V}}).

parse_int(I) when is_integer(I) ->
    I;
parse_int(F) when is_number(F) ->
    trunc(F);
parse_int(S) ->
    try
        list_to_integer(binary_to_list(S))
    catch error:badarg ->
        throw({error, {not_integer, S}})
    end.

amqp_request(VHost, ReqData, Context, Method) ->
    amqp_request(VHost, ReqData, Context, node(), Method).

amqp_request(VHost, ReqData, Context, Node, Method) ->
    with_channel(VHost, ReqData, Context, Node,
                      fun (Ch) ->
                              amqp_channel:call(Ch, Method),
                              {true, ReqData, Context}
                      end).

with_channel(VHost, ReqData, Context, Fun) ->
    with_channel(VHost, ReqData, Context, node(), Fun).

with_channel(VHost, ReqData,
             Context = #context{ user = #user { username = Username },
                                 password = Password }, Node, Fun) ->
    Params = #amqp_params{username     = Username,
                          password     = Password,
                          node         = Node,
                          virtual_host = VHost},
    case amqp_connection:start(direct, Params) of
        {ok, Conn} ->
            {ok, Ch} = amqp_connection:open_channel(Conn),
            try
                Fun(Ch)
            catch
                exit:{{shutdown,
                       {server_initiated_close, ?NOT_FOUND, Reason}}, _} ->
                    not_found(Reason, ReqData, Context);
                exit:{{shutdown,
                      {server_initiated_close, ?ACCESS_REFUSED, Reason}}, _} ->
                    not_authorised(Reason, ReqData, Context);
                exit:{{shutdown, {ServerClose, Code, Reason}}, _}
                  when ServerClose =:= server_initiated_close;
                       ServerClose =:= server_initiated_hard_close ->
                    bad_request_exception(Code, Reason, ReqData, Context);
                exit:{{shutdown, {connection_closing,
                                  {ServerClose, Code, Reason}}}, _}
                  when ServerClose =:= server_initiated_close;
                       ServerClose =:= server_initiated_hard_close ->
                    bad_request_exception(Code, Reason, ReqData, Context)
            after
            catch amqp_channel:close(Ch),
            catch amqp_connection:close(Conn)
            end;
        {error, auth_failure} ->
            not_authorised(<<"">>, ReqData, Context);
        {error, {nodedown, N}} ->
            bad_request(
              list_to_binary(
                io_lib:format("Node ~s could not be contacted", [N])),
              ReqData, Context)
    end.

bad_request_exception(Code, Reason, ReqData, Context) ->
    bad_request(list_to_binary(io_lib:format("~p ~s", [Code, Reason])),
                ReqData, Context).

all_or_one_vhost(ReqData, Fun) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        none      -> lists:append([Fun(V) || V <- rabbit_vhost:list()]);
        not_found -> vhost_not_found;
        VHost     -> Fun(VHost)
    end.

filter_vhost(List, _ReqData, Context) ->
    VHosts = rabbit_access_control:list_vhosts(Context#context.user, write),
    [I || I <- List, lists:member(proplists:get_value(vhost, I), VHosts)].

filter_user(List, _ReqData, #context{user = #user{is_admin = true}}) ->
    List;
filter_user(List, _ReqData,
            #context{user = #user{username = Username, is_admin = false}}) ->
    [I || I <- List, proplists:get_value(user, I) == Username].

redirect(Location, ReqData) ->
    wrq:do_redirect(true,
                    wrq:set_resp_header("Location",
                                        binary_to_list(Location), ReqData)).
args({struct, L}) ->
    args(L);
args(L) ->
    rabbit_mgmt_format:to_amqp_table(L).

%% Make replying to a post look like anything else...
post_respond({{halt, Code}, ReqData, Context}) ->
    {{halt, Code}, ReqData, Context};
post_respond({JSON, ReqData, Context}) ->
    {true, wrq:set_resp_header(
             "content-type", "application/json",
             wrq:append_to_response_body(JSON, ReqData)), Context}.
