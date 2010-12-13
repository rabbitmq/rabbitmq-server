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
-export([bad_request/3, id/2, parse_bool/1]).
-export([with_decode/4, with_decode_opts/4, not_found/3, amqp_request/4]).
-export([all_or_one_vhost/2, with_decode_vhost/4, reply/3, filter_vhost/3]).
-export([filter_user/3, with_decode/5, redirect/2, args/1, vhosts/1]).
-export([reply_list/3, reply_list/4, sort_list/4, destination_type/1]).

-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

is_authorized(ReqData, Context) ->
    is_authorized(ReqData, Context, fun(_) -> true end).

is_authorized_admin(ReqData, Context) ->
    is_authorized(ReqData, Context,
                  fun(#user{is_admin = IsAdmin}) -> IsAdmin end).

is_authorized_vhost(ReqData, Context) ->
    is_authorized(ReqData, Context,
                  fun(#user{username = Username}) ->
                          case vhost(ReqData) of
                              not_found -> true;
                              none      -> true;
                              V         -> lists:member(V, vhosts(Username))
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
        [User, Pass] ->
            case rabbit_access_control:check_user_pass_login(User, Pass) of
                {ok, U = #user{is_admin = IsAdmin}} ->
                    case Fun(U) of
                        true  -> {true, ReqData,
                                  Context#context{username = User,
                                                  password = Pass,
                                                  is_admin = IsAdmin}};
                        false -> Unauthorized
                    end;
                {refused, _} ->
                    Unauthorized
            end;
        _ ->
            Unauthorized
    end.

vhost(ReqData) ->
    case id(vhost, ReqData) of
        none  -> none;
        VHost -> case rabbit_access_control:vhost_exists(VHost) of
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
    {mochijson2:encode(Facts), ReqData1, Context}.

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

%% TODO unify this with the function below after bug23384 is merged
with_decode_opts(Keys, ReqData, Context, Fun) ->
    Body = wrq:req_body(ReqData),
    case decode(Keys, Body) of
        {error, Reason} -> bad_request(Reason, ReqData, Context);
        _Values         -> try
                               {ok, Obj0} = decode(Body),
                               Obj = [{list_to_atom(binary_to_list(K)), V} ||
                                         {K, V} <- Obj0],
                               Fun(Obj)
                           catch {error, Error} ->
                                   bad_request(Error, ReqData, Context)
                           end
    end.

with_decode(Keys, ReqData, Context, Fun) ->
    with_decode(Keys, wrq:req_body(ReqData), ReqData, Context, Fun).

with_decode(Keys, Body, ReqData, Context, Fun) ->
    case decode(Keys, Body) of
        {error, Reason} -> bad_request(Reason, ReqData, Context);
        Values          -> try
                               Fun(Values)
                           catch {error, Error} ->
                                   bad_request(Error, ReqData, Context)
                           end
    end.

decode(Keys, Body) ->
    case decode(Body) of
        {ok, J} -> Results =
                       [get_or_missing(list_to_binary(atom_to_list(K)), J) ||
                           K <- Keys],
                   case [E || E = {key_missing, _} <- Results] of
                       []      -> Results;
                       Errors  -> {error, Errors}
                   end;
        Else    -> Else
    end.

decode(Body) ->
    try
        {struct, J} = mochijson2:decode(Body),
        {ok, J}
    catch error:_ -> {error, not_json}
    end.

with_decode_vhost(Keys, ReqData, Context, Fun) ->
    case vhost(ReqData) of
        not_found -> not_found(vhost_not_found, ReqData, Context);
        VHost     -> with_decode(Keys, ReqData, Context,
                                 fun (Vals) -> Fun(VHost, Vals) end)
    end.

get_or_missing(K, L) ->
    case proplists:get_value(K, L) of
        undefined -> {key_missing, K};
        V         -> V
    end.

parse_bool(<<"true">>)  -> true;
parse_bool(<<"false">>) -> false;
parse_bool(true)        -> true;
parse_bool(false)       -> false;
parse_bool(V)           -> throw({error, {not_boolean, V}}).

amqp_request(VHost, ReqData, Context, Method) ->
    try
        Params = #amqp_params{username = Context#context.username,
                              password = Context#context.password,
                              virtual_host = VHost},
        {ok, Conn} = amqp_connection:start(direct, Params),
        %% No need to check for {error, {auth_failure_likely...
        %% since we will weed out failed logins in some webmachine
        %% is_authorized/2 anyway.
        {ok, Ch} = amqp_connection:open_channel(Conn),
        amqp_channel:call(Ch, Method),
        amqp_channel:close(Ch),
        amqp_connection:close(Conn),
        {true, ReqData, Context}
    catch
        exit:{{server_initiated_close, ?NOT_FOUND, Reason}, _} ->
            not_found(list_to_binary(Reason), ReqData, Context);
        exit:{{server_initiated_close, ?ACCESS_REFUSED, Reason}, _} ->
            not_authorised(list_to_binary(Reason), ReqData, Context);
        exit:{{ServerClose, Code, Reason}, _}
          when ServerClose =:= server_initiated_close;
               ServerClose =:= server_initiated_hard_close ->
            bad_request(list_to_binary(io_lib:format("~p ~s", [Code, Reason])),
                        ReqData, Context)
    end.

all_or_one_vhost(ReqData, Fun) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        none      -> lists:append(
                       [Fun(V) || V <- rabbit_access_control:list_vhosts()]);
        not_found -> vhost_not_found;
        VHost     -> Fun(VHost)
    end.

filter_vhost(List, _ReqData, Context) ->
    VHosts = vhosts(Context#context.username),
    [I || I <- List, lists:member(proplists:get_value(vhost, I), VHosts)].

vhosts(Username) ->
    [VHost || {VHost, _ConfigurePerm, _WritePerm, _ReadPerm}
                  <- rabbit_access_control:list_user_permissions(Username)].

filter_user(List, _ReqData, #context{is_admin = true}) ->
    List;
filter_user(List, _ReqData, #context{username = Username, is_admin = false}) ->
    [I || I <- List, proplists:get_value(user, I) == Username].

redirect(Location, ReqData) ->
    wrq:do_redirect(true,
                    wrq:set_resp_header("Location",
                                        binary_to_list(Location), ReqData)).
args({struct, L}) ->
    args(L);
args(L) ->
    [{K, rabbit_mgmt_format:args_type(V), V} || {K, V} <- L].
