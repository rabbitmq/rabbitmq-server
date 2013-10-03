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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_util).

%% TODO sort all this out; maybe there's scope for rabbit_mgmt_request?

-export([is_authorized/2, is_authorized_admin/2, is_authorized_admin/4,
         vhost/1]).
-export([is_authorized_vhost/2, is_authorized_user/3,
         is_authorized_monitor/2, is_authorized_policies/2]).
-export([bad_request/3, bad_request_exception/4, id/2, parse_bool/1,
         parse_int/1]).
-export([with_decode/4, not_found/3, amqp_request/4]).
-export([with_channel/4, with_channel/5]).
-export([props_to_method/2, props_to_method/4]).
-export([all_or_one_vhost/2, http_to_amqp/5, reply/3, filter_vhost/3]).
-export([filter_conn_ch_list/3, filter_user/2]).
-export([with_decode/5, decode/1, decode/2, redirect/2, args/1]).
-export([reply_list/3, reply_list/4, sort_list/2, destination_type/1]).
-export([post_respond/1, columns/1, is_monitor/1]).
-export([list_visible_vhosts/1, b64decode_or_throw/1, no_range/0, range/1,
         range_ceil/1, floor/2, ceil/2]).

-import(rabbit_misc, [pget/2, pget/3]).

-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(FRAMING, rabbit_framing_amqp_0_9_1).

%%--------------------------------------------------------------------

is_authorized(ReqData, Context) ->
    is_authorized(ReqData, Context, '', fun(_) -> true end).

is_authorized_admin(ReqData, Context) ->
    is_authorized(ReqData, Context,
                  <<"Not administrator user">>,
                  fun(#user{tags = Tags}) -> is_admin(Tags) end).

is_authorized_admin(ReqData, Context, Username, Password) ->
    is_authorized(ReqData, Context, Username, Password,
                  <<"Not administrator user">>,
                  fun(#user{tags = Tags}) -> is_admin(Tags) end).

is_authorized_monitor(ReqData, Context) ->
    is_authorized(ReqData, Context,
                  <<"Not monitor user">>,
                  fun(#user{tags = Tags}) -> is_monitor(Tags) end).

is_authorized_vhost(ReqData, Context) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access virtual host">>,
                  fun(User) ->
                          user_matches_vhost(ReqData, User)
                  end).

user_matches_vhost(ReqData, User) ->
    case vhost(ReqData) of
        not_found -> true;
        none      -> true;
        V         -> lists:member(V, list_login_vhosts(User))
    end.

%% Used for connections / channels. A normal user can only see / delete
%% their own stuff. Monitors can see other users' and delete their
%% own. Admins can do it all.
is_authorized_user(ReqData, Context, Item) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access object">>,
                  fun(#user{username = Username, tags = Tags}) ->
                          case wrq:method(ReqData) of
                              'DELETE' -> is_admin(Tags);
                              _        -> is_monitor(Tags)
                          end orelse Username == pget(user, Item)
                  end).

%% For policies / parameters. Like is_authorized_vhost but you have to
%% be a policymaker.
is_authorized_policies(ReqData, Context) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access object">>,
                  fun(User = #user{tags = Tags}) ->
                          is_policymaker(Tags) andalso
                              user_matches_vhost(ReqData, User)
                  end).

is_authorized(ReqData, Context, ErrorMsg, Fun) ->
    case rabbit_web_dispatch_util:parse_auth_header(
           wrq:get_req_header("authorization", ReqData)) of
        [Username, Password] ->
            is_authorized(ReqData, Context, Username, Password, ErrorMsg, Fun);
        _ ->
            {?AUTH_REALM, ReqData, Context}
    end.

is_authorized(ReqData, Context, Username, Password, ErrorMsg, Fun) ->
    ErrFun = fun (Msg) -> not_authorised(Msg, ReqData, Context) end,
    case rabbit_access_control:check_user_pass_login(Username, Password) of
        {ok, User = #user{tags = Tags}} ->
            case is_mgmt_user(Tags) of
                true  -> case Fun(User) of
                             true  -> {true, ReqData,
                                       Context#context{user     = User,
                                                       password = Password}};
                             false -> ErrFun(ErrorMsg)
                         end;
                false -> ErrFun(<<"Not management user">>)
            end;
        _ ->
            ErrFun(<<"Login failed">>)
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
    reply0(extract_columns(Facts, ReqData), ReqData, Context).

reply0(Facts, ReqData, Context) ->
    ReqData1 = wrq:set_resp_header("Cache-Control", "no-cache", ReqData),
    try
        {mochijson2:encode(Facts), ReqData1, Context}
    catch exit:{json_encode, E} ->
            Error = iolist_to_binary(
                      io_lib:format("JSON encode error: ~p", [E])),
            Reason = iolist_to_binary(
                       io_lib:format("While encoding:~n~p", [Facts])),
            internal_server_error(Error, Reason, ReqData1, Context)
    end.

reply_list(Facts, ReqData, Context) ->
    reply_list(Facts, ["vhost", "name"], ReqData, Context).

reply_list(Facts, DefaultSorts, ReqData, Context) ->
    reply(sort_list(
            extract_columns_list(Facts, ReqData),
            DefaultSorts,
            wrq:get_qs_value("sort", ReqData),
            wrq:get_qs_value("sort_reverse", ReqData)),
          ReqData, Context).

sort_list(Facts, Sorts) -> sort_list(Facts, Sorts, undefined, false).

sort_list(Facts, DefaultSorts, Sort, Reverse) ->
    SortList = case Sort of
               undefined -> DefaultSorts;
               Extra     -> [Extra | DefaultSorts]
           end,
    %% lists:sort/2 is much more expensive than lists:sort/1
    Sorted = [V || {_K, V} <- lists:sort(
                                [{sort_key(F, SortList), F} || F <- Facts])],
    case Reverse of
        "true" -> lists:reverse(Sorted);
        _      -> Sorted
    end.

sort_key(_Item, []) ->
    [];
sort_key(Item, [Sort | Sorts]) ->
    [get_dotted_value(Sort, Item) | sort_key(Item, Sorts)].

get_dotted_value(Key, Item) ->
    Keys = string:tokens(Key, "."),
    get_dotted_value0(Keys, Item).

get_dotted_value0([Key], Item) ->
    %% Put "nothing" before everything else, in number terms it usually
    %% means 0.
    pget_bin(list_to_binary(Key), Item, 0);
get_dotted_value0([Key | Keys], Item) ->
    get_dotted_value0(Keys, pget_bin(list_to_binary(Key), Item, [])).

pget_bin(Key, List, Default) ->
    case lists:partition(fun ({K, _V}) -> a2b(K) =:= Key end, List) of
        {[{_K, V}], _} -> V;
        {[],        _} -> Default
    end.

extract_columns(Item, ReqData) ->
    extract_column_items(Item, columns(ReqData)).

extract_columns_list(Items, ReqData) ->
    Cols = columns(ReqData),
    [extract_column_items(Item, Cols) || Item <- Items].

columns(ReqData) ->
    case wrq:get_qs_value("columns", ReqData) of
        undefined -> all;
        Str       -> [[list_to_binary(T) || T <- string:tokens(C, ".")]
                      || C <- string:tokens(Str, ",")]
    end.

extract_column_items(Item, all) ->
    Item;
extract_column_items({struct, L}, Cols) ->
    extract_column_items(L, Cols);
extract_column_items(Item = [T | _], Cols) when is_tuple(T) ->
    [{K, extract_column_items(V, descend_columns(a2b(K), Cols))} ||
        {K, V} <- Item, want_column(a2b(K), Cols)];
extract_column_items(L, Cols) when is_list(L) ->
    [extract_column_items(I, Cols) || I <- L];
extract_column_items(O, _Cols) ->
    O.

want_column(_Col, all) -> true;
want_column(Col, Cols) -> lists:any(fun([C|_]) -> C == Col end, Cols).

descend_columns(_K, [])                   -> [];
descend_columns( K, [[K]        | _Rest]) -> all;
descend_columns( K, [[K   | K2] |  Rest]) -> [K2 | descend_columns(K, Rest)];
descend_columns( K, [[_K2 | _ ] |  Rest]) -> descend_columns(K, Rest).

a2b(A) when is_atom(A) -> list_to_binary(atom_to_list(A));
a2b(B)                 -> B.

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
    case orddict:find(Key, wrq:path_info(ReqData)) of
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

decode(<<"">>) ->
    {ok, []};

decode(Body) ->
    try
        {struct, J} = mochijson2:decode(Body),
        {ok, J}
    catch error:_ -> {error, not_json}
    end.

get_or_missing(K, L) ->
    case pget(K, L) of
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
                        Node = case pget(<<"node">>, Props) of
                                   undefined -> node();
                                   N         -> rabbit_nodes:make(
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
                            NewR = case pget(K, Props1) of
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
parse_bool(undefined)   -> undefined;
parse_bool(V)           -> throw({error, {not_boolean, V}}).

parse_int(I) when is_integer(I) -> I;
parse_int(F) when is_number(F)  -> trunc(F);
parse_int(S)                    -> try
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
             Context = #context{user     = #user {username = Username},
                                password = Password},
             Node, Fun) ->
    Params = #amqp_params_direct{username     = Username,
                                 password     = Password,
                                 node         = Node,
                                 virtual_host = VHost},
    case amqp_connection:start(Params) of
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
        {error, {auth_failure, Msg}} ->
            not_authorised(Msg, ReqData, Context);
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
    VHosts = list_login_vhosts(Context#context.user),
    [I || I <- List, lists:member(pget(vhost, I), VHosts)].

filter_user(List, _ReqData, #context{user = User}) ->
    filter_user(List, User).

filter_user(List, #user{username = Username, tags = Tags}) ->
    case is_monitor(Tags) of
        true  -> List;
        false -> [I || I <- List, pget(user, I) == Username]
    end.

filter_conn_ch_list(List, ReqData, Context) ->
    rabbit_mgmt_format:strip_pids(
      filter_user(
        case vhost(ReqData) of
            none  -> List;
            VHost -> [I || I <- List, pget(vhost, I) =:= VHost]
        end, ReqData, Context)).

redirect(Location, ReqData) ->
    wrq:do_redirect(true,
                    wrq:set_resp_header("Location",
                                        binary_to_list(Location), ReqData)).

args({struct, L}) -> args(L);
args(L)           -> rabbit_mgmt_format:to_amqp_table(L).

%% Make replying to a post look like anything else...
post_respond({true, ReqData, Context}) ->
    {true, ReqData, Context};
post_respond({{halt, Code}, ReqData, Context}) ->
    {{halt, Code}, ReqData, Context};
post_respond({JSON, ReqData, Context}) ->
    {true, wrq:set_resp_header(
             "content-type", "application/json",
             wrq:append_to_response_body(JSON, ReqData)), Context}.

is_admin(T)       -> intersects(T, [administrator]).
is_policymaker(T) -> intersects(T, [administrator, policymaker]).
is_monitor(T)     -> intersects(T, [administrator, monitoring]).
is_mgmt_user(T)   -> intersects(T, [administrator, monitoring, policymaker,
                                    management]).

intersects(A, B) -> lists:any(fun(I) -> lists:member(I, B) end, A).

%% The distinction between list_visible_vhosts and list_login_vhosts
%% is there to ensure that admins / monitors can always learn of the
%% existence of all vhosts, and can always see their contribution to
%% global stats. However, if an admin / monitor does not have any
%% permissions for a vhost, it's probably less confusing to make that
%% prevent them from seeing "into" it, than letting them see stuff
%% that they then can't touch.

list_visible_vhosts(User = #user{tags = Tags}) ->
    case is_monitor(Tags) of
        true  -> rabbit_vhost:list();
        false -> list_login_vhosts(User)
    end.

list_login_vhosts(User) ->
    [V || V <- rabbit_vhost:list(),
          case catch rabbit_access_control:check_vhost_access(User, V) of
              ok -> true;
              _  -> false
          end].

%% Wow, base64:decode throws lots of weird errors. Catch and convert to one
%% that will cause a bad_request.
b64decode_or_throw(B64) ->
    try
        base64:decode(B64)
    catch error:_ ->
            throw({error, {not_base64, B64}})
    end.

no_range() -> {no_range, no_range, no_range}.

%% Take floor on queries so we make sure we only return samples
%% for which we've finished receiving events. Fixes the "drop at
%% the end" problem.
range(ReqData) -> {range("lengths",    fun floor/2, ReqData),
                   range("msg_rates",  fun floor/2, ReqData),
                   range("data_rates", fun floor/2, ReqData)}.

%% ...but if we know only one event could have contributed towards
%% what we are interested in, then let's take the ceiling instead and
%% get slightly fresher data.
%%
%% Why does msg_rates still use floor/2? Because in the cases where we
%% call this function (for connections and queues) the msg_rates are still
%% aggregated even though the lengths and data rates aren't.
range_ceil(ReqData) -> {range("lengths",    fun ceil/2,  ReqData),
                        range("msg_rates",  fun floor/2, ReqData),
                        range("data_rates", fun ceil/2,  ReqData)}.

range(Prefix, Round, ReqData) ->
    Age0 = int(Prefix ++ "_age", ReqData),
    Incr0 = int(Prefix ++ "_incr", ReqData),
    if
        is_integer(Age0) andalso is_integer(Incr0) ->
            Age = Age0 * 1000,
            Incr = Incr0 * 1000,
            Now = rabbit_mgmt_format:timestamp_ms(erlang:now()),
            Last = Round(Now, Incr),
            #range{first = (Last - Age),
                   last  = Last,
                   incr  = Incr};
        true ->
            no_range
    end.

floor(TS, Interval) -> (TS div Interval) * Interval.

ceil(TS, Interval) -> case floor(TS, Interval) of
                          TS    -> TS;
                          Floor -> Floor + Interval
                      end.

int(Name, ReqData) ->
    case wrq:get_qs_value(Name, ReqData) of
        undefined -> undefined;
        Str       -> case catch list_to_integer(Str) of
                         {'EXIT', _} -> undefined;
                         Integer     -> Integer
                     end
    end.
