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
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_util).

%% TODO sort all this out; maybe there's scope for rabbit_mgmt_request?

-export([is_authorized/2, is_authorized_admin/2, is_authorized_admin/4,
         vhost/1, vhost_from_headers/1]).
-export([is_authorized_vhost/2, is_authorized_user/3,
         is_authorized_monitor/2, is_authorized_policies/2,
         is_authorized_global_parameters/2]).
-export([bad_request/3, bad_request_exception/4, id/2, parse_bool/1,
         parse_int/1]).
-export([with_decode/4, not_found/3, amqp_request/4]).
-export([with_channel/4, with_channel/5]).
-export([props_to_method/2, props_to_method/4]).
-export([all_or_one_vhost/2, http_to_amqp/5, reply/3, responder_map/1,
         filter_vhost/3]).
-export([filter_conn_ch_list/3, filter_user/2, list_login_vhosts/2]).
-export([with_decode/5, decode/1, decode/2, set_resp_header/3,
         args/1]).
-export([reply_list/3, reply_list/5, reply_list/4,
         sort_list/2, destination_type/1, reply_list_or_paginate/3]).
-export([post_respond/1, columns/1, is_monitor/1]).
-export([list_visible_vhosts/1, b64decode_or_throw/1, no_range/0, range/1,
         range_ceil/1, floor/2, ceil/1, ceil/2]).
-export([pagination_params/1, maybe_filter_by_keyword/4,
         get_value_param/2]).

-import(rabbit_misc, [pget/2]).

-include("rabbit_mgmt.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(FRAMING, rabbit_framing_amqp_0_9_1).
-define(DEFAULT_PAGE_SIZE, 100).
-define(MAX_PAGE_SIZE, 500).
-record(pagination, {page = undefined, page_size = undefined,
                     name = undefined, use_regex = undefined}).

-define(MAX_RANGE, 500).

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
        V         -> lists:member(V, list_login_vhosts(User, cowboy_req:get(socket, ReqData)))
    end.

%% Used for connections / channels. A normal user can only see / delete
%% their own stuff. Monitors can see other users' and delete their
%% own. Admins can do it all.
is_authorized_user(ReqData, Context, Item) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access object">>,
                  fun(#user{username = Username, tags = Tags}) ->
                          case element(1, cowboy_req:method(ReqData)) of
                              <<"DELETE">> -> is_admin(Tags);
                              _            -> is_monitor(Tags)
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

%% For global parameters. Must be policymaker.
is_authorized_global_parameters(ReqData, Context) ->
    is_authorized(ReqData, Context,
                  <<"User not authorised to access object">>,
                  fun(#user{tags = Tags}) ->
                           is_policymaker(Tags)
                  end).

is_authorized(ReqData, Context, ErrorMsg, Fun) ->
    case cowboy_req:method(ReqData) of
        {<<"OPTIONS">>, _} -> {true, ReqData, Context};
        _ -> is_authorized1(ReqData, Context, ErrorMsg, Fun)
    end.

is_authorized1(ReqData, Context, ErrorMsg, Fun) ->
    case cowboy_req:parse_header(<<"authorization">>, ReqData) of
        {ok, {<<"basic">>, {Username, Password}}, _} ->
            is_authorized(ReqData, Context,
                Username, Password,
                ErrorMsg, Fun);
        _ ->
            {{false, ?AUTH_REALM}, ReqData, Context}
    end.

is_authorized(ReqData, Context, Username, Password, ErrorMsg, Fun) ->
    ErrFun = fun (Msg) ->
                     rabbit_log:warning("HTTP access denied: user '~s' - ~s~n",
                                        [Username, Msg]),
                     not_authorised(Msg, ReqData, Context)
             end,
    AuthProps = [{password, Password}] ++ case vhost(ReqData) of
        VHost when is_binary(VHost) -> [{vhost, VHost}];
        _                           -> []
    end,
    case rabbit_access_control:check_user_login(Username, AuthProps) of
        {ok, User = #user{tags = Tags}} ->
            {{IP, _}, _} = cowboy_req:peer(ReqData),
            case rabbit_access_control:check_user_loopback(Username, IP) of
                ok ->
                    case is_mgmt_user(Tags) of
                        true ->
                            case Fun(User) of
                                true  -> {true, ReqData,
                                          Context#context{user     = User,
                                                          password = Password}};
                                false -> ErrFun(ErrorMsg)
                            end;
                        false ->
                            ErrFun(<<"Not management user">>)
                    end;
                not_allowed ->
                    ErrFun(<<"User can only log in via localhost">>)
            end;
        {refused, _Username, Msg, Args} ->
            rabbit_log:warning("HTTP access denied: ~s~n",
                               [rabbit_misc:format(Msg, Args)]),
            not_authorised(<<"Login failed">>, ReqData, Context)
    end.

vhost_from_headers(ReqData) ->
    case cowboy_req:header(<<"x-vhost">>, ReqData) of
        {undefined, _} -> none;
        %% blank x-vhost means "All hosts" is selected in the UI
        {<<>>, _}        -> none;
        {VHost, _}     -> VHost
    end.

vhost(ReqData) ->
    case id(vhost, ReqData) of
      none  -> vhost_from_headers(ReqData);
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

%% Provides a map of content type-to-responder that are supported by
%% reply/3. The map can be used in the content_types_provided/2 callback
%% used by cowboy_rest. Responder functions must be
%% exported from the caller module and must use reply/3
%% under the hood.
responder_map(FunctionName) ->
    [
      {<<"application/json">>, FunctionName}
    , {<<"application/bert">>, FunctionName}
    ].

reply(Facts, ReqData, Context) ->
    reply0(extract_columns(Facts, ReqData), ReqData, Context).

reply0(Facts, ReqData, Context) ->
    ReqData1 = set_resp_header(<<"Cache-Control">>, "no-cache", ReqData),
    try
        case cowboy_req:meta(media_type, ReqData1) of
            {{<<"application">>, <<"bert">>, _}, _} ->
                {term_to_binary(Facts), ReqData1, Context};
            _ ->
                {mochijson2:encode(rabbit_mgmt_format:format_nulls(Facts)),
                 ReqData1, Context}
        end
    catch exit:{json_encode, E} ->
            Error = iolist_to_binary(
                      io_lib:format("JSON encode error: ~p", [E])),
            Reason = iolist_to_binary(
                       io_lib:format("While encoding: ~n~p", [Facts])),
            internal_server_error(Error, Reason, ReqData1, Context)
    end.

reply_list(Facts, ReqData, Context) ->
    reply_list(Facts, ["vhost", "name"], ReqData, Context, undefined).

reply_list(Facts, DefaultSorts, ReqData, Context) ->
    reply_list(Facts, DefaultSorts, ReqData, Context, undefined).

get_value_param(Name, ReqData) ->
    case cowboy_req:qs_val(Name, ReqData) of
        {undefined, _} -> undefined;
        {Bin, _} -> binary_to_list(Bin)
    end.

reply_list(Facts, DefaultSorts, ReqData, Context, Pagination) ->
    SortList =
    sort_list(
          extract_columns_list(Facts, ReqData),
          DefaultSorts,
          get_value_param(<<"sort">>, ReqData),
          get_sort_reverse(ReqData), Pagination),

    reply(SortList, ReqData, Context).

-spec get_sort_reverse(cowboy_req:req()) -> atom().
get_sort_reverse(ReqData) ->
    case get_value_param(<<"sort_reverse">>, ReqData) of
        undefined -> false;
        V -> list_to_atom(V)
    end.

reply_list_or_paginate(Facts, ReqData, Context) ->
    try
        Pagination = pagination_params(ReqData),
        reply_list(Facts, ["vhost", "name"], ReqData, Context, Pagination)
    catch error:badarg ->
        Reason = iolist_to_binary(
               io_lib:format("Pagination parameters are invalid", [])),
        invalid_pagination(bad_request, Reason, ReqData, Context);
      {error, ErrorType, S} ->
            Reason = iolist_to_binary(S),
            invalid_pagination(ErrorType, Reason, ReqData, Context)
    end.


sort_list(Facts, Sorts) -> sort_list(Facts, Sorts, undefined, false,
  undefined).

sort_list(Facts, _, [], _, _) ->
    %% Do not sort when we are explicitly requsted to sort with an
    %% empty sort columns list. Note that this clause won't match when
    %% 'sort' parameter is not provided in a HTTP request at all.
    Facts;
sort_list(Facts, DefaultSorts, Sort, Reverse, Pagination) ->
    SortList = case Sort of
           undefined -> DefaultSorts;
           Extra     -> [Extra | DefaultSorts]
           end,
    %% lists:sort/2 is much more expensive than lists:sort/1
    Sorted = [V || {_K, V} <- lists:sort(
                                [{sort_key(F, SortList), F} || F <- Facts])],

    ContextList = maybe_filter_context(Sorted, Pagination),
    range_filter(maybe_reverse(ContextList, Reverse), Pagination, Sorted).

%%
%% Filtering functions
%%


maybe_filter_context(List, #pagination{name = Name, use_regex = UseRegex}) when
      is_list(Name) ->
    lists:filter(fun(ListF) ->
             maybe_filter_by_keyword(name, Name, ListF, UseRegex)
         end,
         List);
%% Here it is backward with the other API(s), that don't filter the data
maybe_filter_context(List, _) ->
    List.


match_value({_, Value}, ValueTag, UseRegex) when UseRegex =:= "true" ->
    case re:run(Value, ValueTag, [caseless]) of
        {match, _} -> true;
        nomatch ->  false
    end;
match_value({_, Value}, ValueTag, _) ->
    Pos = string:str(string:to_lower(binary_to_list(Value)),
        string:to_lower(ValueTag)),
    case Pos of
        Pos  when Pos > 0 -> true;
        _ -> false
    end.

maybe_filter_by_keyword(KeyTag, ValueTag, List, UseRegex) when
      is_list(ValueTag), length(ValueTag) > 0 ->
    match_value(lists:keyfind(KeyTag, 1, List), ValueTag, UseRegex);
maybe_filter_by_keyword(_, _, _, _) ->
    true.

check_request_param(V, ReqData) ->
    case cowboy_req:qs_val(V, ReqData) of
    {undefined, _} -> undefined;
    {Str, _}       -> list_to_integer(binary_to_list(Str))
    end.

%% Validates and returns pagination parameters:
%% Page is assumed to be > 0, PageSize > 0 PageSize <= ?MAX_PAGE_SIZE
pagination_params(ReqData) ->
    PageNum  = check_request_param(<<"page">>, ReqData),
    PageSize = check_request_param(<<"page_size">>, ReqData),
    Name = get_value_param(<<"name">>, ReqData),
    UseRegex = get_value_param(<<"use_regex">>, ReqData),
    case {PageNum, PageSize} of
        {undefined, _} ->
            undefined;
    {PageNum, undefined} when is_integer(PageNum) andalso PageNum > 0 ->
            #pagination{page = PageNum, page_size = ?DEFAULT_PAGE_SIZE,
                name =  Name, use_regex = UseRegex};
        {PageNum, PageSize}  when is_integer(PageNum)
                                  andalso is_integer(PageSize)
                                  andalso (PageNum > 0)
                                  andalso (PageSize > 0)
                                  andalso (PageSize =< ?MAX_PAGE_SIZE) ->
            #pagination{page = PageNum, page_size = PageSize,
                name =  Name, use_regex = UseRegex};
        _ -> throw({error, invalid_pagination_parameters,
                    io_lib:format("Invalid pagination parameters: page number ~p, page size ~p",
                                  [PageNum, PageSize])})
    end.

-spec maybe_reverse([any()], string() | true | false) -> [any()].
maybe_reverse([], _) ->
    [];
maybe_reverse(RangeList, true) when is_list(RangeList) ->
    lists:reverse(RangeList);
maybe_reverse(RangeList, false) ->
    RangeList.

%% for backwards compatibility, does not filter the list
range_filter(List, undefined, _)
      -> List;

range_filter(List, RP = #pagination{page = PageNum, page_size = PageSize},
         TotalElements) ->
    Offset = (PageNum - 1) * PageSize + 1,
    try
        range_response(lists:sublist(List, Offset, PageSize), RP, List,
               TotalElements)
    catch
        error:function_clause ->
            Reason = io_lib:format(
               "Page out of range, page: ~p page size: ~p, len: ~p",
               [PageNum, PageSize, length(List)]),
            throw({error, page_out_of_range, Reason})
    end.

%% Injects pagination information into
range_response([], #pagination{page = PageNum, page_size = PageSize},
    TotalFiltered, TotalElements) ->
    TotalPages = trunc((length(TotalFiltered) + PageSize - 1) / PageSize),
    [{total_count, length(TotalElements)},
     {item_count, 0},
     {filtered_count, length(TotalFiltered)},
     {page, PageNum},
     {page_size, PageSize},
     {page_count, TotalPages},
     {items, []}
    ];
range_response(List, #pagination{page = PageNum, page_size = PageSize},
    TotalFiltered, TotalElements) ->
    TotalPages = trunc((length(TotalFiltered) + PageSize - 1) / PageSize),
    [{total_count, length(TotalElements)},
     {item_count, length(List)},
     {filtered_count, length(TotalFiltered)},
     {page, PageNum},
     {page_size, PageSize},
     {page_count, TotalPages},
     {items, List}
    ].

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

pget_bin(Key, {struct, List}, Default) ->
    pget_bin(Key, List, Default);
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
    case cowboy_req:qs_val(<<"columns">>, ReqData) of
        {undefined, _} -> all;
        {Bin, _}       -> [[list_to_binary(T) || T <- string:tokens(C, ".")]
                      || C <- string:tokens(binary_to_list(Bin), ",")]
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

% want_column(_Col, all) -> true;
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

invalid_pagination(Type,Reason, ReqData, Context) ->
    halt_response(400, Type, Reason, ReqData, Context).

halt_response(Code, Type, Reason, ReqData, Context) ->
    Json = {struct, [{error, Type},
                     {reason, rabbit_mgmt_format:tuple(Reason)}]},
    {ok, ReqData1} = cowboy_req:reply(Code,
        [{<<"content-type">>, <<"application/json">>}],
        mochijson2:encode(Json), ReqData),
    {halt, ReqData1, Context}.

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
    case cowboy_req:binding(Key, ReqData) of
        {undefined, _} -> none;
        {Id, _}        -> Id
    end.

with_decode(Keys, ReqData, Context, Fun) ->
    {ok, Body, ReqData1} = cowboy_req:body(ReqData),
    with_decode(Keys, Body, ReqData1, Context, Fun).

with_decode(Keys, Body, ReqData, Context, Fun) ->
    case decode(Keys, Body) of
        {error, Reason}    -> bad_request(Reason, ReqData, Context);
        {ok, Values, JSON} -> try
                                  Fun(Values, JSON, ReqData)
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
            {ok, Body, ReqData1} = cowboy_req:body(ReqData),
            case decode(Body) of
                {ok, Props} ->
                    try
                        Node = case pget(<<"node">>, Props) of
                                   undefined -> node();
                                   N         -> rabbit_nodes:make(
                                                  binary_to_list(N))
                               end,
                        amqp_request(VHost, ReqData1, Context, Node,
                                     props_to_method(
                                       MethodName, Props, Transformers, Extra))
                    catch {error, Error} ->
                            bad_request(Error, ReqData1, Context)
                    end;
                {error, Reason} ->
                    bad_request(rabbit_mgmt_format:escape_html_tags(Reason),
                                ReqData1, Context)
            end
    end.

props_to_method(MethodName, Props, Transformers, Extra) ->
    Props1 = [{list_to_atom(binary_to_list(K)), V} || {K, V} <- Props],
    props_to_method(
      MethodName, rabbit_mgmt_format:format(Props1 ++ Extra, {Transformers, true})).

props_to_method(MethodName, Props) ->
    Props1 = rabbit_mgmt_format:format(
               Props,
               {fun rabbit_mgmt_format:format_args/1, true}),
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
        {error, not_allowed} ->
            not_authorised(<<"Access refused.">>, ReqData, Context);
        {error, access_refused} ->
            not_authorised(<<"Access refused.">>, ReqData, Context);
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

filter_vhost(List, ReqData, Context) ->
    VHosts = list_login_vhosts(Context#context.user, cowboy_req:get(socket, ReqData)),
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

set_resp_header(K, V, ReqData) ->
    cowboy_req:set_resp_header(K, strip_crlf(V), ReqData).

strip_crlf(Str) -> lists:append(string:tokens(Str, "\r\n")).

args({struct, L}) -> args(L);
args(L)           -> rabbit_mgmt_format:to_amqp_table(L).

%% Make replying to a post look like anything else...
post_respond({true, ReqData, Context}) ->
    {true, ReqData, Context};
post_respond({halt, ReqData, Context}) ->
    {halt, ReqData, Context};
post_respond({JSON, ReqData, Context}) ->
    {true, set_resp_header(
             <<"Content-Type">>, "application/json",
             cowboy_req:set_resp_body(JSON, ReqData)), Context}.

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
        false -> list_login_vhosts(User, undefined)
    end.

list_login_vhosts(User, Sock) ->
    [V || V <- rabbit_vhost:list(),
          case catch rabbit_access_control:check_vhost_access(User, V, Sock) of
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

no_range() -> {no_range, no_range, no_range, no_range}.

%% Take floor on queries so we make sure we only return samples
%% for which we've finished receiving events. Fixes the "drop at
%% the end" problem.
range(ReqData) -> {range("lengths",    fun floor/2, ReqData),
                   range("msg_rates",  fun floor/2, ReqData),
                   range("data_rates", fun floor/2, ReqData),
                   range("node_stats", fun floor/2, ReqData)}.

%% ...but if we know only one event could have contributed towards
%% what we are interested in, then let's take the ceiling instead and
%% get slightly fresher data that will match up with any
%% non-historical data we have (e.g. queue length vs queue messages in
%% RAM, they should both come from the same snapshot or we might
%% report more messages in RAM than total).
%%
%% However, we only do this for queue lengths since a) it's the only
%% thing where this ends up being really glaring and b) for other
%% numbers we care more about the rate than the absolute value, and if
%% we use ceil() we stand a 50:50 chance of looking up the last sample
%% in the range before we get it, and thus deriving an instantaneous
%% rate of 0.0.
%%
%% Age is assumed to be > 0, Incr > 0 and (Age div Incr) <= ?MAX_RANGE.
%% The latter condition allows us to limit the number of samples that
%% will be sent to the client.
range_ceil(ReqData) -> {range("lengths",    fun ceil/2,  ReqData),
                        range("msg_rates",  fun floor/2, ReqData),
                        range("data_rates", fun floor/2,  ReqData),
                        range("node_stats", fun floor/2,  ReqData)}.

range(Prefix, Round, ReqData) ->
    Age0 = int(Prefix ++ "_age", ReqData),
    Incr0 = int(Prefix ++ "_incr", ReqData),
    if
        is_atom(Age0) orelse is_atom(Incr0) -> no_range;
        (Age0 > 0) andalso (Incr0 > 0) andalso ((Age0 div Incr0) =< ?MAX_RANGE) ->
            Age = Age0 * 1000,
            Incr = Incr0 * 1000,
            Now = time_compat:os_system_time(milli_seconds),
            Last = Round(Now, Incr),
            #range{first = (Last - Age),
                   last  = Last,
                   incr  = Incr};
        true -> throw({error, invalid_range_parameters,
                    io_lib:format("Invalid range parameters: age ~p, incr ~p",
                                  [Age0, Incr0])})
    end.

floor(TS, Interval) -> (TS div Interval) * Interval.

ceil(TS, Interval) -> case floor(TS, Interval) of
                          TS    -> TS;
                          Floor -> Floor + Interval
                      end.

ceil(X) when X < 0 ->
    trunc(X);
ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

int(Name, ReqData) ->
    case cowboy_req:qs_val(list_to_binary(Name), ReqData) of
        {undefined, _} -> undefined;
        {Bin, _}       -> case catch list_to_integer(binary_to_list(Bin)) of
                         {'EXIT', _} -> undefined;
                         Integer     -> Integer
                     end
    end.

