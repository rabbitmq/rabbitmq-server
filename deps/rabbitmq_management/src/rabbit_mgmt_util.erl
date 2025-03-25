%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_util).

%% TODO sort all this out; maybe there's scope for rabbit_mgmt_request?

-export([is_authorized/2, is_authorized_admin/2, is_authorized_admin/4,
         is_authorized_admin/3, vhost/1, vhost_from_headers/1]).
-export([is_authorized_vhost/2, is_authorized_user/3,
         is_authorized_user/4, is_authorized_user/5,
         is_authorized_monitor/2, is_authorized_policies/2,
         is_authorized_vhost_visible/2,
         is_authorized_vhost_visible_for_monitoring/2,
         is_authorized_global_parameters/2]).
-export([user/1]).
-export([bad_request/3, service_unavailable/3, bad_request_exception/4,
         internal_server_error/3, internal_server_error/4, precondition_failed/3,
         id/2, parse_bool/1, parse_int/1, redirect_to_home/3]).
-export([with_decode/4, not_found/3]).
-export([with_channel/4, with_channel/5]).
-export([props_to_method/2, props_to_method/4]).
-export([all_or_one_vhost/2, reply/3, responder_map/1,
         filter_vhost/3]).
-export([filter_conn_ch_list/3, filter_user/2, list_login_vhosts/2,
         list_login_vhosts_names/2]).
-export([filter_tracked_conn_list/3]).
-export([with_decode/5, decode/1, decode/2, set_resp_header/3,
         args/1, read_complete_body/1, read_complete_body_with_limit/2]).
-export([reply_list/3, reply_list/5, reply_list/4,
         sort_list/2, destination_type/1, reply_list_or_paginate/3
         ]).
-export([post_respond/1, columns/1, is_monitor/1]).
-export([list_visible_vhosts/1, list_visible_vhosts_names/1,
         b64decode_or_throw/1, no_range/0, range/1,
         range_ceil/1, floor/2, ceil/1, ceil/2]).
-export([pagination_params/1,
         maybe_filter_by_keyword/4,
         get_value_param/2,
         augment_resources/6
        ]).
-export([direct_request/6]).
-export([qs_val/2]).
-export([get_path_prefix/0]).
-export([catch_no_such_user_or_vhost/2]).
-export([method_not_allowed/3]).

-export([disable_stats/1, enable_queue_totals/1]).

-export([set_resp_not_found/2]).

-import(rabbit_misc, [pget/2]).

-include("rabbit_mgmt.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(FRAMING, rabbit_framing_amqp_0_9_1).
-define(DEFAULT_PAGE_SIZE, 100).
-define(MAX_PAGE_SIZE, 500).

-define(MAX_RANGE, 500).

-record(pagination, {page = undefined, page_size = undefined,
                     name = undefined, use_regex = undefined}).

-record(aug_ctx, {req_data :: cowboy_req:req(),
                  pagination :: #pagination{},
                  sort = [] :: [atom()],
                  columns = [] :: [atom()],
                  data :: term()}).

%%--------------------------------------------------------------------

is_authorized(ReqData, Context) ->
    rabbit_web_dispatch_access_control:is_authorized(ReqData,
                                           Context,
                                           auth_config()).

is_authorized_admin(ReqData, Context) ->
    rabbit_web_dispatch_access_control:is_authorized_admin(ReqData,
                                                 Context,
                                                 auth_config()).

is_authorized_admin(ReqData, Context, Token) ->
    AuthConfig = auth_config(),
    rabbit_web_dispatch_access_control:is_authorized(
      ReqData, Context,
      AuthConfig#auth_settings.oauth_client_id,
      Token, <<"Not administrator user">>,
      fun(#user{tags = Tags}) ->
              rabbit_web_dispatch_access_control:is_admin(Tags)
      end, AuthConfig).

is_authorized_admin(ReqData, Context, Username, Password) ->
    rabbit_web_dispatch_access_control:is_authorized_admin(ReqData,
                                                 Context,
                                                 Username,
                                                 Password,
                                                 auth_config()).

is_authorized_monitor(ReqData, Context) ->
    rabbit_web_dispatch_access_control:is_authorized_monitor(ReqData,
                                                   Context,
                                                   auth_config()).

is_authorized_vhost(ReqData, Context) ->
    rabbit_web_dispatch_access_control:is_authorized_vhost(ReqData,
                                                 Context,
                                                 auth_config()).

is_authorized_vhost_visible(ReqData, Context) ->
    rabbit_web_dispatch_access_control:is_authorized_vhost_visible(ReqData,
                                                         Context,
                                                         auth_config()).

is_authorized_vhost_visible_for_monitoring(ReqData, Context) ->
    rabbit_web_dispatch_access_control:is_authorized_vhost_visible_for_monitoring(ReqData,
                                                                        Context,
                                                                        auth_config()).

auth_config() ->
    BasicAuthEnabled = not get_bool_env(rabbitmq_management, disable_basic_auth, false),
    OauthEnabled = get_bool_env(rabbitmq_management, oauth_enabled, false),
    OauthClientId = rabbit_data_coercion:to_binary(
                        application:get_env(rabbitmq_management, oauth_client_id, "")),
    #auth_settings{auth_realm = ?AUTH_REALM,
                   basic_auth_enabled = BasicAuthEnabled,
                   oauth2_enabled = OauthEnabled,
                   oauth_client_id = OauthClientId}.

disable_stats(ReqData) ->
    MgmtOnly = case qs_val(<<"disable_stats">>, ReqData) of
                   <<"true">> -> true;
                   _ -> false
               end,
    MgmtOnly orelse
    not rabbit_mgmt_agent_config:is_metrics_collector_enabled() orelse
    not rabbit_mgmt_features:are_stats_enabled().

enable_queue_totals(ReqData) ->
    EnableTotals = case qs_val(<<"enable_queue_totals">>, ReqData) of
                       <<"true">> -> true;
                       _ -> false
                   end,
    EnableTotals orelse get_bool_env(rabbitmq_management, enable_queue_totals, false).

get_bool_env(Application, Par, Default) ->
    case application:get_env(Application, Par, Default) of
        true -> true;
        false -> false;
        Other ->
            rabbit_log:warning("Invalid configuration for application ~tp: ~tp set to ~tp",
                               [Application, Par, Other]),
            Default
    end.

get_authz_data(ReqData) ->
    {PeerAddress, _PeerPort} = cowboy_req:peer(ReqData),
    {ip, PeerAddress}.

%% Used for connections / channels. A normal user can only see / delete
%% their own stuff. Monitors can see other users' and delete their
%% own. Admins can do it all.
is_authorized_user(ReqData, Context, Item) ->
    rabbit_web_dispatch_access_control:is_authorized_user(ReqData,
                                                Context,
                                                Item,
                                                auth_config()).

%% For policies / parameters. Like is_authorized_vhost but you have to
%% be a policymaker.
is_authorized_policies(ReqData, Context) ->
    rabbit_web_dispatch_access_control:is_authorized_policies(ReqData,
                                                    Context,
                                                    auth_config()).

%% For global parameters. Must be policymaker.
is_authorized_global_parameters(ReqData, Context) ->
    rabbit_web_dispatch_access_control:is_authorized_global_parameters(ReqData,
                                                             Context,
                                                             auth_config()).

%% is_basic_auth_disabled() ->
%%     get_bool_env(rabbitmq_management, disable_basic_auth, false).

%% is_oauth2_enabled() ->
%%     get_bool_env(rabbitmq_management, oauth_enabled, false).

is_authorized_user(ReqData, Context, Username, Password) ->

    rabbit_web_dispatch_access_control:is_authorized_user(ReqData,
                                                Context,
                                                Username,
                                                Password,
                                                auth_config()).

is_authorized_user(ReqData, Context, Username, Password, ReplyWhenFailed) ->
    rabbit_web_dispatch_access_control:is_authorized_user(ReqData,
                                                Context,
                                                Username,
                                                Password,
                                                ReplyWhenFailed,
                                                auth_config()).

vhost_from_headers(ReqData) ->
    rabbit_web_dispatch_access_control:vhost_from_headers(ReqData).

vhost(ReqData) ->
    rabbit_web_dispatch_access_control:vhost(ReqData).

user(ReqData) ->
  case id(user, ReqData) of
    none     -> not_found;
    Username ->
      case rabbit_auth_backend_internal:exists(Username) of
        true  -> Username;
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
      {<<"application/json">>, FunctionName},
      {<<"application/bert">>, FunctionName}
    ].

reply({stop, _, _} = Reply, _ReqData, _Context) ->
    Reply;
reply(Facts, ReqData, Context) ->
    reply0(extract_columns(Facts, ReqData), ReqData, Context).

reply0(Facts, ReqData, Context) ->
    ReqData1 = set_resp_header(<<"cache-control">>, "no-cache", ReqData),
    try
        case maps:get(media_type, ReqData1, undefined) of
            {<<"application">>, <<"bert">>, _} ->
                {term_to_binary(Facts), ReqData1, Context};
            _ ->
                {rabbit_json:encode(rabbit_mgmt_format:format_nulls(Facts)),
                 ReqData1, Context}
        end
    catch exit:{json_encode, E} ->
            Error = iolist_to_binary(
                      io_lib:format("JSON encode error: ~tp", [E])),
            Reason = iolist_to_binary(
                       io_lib:format("While encoding: ~n~tp", [Facts])),
            internal_server_error(Error, Reason, ReqData1, Context)
    end.

reply_list(Facts, ReqData, Context) ->
    reply_list(Facts, ["vhost", "name"], ReqData, Context, undefined).

reply_list(Facts, DefaultSorts, ReqData, Context) ->
    reply_list(Facts, DefaultSorts, ReqData, Context, undefined).

get_value_param(Name, ReqData) ->
    case qs_val(Name, ReqData) of
        undefined -> undefined;
        Bin       -> binary_to_list(Bin)
    end.

get_sorts_param(ReqData, Def) ->
    case get_value_param(<<"sort">>, ReqData) of
        undefined ->
            Def;
        [] ->
            Def;
        S ->
            [S]
    end.

reply_list(Facts, DefaultSorts, ReqData, Context, Pagination) ->
    SortList =
        sort_list_and_paginate(
              extract_columns_list(Facts, ReqData),
              DefaultSorts,
              get_sorts_param(ReqData, undefined),
              get_sort_reverse(ReqData), Pagination),

    reply(SortList, ReqData, Context).

-spec get_sort_reverse(cowboy_req:req()) -> atom().
get_sort_reverse(ReqData) ->
    case get_value_param(<<"sort_reverse">>, ReqData) of
        undefined -> false;
        V -> list_to_atom(V)
    end.


-spec is_pagination_requested(#pagination{} | undefined) -> boolean().
is_pagination_requested(undefined) ->
    false;
is_pagination_requested(#pagination{}) ->
    true.


with_valid_pagination(ReqData, Context, Fun) ->
    try
        Pagination = pagination_params(ReqData),
        Fun(Pagination)
    catch error:badarg ->
        Reason = iolist_to_binary(
               io_lib:format("Pagination parameters are invalid", [])),
        invalid_pagination(bad_request, Reason, ReqData, Context);
      {error, ErrorType, S} ->
            Reason = iolist_to_binary(S),
            invalid_pagination(ErrorType, Reason, ReqData, Context)
    end.

reply_list_or_paginate(Facts, ReqData, Context) ->
    with_valid_pagination(
      ReqData, Context,
      fun(Pagination) ->
              reply_list(Facts, ["vhost", "name"], ReqData, Context, Pagination)
      end).

merge_sorts(DefaultSorts, Extra) ->
    case Extra of
        undefined -> DefaultSorts;
        Extra ->
            %% it is possible that the extra sorts have an overlap with default
            lists:uniq(Extra ++ DefaultSorts)
    end.

%% Resource augmentation. Works out the most optimal configuration of the operations:
%% sort, page, augment and executes it returning the result.

column_strategy(all, _) -> extended;
column_strategy(Cols, BasicColumns) ->
    case Cols -- BasicColumns of
        [] -> basic;
        _ -> extended
    end.

% columns are [[binary()]] - this takes the first item
columns_as_strings(all) -> all;
columns_as_strings(Columns0) ->
    [rabbit_data_coercion:to_list(C) || [C | _] <- Columns0].

augment_resources(Resources, DefaultSort, BasicColumns, ReqData, Context,
                  AugmentFun) ->
    with_valid_pagination(ReqData, Context,
                          fun (Pagination) ->
                                  augment_resources0(Resources, DefaultSort,
                                                     BasicColumns, Pagination,
                                                     ReqData, AugmentFun)
                          end).

augment_resources0(Resources, DefaultSort, BasicColumns, Pagination, ReqData,
                   AugmentFun) ->
    SortFun = fun (AugCtx) -> sort(DefaultSort, AugCtx) end,
    AugFun = fun (AugCtx) -> augment(AugmentFun, AugCtx) end,
    PageFun = fun page/1,
    %% Sort needs to be a list of, erm, strings which are lists
    Sort = get_sorts_param(ReqData, DefaultSort),
    Columns = columns(ReqData),
    ColumnsAsStrings = columns_as_strings(Columns),
    Pipeline =
        case {Pagination =/= undefined,
              column_strategy(Sort, BasicColumns),
              column_strategy(ColumnsAsStrings, BasicColumns)} of
            {false, basic, basic} -> % no pagination, no extended fields
                [SortFun];
            {false, _, _} ->
                % no pagination, extended columns means we need to augment all - SLOW
                [AugFun, SortFun];
            {true, basic, basic} ->
                [SortFun, PageFun];
            {true, extended, _} ->
                Path = cowboy_req:path(ReqData),
                rabbit_log:debug("HTTP API: ~s slow query mode requested - extended sort on ~0p",
                                 [Path, Sort]),
                % pagination with extended sort columns - SLOW!
                [AugFun, SortFun, PageFun];
            {true, basic, extended} ->
                % pagination with extended columns and sorting on basic
                % here we can reduce the augmentation set before
                % augmenting
                [SortFun, PageFun, AugFun]
        end,
    #aug_ctx{data = {_, Reply}} = run_augmentation(
                                    #aug_ctx{req_data = ReqData,
                                             pagination = Pagination,
                                             sort = Sort,
                                             columns = Columns,
                                             data = {loaded, Resources}},
                                    Pipeline),
    rabbit_mgmt_format:strip_pids(Reply).

run_augmentation(C, []) -> C;
run_augmentation(C, [Next | Rem]) ->
    C2 = Next(C),
    run_augmentation(C2, Rem).

sort(DefaultSort, Ctx = #aug_ctx{data = Data0,
                                 req_data = ReqData,
                                 sort = Sort}) ->
    Data1 = get_data(Data0),
    Data = sort_list(Data1, DefaultSort, Sort,
                     get_sort_reverse(ReqData)),
    Ctx#aug_ctx{data = update_data(Data0, {sorted, Data})}.

page(Ctx = #aug_ctx{data = Data0,
                    pagination = Pagination}) ->
    Data = filter_and_paginate(get_data(Data0), Pagination),
    Ctx#aug_ctx{data = update_data(Data0, {paged, Data})}.

update_data({paged, Old}, New) ->
    {paged, rabbit_misc:pset(items, get_data(New), Old)};
update_data(_, New) ->
    New.

augment(AugmentFun, Ctx = #aug_ctx{data = Data0, req_data = ReqData}) ->
    Data1 = get_data(Data0),
    Data = AugmentFun(Data1, ReqData),
    Ctx#aug_ctx{data = update_data(Data0, {augmented, Data})}.

get_data({paged, Data}) ->
    rabbit_misc:pget(items, Data);
get_data({_, Data}) ->
    Data.

get_path_prefix() ->
    EnvPrefix = rabbit_misc:get_env(rabbitmq_management, path_prefix, ""),
    fixup_prefix(EnvPrefix).

fixup_prefix("") ->
    "";
fixup_prefix([Char|_Rest]=EnvPrefix) when is_list(EnvPrefix), Char =:= $/ ->
    EnvPrefix;
fixup_prefix(EnvPrefix) when is_list(EnvPrefix) ->
    "/" ++ EnvPrefix;
fixup_prefix(EnvPrefix) when is_binary(EnvPrefix) ->
    fixup_prefix(rabbit_data_coercion:to_list(EnvPrefix)).

%% XXX sort_list_and_paginate/2 is a more proper name for this function, keeping it
%% with this name for backwards compatibility
-spec sort_list([Fact], [string()]) -> [Fact] when
      Fact :: [{atom(), term()}].
sort_list(Facts, Sorts) -> sort_list_and_paginate(Facts, Sorts, undefined, false,
  undefined).

-spec sort_list([Fact], [SortColumn], [SortColumn] | undefined, boolean()) -> [Fact] when
      Fact :: [{atom(), term()}],
      SortColumn :: string().
sort_list(Facts, _, [], _) ->
    %% Do not sort when we are explicitly requested to sort with an
    %% empty sort columns list. Note that this clause won't match when
    %% 'sort' parameter is not provided in a HTTP request at all.
    Facts;
sort_list(Facts, DefaultSorts, Sort, Reverse) ->
    SortList = merge_sorts(DefaultSorts, Sort),
    %% lists:sort/2 is much more expensive than lists:sort/1
    Sorted = [V || {_K, V} <- lists:sort(
                                [{sort_key(F, SortList), F} || F <- Facts])],
    maybe_reverse(Sorted, Reverse).

sort_list_and_paginate(Facts, DefaultSorts, Sort, Reverse, Pagination) ->
    filter_and_paginate(sort_list(Facts, DefaultSorts, Sort, Reverse), Pagination).

filter_and_paginate(Sorted, Pagination) ->
    ContextList = maybe_filter_context(Sorted, Pagination),
    range_filter(ContextList, Pagination, Sorted).

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
    case qs_val(V, ReqData) of
	undefined -> undefined;
	Str       -> list_to_integer(binary_to_list(Str))
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
                    io_lib:format("Invalid pagination parameters: page number ~tp, page size ~tp",
                                  [PageNum, PageSize])})
    end.

-spec maybe_reverse([any()], string() | boolean()) -> [any()].
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
        range_response(sublist(List, Offset, PageSize), RP, List,
		       TotalElements)
    catch
        error:function_clause ->
            Reason = io_lib:format(
               "Page out of range, page: ~tp page size: ~tp, len: ~tp",
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

pget_bin(Key, Map, Default) when is_map(Map) ->
    maps:get(Key, Map, Default);
pget_bin(Key, List, Default) when is_list(List) ->
    case lists:partition(fun ({K, _V}) -> a2b(K) =:= Key end, List) of
        {[{_K, V}], _} -> V;
        {[],        _} -> Default
    end.
maybe_pagination(Item, false, ReqData) ->
    extract_column_items(Item, columns(ReqData));
maybe_pagination([{items, Item} | T], true, ReqData) ->
    [{items, extract_column_items(Item, columns(ReqData))} |
     maybe_pagination(T, true, ReqData)];
maybe_pagination([H | T], true, ReqData) ->
    [H | maybe_pagination(T,true, ReqData)];
maybe_pagination(Item, true, ReqData) ->
    [maybe_pagination(X, true, ReqData) || X <- Item].

extract_columns(Item, ReqData) ->
    maybe_pagination(Item, is_pagination_requested(pagination_params(ReqData)),
		     ReqData).

extract_columns_list(Items, ReqData) ->
    Cols = columns(ReqData),
    [extract_column_items(Item, Cols) || Item <- Items].

columns(ReqData) ->
    case qs_val(<<"columns">>, ReqData) of
        undefined -> all;
        Bin       -> [[list_to_binary(T) || T <- string:tokens(C, ".")]
                      || C <- string:tokens(binary_to_list(Bin), ",")]
    end.

extract_column_items(Item, all) ->
    Item;
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

service_unavailable(Reason, ReqData, Context) ->
    halt_response(503, service_unavailable, Reason, ReqData, Context).


not_authorised(Reason, ReqData, Context) ->
    rabbit_web_dispatch_access_control:not_authorised(Reason, ReqData, Context).

not_found(Reason, ReqData, Context) ->
    halt_response(404, not_found, Reason, ReqData, Context).

method_not_allowed(Reason, ReqData, Context) ->
    halt_response(405, method_not_allowed, Reason, ReqData, Context).

precondition_failed(Reason, ReqData, Context) ->
    halt_response(412, precondition_failed, Reason, ReqData, Context).

internal_server_error(Reason, ReqData, Context) ->
    internal_server_error(internal_server_error, Reason, ReqData, Context).

internal_server_error(Error, Reason, ReqData, Context) ->
    rabbit_log:error("~ts~n~ts", [Error, Reason]),
    halt_response(500, Error, Reason, ReqData, Context).

invalid_pagination(Type,Reason, ReqData, Context) ->
    halt_response(400, Type, Reason, ReqData, Context).

redirect_to_home(ReqData, Reason, Context) ->
    Home = cowboy_req:uri(ReqData, #{path => rabbit_mgmt_util:get_path_prefix() ++ "/", qs => Reason}),
    rabbit_log:info("redirect_to_home ~ts ~ts", [Reason, iolist_to_binary(Home)]),
    ReqData1 = cowboy_req:reply(302,
        #{<<"Location">> => iolist_to_binary(Home) },
        <<>>, ReqData),
    {stop, ReqData1, Context}.

halt_response(Code, Type, Reason, ReqData, Context) ->
    rabbit_web_dispatch_access_control:halt_response(Code,
                                           Type,
                                           Reason,
                                           ReqData,
                                           Context).

id(Key, ReqData) ->
    rabbit_web_dispatch_access_control:id(Key, ReqData).

%% IMPORTANT:
%% Prefer read_complete_body_with_limit/2 with an explicit limit to make it easier
%% to reason about what limit will be used.
read_complete_body(Req) ->
    read_complete_body(Req, <<"">>).
read_complete_body(Req, Acc) ->
    BodySizeLimit = application:get_env(rabbitmq_management, max_http_body_size, ?MANAGEMENT_DEFAULT_HTTP_MAX_BODY_SIZE),
    read_complete_body(Req, Acc, BodySizeLimit).
read_complete_body(Req0, Acc, BodySizeLimit) ->
    N = byte_size(Acc),
    case N > BodySizeLimit of
        true ->
            {error, http_body_limit_exceeded, BodySizeLimit, N};
        false ->
            case cowboy_req:read_body(Req0) of
                {ok, Data, Req}   -> {ok, <<Acc/binary, Data/binary>>, Req};
                {more, Data, Req} -> read_complete_body(Req, <<Acc/binary, Data/binary>>)
            end
    end.

read_complete_body_with_limit(Req, BodySizeLimit) when is_integer(BodySizeLimit) ->
    case cowboy_req:body_length(Req) of
        N when is_integer(N) ->
            case N > BodySizeLimit of
                true ->
                    {error, http_body_limit_exceeded, BodySizeLimit, N};
                false ->
                    do_read_complete_body_with_limit(Req, <<"">>, BodySizeLimit)
            end;
        undefined ->
            do_read_complete_body_with_limit(Req, <<"">>, BodySizeLimit)
    end.

do_read_complete_body_with_limit(Req0, Acc, BodySizeLimit) ->
    N = byte_size(Acc),
    case N > BodySizeLimit of
        true ->
            {error, http_body_limit_exceeded, BodySizeLimit, N};
        false ->
            case cowboy_req:read_body(Req0, #{length => BodySizeLimit, period => 30000}) of
                {ok, Data, Req}   -> {ok, <<Acc/binary, Data/binary>>, Req};
                {more, Data, Req} -> do_read_complete_body_with_limit(Req, <<Acc/binary, Data/binary>>, BodySizeLimit)
            end
    end.

with_decode(Keys, ReqData, Context, Fun) ->
    case read_complete_body(ReqData) of
        {error, http_body_limit_exceeded, LimitApplied, BytesRead} ->
            rabbit_log:warning("HTTP API: request exceeded maximum allowed payload size (limit: ~tp bytes, payload size: ~tp bytes)", [LimitApplied, BytesRead]),
            bad_request("Exceeded HTTP request body size limit", ReqData, Context);
        {ok, Body, ReqData1} ->
            with_decode(Keys, Body, ReqData1, Context, Fun)
    end.

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
        {ok, J0} ->
            J = maps:fold(fun(K, V, Acc) ->
                    Acc#{binary_to_atom(K, utf8) => V}
                end, J0, J0),
                Results = [get_or_missing(K, J) || K <- Keys],
                case [E || E = {key_missing, _} <- Results] of
                    []      -> {ok, Results, J};
                    Errors  -> {error, Errors}
                end;
        Else -> Else
    end.

-type parsed_json() :: map() | atom() | binary().
-spec decode(binary()) -> {ok, parsed_json()} | {error, term()}.

decode(<<"">>) ->
    {ok, #{}};
%% some HTTP API clients include "null" for payload in certain scenarios
decode(<<"null">>) ->
    {ok, #{}};
decode(<<"undefined">>) ->
    {ok, #{}};
decode(Body) ->
    try
        case rabbit_json:decode(Body) of
            Val when is_map(Val) ->
                {ok, Val};
            %% handle double encoded JSON, see rabbitmq/rabbitmq-management#839
            Bin when is_binary(Bin) ->
                {error, "invalid payload: the request body JSON-decoded to a string. "
                        "Is the input doubly-JSON-encoded?"};
            _                       ->
                {error, not_json}
        end
    catch error:_ -> {error, not_json}
    end.

get_or_missing(K, L) ->
    case maps:get(K, L, undefined) of
        undefined -> {key_missing, K};
        V         -> V
    end.

get_node(Props) ->
    case maps:get(<<"node">>, Props, undefined) of
        undefined -> node();
        N         -> rabbit_nodes:make(
                       binary_to_list(N))
    end.

direct_request(MethodName, Transformers, Extra, ErrorMsg, ReqData,
               Context = #context{user = User}) ->
    with_vhost_and_props(
      fun(VHost, Props, ReqData1) ->
              Method = props_to_method(MethodName, Props, Transformers, Extra),
              Node = get_node(Props),
              case rabbit_misc:rpc_call(Node, rabbit_channel, handle_method,
                                        [Method, none, #{}, none,
                                         VHost, User]) of
                  {badrpc, nodedown} ->
                      Msg = io_lib:format("Node ~tp could not be contacted", [Node]),
                      rabbit_log:warning(ErrorMsg, [Msg]),
                      bad_request(list_to_binary(Msg), ReqData1, Context);
                  {badrpc, {'EXIT', #amqp_error{name = not_found, explanation = Explanation}}} ->
                      rabbit_log:warning(ErrorMsg, [Explanation]),
                      not_found(Explanation, ReqData1, Context);
                  {badrpc, {'EXIT', #amqp_error{name = access_refused, explanation = Explanation}}} ->
                      rabbit_log:warning(ErrorMsg, [Explanation]),
                      not_authorised(<<"Access refused.">>, ReqData1, Context);
                  {badrpc, {'EXIT', #amqp_error{name = not_allowed, explanation = Explanation}}} ->
                      rabbit_log:warning(ErrorMsg, [Explanation]),
                      not_authorised(<<"Access refused.">>, ReqData1, Context);
                  {badrpc, {'EXIT', #amqp_error{explanation = Explanation}}} ->
                      rabbit_log:warning(ErrorMsg, [Explanation]),
                      bad_request(list_to_binary(Explanation), ReqData1, Context);
                  {badrpc, Reason} ->
                      Msg = io_lib:format("~tp", [Reason]),
                      rabbit_log:warning(ErrorMsg, [Msg]),
                      bad_request(
                        list_to_binary(
                          io_lib:format("Request to node ~ts failed with ~tp",
                                        [Node, Reason])),
                        ReqData1, Context);
                  _      -> {true, ReqData1, Context}
              end
      end, ReqData, Context).

with_vhost_and_props(Fun, ReqData, Context) ->
    case vhost(ReqData) of
        not_found ->
            not_found(rabbit_data_coercion:to_binary("vhost_not_found"),
                      ReqData, Context);
        VHost ->
            {ok, Body, ReqData1} = read_complete_body(ReqData),
            case decode(Body) of
                {ok, Props} ->
                    try
                        Fun(VHost, Props, ReqData1)
                    catch {error, Error} ->
                            bad_request(Error, ReqData1, Context)
                    end;
                {error, Reason} ->
                    bad_request(rabbit_mgmt_format:escape_html_tags(Reason),
                                ReqData1, Context)
            end
    end.

props_to_method(MethodName, Props, Transformers, Extra) when Props =:= null orelse
                                                             Props =:= undefined ->
    props_to_method(MethodName, #{}, Transformers, Extra);
props_to_method(MethodName, Props, Transformers, Extra) ->
    Props1 = [{list_to_atom(binary_to_list(K)), V} || {K, V} <- maps:to_list(Props)],
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

parse_bool(V) -> rabbit_misc:parse_bool(V).

parse_int(V) -> rabbit_misc:parse_int(V).

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
            _ = erlang:link(Conn),
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
            erlang:unlink(Conn),
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
                io_lib:format("Node ~ts could not be contacted", [N])),
              ReqData, Context)
    end.

bad_request_exception(Code, Reason, ReqData, Context) ->
    bad_request(list_to_binary(io_lib:format("~tp ~ts", [Code, Reason])),
                ReqData, Context).

all_or_one_vhost(ReqData, Fun) ->
    case vhost(ReqData) of
        none      -> lists:append([Fun(V) || V <- rabbit_vhost:list_names()]);
        not_found -> vhost_not_found;
        VHost     -> Fun(VHost)
    end.

filter_vhost(List, ReqData, Context) ->
    User = #user{tags = Tags} = Context#context.user,
    Fn   = case rabbit_web_dispatch_access_control:is_admin(Tags) of
               true  -> fun list_visible_vhosts_names/2;
               false -> fun list_login_vhosts_names/2
           end,
    AuthzData = get_authz_data(ReqData),
    VHosts = Fn(User, AuthzData),
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

filter_tracked_conn_list(List, ReqData, #context{user = #user{username = FilterUsername, tags = Tags}}) ->
    %% A bit of duplicated code, but we only go through the list once
    case {vhost(ReqData), is_monitor(Tags)} of
        {none, true} ->
            [[{name, Name},
              {vhost, VHost},
              {user, Username},
              {node, Node}] || #tracked_connection{name = Name, vhost = VHost, username = Username, node = Node} <- List];
        {FilterVHost, true} ->
            [[{name, Name},
              {vhost, VHost},
              {user, Username},
              {node, Node}] || #tracked_connection{name = Name, vhost = VHost, username = Username, node = Node} <- List, VHost == FilterVHost];
        {none, false} ->
            [[{name, Name},
              {vhost, VHost},
              {user, Username},
              {node, Node}] || #tracked_connection{name = Name, vhost = VHost, username = Username, node = Node} <- List, Username == FilterUsername];
        {FilterVHost, false} ->
            [[{name, Name},
              {vhost, VHost},
              {user, Username},
              {node, Node}] || #tracked_connection{name = Name, vhost = VHost, username = Username, node = Node} <- List, VHost == FilterVHost, Username == FilterUsername]
    end.

set_resp_header(K, V, ReqData) ->
    cowboy_req:set_resp_header(K, strip_crlf(V), ReqData).

strip_crlf(Str) -> lists:append(string:tokens(Str, "\r\n")).

args([]) -> args(#{});
args(L)  -> rabbit_mgmt_format:to_amqp_table(L).

%% Make replying to a post look like anything else...
post_respond({true, ReqData, Context}) ->
    {true, ReqData, Context};
post_respond({stop, ReqData, Context}) ->
    {stop, ReqData, Context};
post_respond({JSON, ReqData, Context}) ->
    {true, cowboy_req:set_resp_body(JSON, ReqData), Context}.

is_monitor(T) -> rabbit_web_dispatch_access_control:is_monitor(T).

%% The distinction between list_visible_vhosts and list_login_vhosts
%% is there to ensure that monitors can always learn of the
%% existence of all vhosts, and can always see their contribution to
%% global stats.

list_visible_vhosts_names(User) ->
    list_visible_vhosts(User, undefined).

list_visible_vhosts_names(User, AuthzData) ->
    list_visible_vhosts(User, AuthzData).

list_visible_vhosts(User) ->
    list_visible_vhosts(User, undefined).

list_visible_vhosts(User = #user{tags = Tags}, AuthzData) ->
    case is_monitor(Tags) of
        true  -> rabbit_vhost:list_names();
        false -> list_login_vhosts_names(User, AuthzData)
    end.

list_login_vhosts_names(User, AuthzData) ->
    [V || V <- rabbit_vhost:list_names(),
          case catch rabbit_access_control:check_vhost_access(User, V, AuthzData, #{}) of
              ok -> true;
              NotOK ->
                  log_access_control_result(NotOK),
                  false
          end].

list_login_vhosts(User, AuthzData) ->
    [V || V <- rabbit_vhost:all(),
          case catch rabbit_access_control:check_vhost_access(User, vhost:get_name(V), AuthzData, #{}) of
              ok -> true;
              NotOK ->
                  log_access_control_result(NotOK),
                  false
          end].

% rabbitmq/rabbitmq-auth-backend-http#100
log_access_control_result(NotOK) ->
    rabbit_log:debug("rabbit_access_control:check_vhost_access result: ~tp", [NotOK]).

%% base64:decode throws lots of weird errors. Catch and convert to one
%% that will cause a bad_request.
b64decode_or_throw(B64) ->
    rabbit_misc:b64decode_or_throw(B64).

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
            Now = os:system_time(milli_seconds),
            Last = Round(Now, Incr),
            #range{first = (Last - Age),
                   last  = Last,
                   incr  = Incr};
        true -> throw({error, invalid_range_parameters,
                    io_lib:format("Invalid range parameters: age ~tp, incr ~tp",
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
    case qs_val(list_to_binary(Name), ReqData) of
        undefined -> undefined;
        Bin       -> case catch list_to_integer(binary_to_list(Bin)) of
                         {'EXIT', _} -> undefined;
                         Integer     -> Integer
                     end
    end.

-spec qs_val(binary(), cowboy_req:req()) -> any() | undefined.
qs_val(Name, ReqData) ->
    Qs = cowboy_req:parse_qs(ReqData),
    proplists:get_value(Name, Qs, undefined).

-spec catch_no_such_user_or_vhost(fun(() -> Result), Replacement) -> Result | Replacement.
catch_no_such_user_or_vhost(Fun, Replacement) ->
    try
        Fun()
    catch throw:{error, {E, _}} when E =:= no_such_user; E =:= no_such_vhost ->
        Replacement()
    end.

%% this retains the old, buggy, pre 23.1 behavour of lists:sublist/3 where an
%% error is thrown when the request is out of range
sublist(List, S, L) when is_integer(L), L >= 0 ->
    lists:sublist(lists:nthtail(S-1, List), L).

-spec set_resp_not_found(binary(), cowboy_req:req()) -> cowboy_req:req().
set_resp_not_found(NotFoundBin, ReqData) ->
    ErrorMessage = case rabbit_mgmt_util:vhost(ReqData) of
        not_found ->
            <<"vhost_not_found">>;
        _ ->
            NotFoundBin
    end,
    ReqData1 = cowboy_req:set_resp_header(
        <<"content-type">>, <<"application/json">>, ReqData),
    cowboy_req:set_resp_body(rabbit_json:encode(#{
        <<"error">> => <<"not_found">>,
        <<"reason">> => ErrorMessage
    }), ReqData1).
