%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016
%% @doc urilib is a RFC-3986 URI Library for Erlang
%%      https://github.com/gmr/urilib
%% @end
%% ====================================================================
-module(rabbitmq_aws_urilib).

-export([build/1,
         build_query_string/1,
         parse/1,
         parse_userinfo/1,
         parse_userinfo_result/1
        ]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("rabbitmq_aws.hrl").

-spec build(#uri{}) -> string().
%% @doc Build a URI string
%% @end
build(URI) ->
  {UserInfo, Host, Port} = URI#uri.authority,
  UriMap = #{
    scheme => to_list(URI#uri.scheme),
    host => Host
  },
  UriMap1 = case UserInfo of
    undefined         -> UriMap;
    {User, undefined} -> maps:put(userinfo, User, UriMap);
    {User, Password}  -> maps:put(userinfo, User ++ ":" ++ Password, UriMap);
    Value             -> maps:put(userinfo, Value, UriMap)
  end,
  UriMap2 = case Port of
    undefined -> UriMap1;
    Value1    -> maps:put(port, Value1, UriMap1)
  end,
  UriMap3 = case URI#uri.path of
    undefined -> maps:put(path, "", UriMap2);
    Value2    ->
      PrefixedPath = case string:slice(Value2, 0, 1) of
        "/" -> Value2;
        _   -> "/" ++ Value2
      end,
      maps:put(path, PrefixedPath, UriMap2)
  end,
  UriMap4 = case URI#uri.query of
    undefined -> UriMap3;
    ""        -> UriMap3;
    Value3    -> maps:put(query, build_query_string(Value3), UriMap3)
  end,
  UriMap5 = case URI#uri.fragment of
    undefined -> UriMap4;
    Value4    -> maps:put(fragment, Value4, UriMap4)
  end,
  uri_string:recompose(UriMap5).

-spec parse(string()) -> #uri{} | {error, any()}.
%% @doc Parse a URI string returning a record with the parsed results
%% @end
parse(Value) ->
  UriMap = uri_string:parse(Value),
  Scheme = maps:get(scheme, UriMap, "https"),
  Host = maps:get(host, UriMap),

  DefaultPort = case Scheme of
    "http"  -> 80;
    "https" -> 443;
    _       -> undefined
  end,
  Port = maps:get(port, UriMap, DefaultPort),
  UserInfo = parse_userinfo(maps:get(userinfo, UriMap, undefined)),
  Path = maps:get(path, UriMap),
  Query = maps:get(query, UriMap, ""),
  #uri{scheme = Scheme,
       authority = {parse_userinfo(UserInfo), Host, Port},
       path = Path,
       query = uri_string:dissect_query(Query),
       fragment = maps:get(fragment, UriMap, undefined)
  }.


-spec parse_userinfo(string() | undefined)
    -> {username() | undefined, password() | undefined} | undefined.
parse_userinfo(undefined) -> undefined;
parse_userinfo([]) -> undefined;
parse_userinfo({User, undefined}) -> {User, undefined};
parse_userinfo({User, Password})  -> {User, Password};
parse_userinfo(Value) ->
  parse_userinfo_result(string:tokens(Value, ":")).


-spec parse_userinfo_result(list())
    -> {username() | undefined, password() | undefined} | undefined.
parse_userinfo_result([User, Password]) -> {User, Password};
parse_userinfo_result([User]) -> {User, undefined};
parse_userinfo_result({User, undefined}) -> {User, undefined};
parse_userinfo_result([]) -> undefined;
parse_userinfo_result(User) -> {User, undefined}.

%% @spec build_query(proplist()) -> string()
%% @doc Build the query parameters string from a proplist
%% @end
%%

-spec build_query_string([{any(), any()}]) -> string().

build_query_string(Args) when is_list(Args) ->
  Normalized = [{to_list(K), to_list(V)} || {K, V} <- Args],
  uri_string:compose_query(Normalized).

-spec to_list(Val :: integer() | list() | binary() | atom() | map()) -> list().
to_list(Val) when is_list(Val)    -> Val;
to_list(Val) when is_map(Val)     -> maps:to_list(Val);
to_list(Val) when is_atom(Val)    -> atom_to_list(Val);
to_list(Val) when is_binary(Val)  -> binary_to_list(Val);
to_list(Val) when is_integer(Val) -> integer_to_list(Val).
