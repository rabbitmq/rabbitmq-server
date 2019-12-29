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
         percent_decode/1,
         percent_encode/1]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("rabbitmq_aws.hrl").

-ifdef (OTP_RELEASE).
  -if(?OTP_RELEASE >= 23).
    -compile({nowarn_deprecated_function, [{http_uri, decode, 1},
                                           {http_uri, encode, 1},
                                           {http_uri, parse,  1},
                                           {http_uri, parse,  2},
                                           {http_uri, scheme_defaults, 0}]}).

    -ignore_xref([{http_uri, decode, 1},
                 {http_uri, encode, 1},
                 {http_uri, parse,  1},
                 {http_uri, parse,  2},
                 {http_uri, scheme_defaults, 0}]).
  -endif.
-endif.

-spec build(#uri{}) -> string().
%% @doc Build a URI string
%% @end
build(URI) ->
  U1 = url_add_scheme(URI#uri.scheme),
  {UserInfo, Host, Port} = URI#uri.authority,
  U2 = url_maybe_add_userinfo(UserInfo, U1),
  U3 = url_add_host_and_port(URI#uri.scheme, Host, Port, U2),
  U4 = url_add_path(URI#uri.path, U3),
  U5 = url_maybe_add_qargs(URI#uri.query, U4),
  url_maybe_add_fragment(URI#uri.fragment, U5).


-spec build_query_string(QueryArgs :: list()) -> string().
%% @doc build the query string, properly encoding the URI values
%% @end
build_query_string(QArgs) ->
  string:join([url_maybe_encode_query_arg(Arg) || Arg <- QArgs], "&").


-spec parse(string()) -> #uri{} | {error, any()}.
%% @doc Parse a URI string returning a record with the parsed results
%% @end
parse(Value) ->
  case http_uri:parse(Value, [{scheme_defaults, http_uri:scheme_defaults()},
    {fragment, true}]) of
    {ok, {Scheme, UserInfo, Host, Port, Path, Query, Fragment}} ->
      #uri{scheme=Scheme,
        authority={parse_userinfo(UserInfo), Host, Port},
        path=Path,
        query=parse_query(Query),
        fragment=parse_fragment(Fragment)};
    {ok, {Scheme, UserInfo, Host, Port, Path, Query}} ->
      #uri{scheme=Scheme,
        authority={parse_userinfo(UserInfo), Host, Port},
        path=Path,
        query=parse_query(Query)};
    {error, Reason} ->
      {error, Reason}
  end.


-spec percent_encode(string()) -> string().
%% @doc Percent encode a string value.
%% @end
percent_encode(Value) ->
  url_encode(Value).


-spec percent_decode(string()) -> string().
%% @doc Decode a percent encoded string value.
%% @end
percent_decode(Value) ->
  http_uri:decode(Value).

%% Private Functions

-spec parse_fragment(string()) -> string() | undefined.
%% @private
parse_fragment([]) -> undefined;
parse_fragment(Value) -> Value.


-spec parse_query(string()) -> [tuple() | string()] | undefined.
%% @private
parse_query(Query) ->
  QArgs = re:split(Query, "[&|?]", [{return, list}]),
  parse_query_result([split_query_arg(Arg) || Arg <- QArgs, Arg =/= []]).


-spec parse_query_result(string()) -> [tuple() | string()] | undefined.
parse_query_result([]) ->
  undefined;
parse_query_result(QArgs) ->
  QArgs.


-spec parse_userinfo(string())
    -> {username() | undefined, password() | undefined}
  | undefined.
parse_userinfo(Value) ->
  parse_userinfo_result(string:tokens(Value, ":")).


-spec parse_userinfo_result(list())
    -> {username() | undefined, password() | undefined}
  | undefined.
parse_userinfo_result([User, Password]) -> {User, Password};
parse_userinfo_result([User]) -> {User, undefined};
parse_userinfo_result([]) -> undefined.


-spec split_query_arg(string()) -> {string(), string()} | undefined.
%% @private
split_query_arg(Argument) ->
  case string:tokens(Argument, "=") of
    [K, V] -> {percent_decode(K), percent_decode(V)};
    [Value] -> percent_decode(Value)
  end.


-spec url_add_scheme(string() | undefined) -> string().
url_add_scheme(undefined) -> "http://";
url_add_scheme(Scheme) -> string:concat(atom_to_list(Scheme), "://").


-spec url_maybe_add_userinfo(UserInfo :: {Username :: username() | undefined,
  Password :: password() | undefined}
| undefined,
  string()) -> string().
url_maybe_add_userinfo(undefined, URL) -> URL;
url_maybe_add_userinfo({undefined, undefined}, URL) -> URL;
url_maybe_add_userinfo({[], []}, URL) -> URL;
url_maybe_add_userinfo({Username, []}, URL) ->
  url_maybe_add_userinfo({Username, undefined}, URL);
url_maybe_add_userinfo({Username, undefined}, URL) ->
  string:concat(URL, string:concat(Username, "@"));
url_maybe_add_userinfo({Username, Password}, URL) ->
  string:concat(URL, string:concat(string:join([Username, Password], ":"), "@")).


-spec url_add_host_and_port(Scheme :: atom() | undefined,
  Host :: string(),
  Port :: integer() | undefined,
  URL :: string()) -> string().
url_add_host_and_port(undefined, Host, undefined, URL) ->
  string:concat(URL, Host);
url_add_host_and_port(http, Host, undefined, URL) ->
  string:concat(URL, Host);
url_add_host_and_port(http, Host, 80, URL) ->
  string:concat(URL, Host);
url_add_host_and_port(https, Host, undefined, URL) ->
  string:concat(URL, Host);
url_add_host_and_port(https, Host, 443, URL) ->
  string:concat(URL, Host);
url_add_host_and_port(_, Host, Port, URL) ->
  string:concat(URL, string:join([Host, integer_to_list(Port)], ":")).


-spec url_add_path(string(), string()) -> string().
url_add_path(undefined, URL) ->
  string:concat(URL, "/");
url_add_path(Path, URL) ->
  Escaped = string:join([url_escape_path_segment(P) || P <- string:tokens(Path, "/")], "/"),
  string:join([URL, Escaped], "/").


-spec url_escape_path_segment(string()) -> string().
url_escape_path_segment(Value) ->
  percent_encode(percent_decode(Value)).


-spec url_maybe_add_qargs([tuple() | string()], string()) -> string().
url_maybe_add_qargs(undefined, URL) -> URL;
url_maybe_add_qargs([], URL) -> URL;
url_maybe_add_qargs(QArgs, URL) ->
  QStr = string:join([url_maybe_encode_query_arg(Arg) || Arg <- QArgs], "&"),
  string:join([URL, QStr], "?").


-spec url_maybe_encode_query_arg(tuple() | string()) -> string().
url_maybe_encode_query_arg({K, V}) ->
  string:join([percent_encode(K), percent_encode(V)], "=");
url_maybe_encode_query_arg(V) ->
  percent_encode(V).


-spec url_maybe_add_fragment(string() | undefined, string()) -> string().
url_maybe_add_fragment(undefined, URL) -> URL;
url_maybe_add_fragment([], URL) -> URL;
url_maybe_add_fragment(Value, URL) ->
  Fragment = case string:left(Value, 1) of
               "#" -> http_uri:encode(string:sub_string(Value, 2));
               _ -> http_uri:encode(Value)
             end,
  string:join([URL, Fragment], "#").


%% ====================================================================
%% The following code originally from erlcloud
%% https://github.com/erlcloud/erlcloud
%% @copyright (C) 2010 Brian Buchanan. All rights reserved.
%% ====================================================================


url_encode(String) ->
  url_encode(String, []).
url_encode([], Accum) ->
  lists:reverse(Accum);
url_encode([Char|String], Accum)
  when Char >= $A, Char =< $Z;
  Char >= $a, Char =< $z;
  Char >= $0, Char =< $9;
  Char =:= $-; Char =:= $_;
  Char =:= $.; Char =:= $~ ->
  url_encode(String, [Char|Accum]);
url_encode([Char|String], Accum) ->
  url_encode(String, utf8_encode_char(Char) ++ Accum).


utf8_encode_char(Char) when Char > 16#7FFF, Char =< 16#7FFFF ->
  encode_char(Char band 16#3F + 16#80) ++
    encode_char((16#3F band (Char bsr 6)) + 16#80) ++
    encode_char((16#3F band (Char bsr 12)) + 16#80) ++
    encode_char((Char bsr 18) + 16#F0);
utf8_encode_char(Char) when Char > 16#7FF, Char =< 16#7FFF ->
  encode_char(Char band 16#3F + 16#80) ++
    encode_char((16#3F band (Char bsr 6)) + 16#80) ++
    encode_char((Char bsr 12) + 16#E0);
utf8_encode_char(Char) when Char > 16#7F, Char =< 16#7FF ->
  encode_char(Char band 16#3F + 16#80) ++
  encode_char((Char bsr 6) + 16#C0);
utf8_encode_char(Char) when Char =< 16#7F ->
  encode_char(Char).


encode_char(Char) ->
  [hex_char(Char rem 16), hex_char(Char div 16), $%].


hex_char(C) when C < 10 -> $0 + C;
hex_char(C) when C < 16 -> $A + C - 10.
