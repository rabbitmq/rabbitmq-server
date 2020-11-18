%% @author Bob Ippolito <bob@mochimedia.com>
%% @copyright 2007 Mochi Media, Inc.

%% @doc Utilities for parsing and quoting.

-module(rabbit_http_util).
-author('bob@mochimedia.com').
-export([join/2, quote_plus/1, urlencode/1, parse_qs/1, unquote/1]).
-export([path_split/1]).
-export([urlsplit/1, urlsplit_path/1, urlunsplit/1, urlunsplit_path/1]).
-export([parse_header/1]).
-export([shell_quote/1, cmd/1, cmd_string/1, cmd_port/2, cmd_status/1, cmd_status/2]).
-export([record_to_proplist/2, record_to_proplist/3]).
-export([safe_relative_path/1, partition/2]).
-export([parse_qvalues/1, pick_accepted_encodings/3]).
-export([make_io/1]).

-define(PERCENT, 37).  % $\%
-define(FULLSTOP, 46). % $\.
-define(IS_HEX(C), ((C >= $0 andalso C =< $9) orelse
                    (C >= $a andalso C =< $f) orelse
                    (C >= $A andalso C =< $F))).
-define(QS_SAFE(C), ((C >= $a andalso C =< $z) orelse
                     (C >= $A andalso C =< $Z) orelse
                     (C >= $0 andalso C =< $9) orelse
                     (C =:= ?FULLSTOP orelse C =:= $- orelse C =:= $~ orelse
                      C =:= $_))).

hexdigit(C) when C < 10 -> $0 + C;
hexdigit(C) when C < 16 -> $A + (C - 10).

unhexdigit(C) when C >= $0, C =< $9 -> C - $0;
unhexdigit(C) when C >= $a, C =< $f -> C - $a + 10;
unhexdigit(C) when C >= $A, C =< $F -> C - $A + 10.

%% @spec partition(String, Sep) -> {String, [], []} | {Prefix, Sep, Postfix}
%% @doc Inspired by Python 2.5's str.partition:
%%      partition("foo/bar", "/") = {"foo", "/", "bar"},
%%      partition("foo", "/") = {"foo", "", ""}.
partition(String, Sep) ->
    case partition(String, Sep, []) of
        undefined ->
            {String, "", ""};
        Result ->
            Result
    end.

partition("", _Sep, _Acc) ->
    undefined;
partition(S, Sep, Acc) ->
    case partition2(S, Sep) of
        undefined ->
            [C | Rest] = S,
            partition(Rest, Sep, [C | Acc]);
        Rest ->
            {lists:reverse(Acc), Sep, Rest}
    end.

partition2(Rest, "") ->
    Rest;
partition2([C | R1], [C | R2]) ->
    partition2(R1, R2);
partition2(_S, _Sep) ->
    undefined.



%% @spec safe_relative_path(string()) -> string() | undefined
%% @doc Return the reduced version of a relative path or undefined if it
%%      is not safe. safe relative paths can be joined with an absolute path
%%      and will result in a subdirectory of the absolute path. Safe paths
%%      never contain a backslash character.
safe_relative_path("/" ++ _) ->
    undefined;
safe_relative_path(P) ->
    case string:chr(P, $\\) of
        0 ->
           safe_relative_path(P, []);
        _ ->
           undefined
    end.

safe_relative_path("", Acc) ->
    case Acc of
        [] ->
            "";
        _ ->
            string:join(lists:reverse(Acc), "/")
    end;
safe_relative_path(P, Acc) ->
    case partition(P, "/") of
        {"", "/", _} ->
            %% /foo or foo//bar
            undefined;
        {"..", _, _} when Acc =:= [] ->
            undefined;
        {"..", _, Rest} ->
            safe_relative_path(Rest, tl(Acc));
        {Part, "/", ""} ->
            safe_relative_path("", ["", Part | Acc]);
        {Part, _, Rest} ->
            safe_relative_path(Rest, [Part | Acc])
    end.

%% @spec shell_quote(string()) -> string()
%% @doc Quote a string according to UNIX shell quoting rules, returns a string
%%      surrounded by double quotes.
shell_quote(L) ->
    shell_quote(L, [$\"]).

%% @spec cmd_port([string()], Options) -> port()
%% @doc open_port({spawn, mochiweb_util:cmd_string(Argv)}, Options).
cmd_port(Argv, Options) ->
    open_port({spawn, cmd_string(Argv)}, Options).

%% @spec cmd([string()]) -> string()
%% @doc os:cmd(cmd_string(Argv)).
cmd(Argv) ->
    os:cmd(cmd_string(Argv)).

%% @spec cmd_string([string()]) -> string()
%% @doc Create a shell quoted command string from a list of arguments.
cmd_string(Argv) ->
    string:join([shell_quote(X) || X <- Argv], " ").

%% @spec cmd_status([string()]) -> {ExitStatus::integer(), Stdout::binary()}
%% @doc Accumulate the output and exit status from the given application,
%%      will be spawned with cmd_port/2.
cmd_status(Argv) ->
    cmd_status(Argv, []).

%% @spec cmd_status([string()], [atom()]) -> {ExitStatus::integer(), Stdout::binary()}
%% @doc Accumulate the output and exit status from the given application,
%%      will be spawned with cmd_port/2.
cmd_status(Argv, Options) ->
    Port = cmd_port(Argv, [exit_status, stderr_to_stdout,
                           use_stdio, binary | Options]),
    try cmd_loop(Port, [])
    after catch port_close(Port)
    end.

%% @spec cmd_loop(port(), list()) -> {ExitStatus::integer(), Stdout::binary()}
%% @doc Accumulate the output and exit status from a port.
cmd_loop(Port, Acc) ->
    receive
        {Port, {exit_status, Status}} ->
            {Status, iolist_to_binary(lists:reverse(Acc))};
        {Port, {data, Data}} ->
            cmd_loop(Port, [Data | Acc])
    end.

%% @spec join([iolist()], iolist()) -> iolist()
%% @doc Join a list of strings or binaries together with the given separator
%%      string or char or binary. The output is flattened, but may be an
%%      iolist() instead of a string() if any of the inputs are binary().
join([], _Separator) ->
    [];
join([S], _Separator) ->
    lists:flatten(S);
join(Strings, Separator) ->
    lists:flatten(revjoin(lists:reverse(Strings), Separator, [])).

revjoin([], _Separator, Acc) ->
    Acc;
revjoin([S | Rest], Separator, []) ->
    revjoin(Rest, Separator, [S]);
revjoin([S | Rest], Separator, Acc) ->
    revjoin(Rest, Separator, [S, Separator | Acc]).

%% @spec quote_plus(atom() | integer() | float() | string() | binary()) -> string()
%% @doc URL safe encoding of the given term.
quote_plus(Atom) when is_atom(Atom) ->
    quote_plus(atom_to_list(Atom));
quote_plus(Int) when is_integer(Int) ->
    quote_plus(integer_to_list(Int));
quote_plus(Binary) when is_binary(Binary) ->
    quote_plus(binary_to_list(Binary));
quote_plus(Float) when is_float(Float) ->
    quote_plus(rabbit_numerical:digits(Float));
quote_plus(String) ->
    quote_plus(String, []).

quote_plus([], Acc) ->
    lists:reverse(Acc);
quote_plus([C | Rest], Acc) when ?QS_SAFE(C) ->
    quote_plus(Rest, [C | Acc]);
quote_plus([$\s | Rest], Acc) ->
    quote_plus(Rest, [$+ | Acc]);
quote_plus([C | Rest], Acc) ->
    <<Hi:4, Lo:4>> = <<C>>,
    quote_plus(Rest, [hexdigit(Lo), hexdigit(Hi), ?PERCENT | Acc]).

%% @spec urlencode([{Key, Value}]) -> string()
%% @doc URL encode the property list.
urlencode(Props) ->
    Pairs = lists:foldr(
              fun ({K, V}, Acc) ->
                      [quote_plus(K) ++ "=" ++ quote_plus(V) | Acc]
              end, [], Props),
    string:join(Pairs, "&").

%% @spec parse_qs(string() | binary()) -> [{Key, Value}]
%% @doc Parse a query string or application/x-www-form-urlencoded.
parse_qs(Binary) when is_binary(Binary) ->
    parse_qs(binary_to_list(Binary));
parse_qs(String) ->
    parse_qs(String, []).

parse_qs([], Acc) ->
    lists:reverse(Acc);
parse_qs(String, Acc) ->
    {Key, Rest} = parse_qs_key(String),
    {Value, Rest1} = parse_qs_value(Rest),
    parse_qs(Rest1, [{Key, Value} | Acc]).

parse_qs_key(String) ->
    parse_qs_key(String, []).

parse_qs_key([], Acc) ->
    {qs_revdecode(Acc), ""};
parse_qs_key([$= | Rest], Acc) ->
    {qs_revdecode(Acc), Rest};
parse_qs_key(Rest=[$; | _], Acc) ->
    {qs_revdecode(Acc), Rest};
parse_qs_key(Rest=[$& | _], Acc) ->
    {qs_revdecode(Acc), Rest};
parse_qs_key([C | Rest], Acc) ->
    parse_qs_key(Rest, [C | Acc]).

parse_qs_value(String) ->
    parse_qs_value(String, []).

parse_qs_value([], Acc) ->
    {qs_revdecode(Acc), ""};
parse_qs_value([$; | Rest], Acc) ->
    {qs_revdecode(Acc), Rest};
parse_qs_value([$& | Rest], Acc) ->
    {qs_revdecode(Acc), Rest};
parse_qs_value([C | Rest], Acc) ->
    parse_qs_value(Rest, [C | Acc]).

%% @spec unquote(string() | binary()) -> string()
%% @doc Unquote a URL encoded string.
unquote(Binary) when is_binary(Binary) ->
    unquote(binary_to_list(Binary));
unquote(String) ->
    qs_revdecode(lists:reverse(String)).

qs_revdecode(S) ->
    qs_revdecode(S, []).

qs_revdecode([], Acc) ->
    Acc;
qs_revdecode([$+ | Rest], Acc) ->
    qs_revdecode(Rest, [$\s | Acc]);
qs_revdecode([Lo, Hi, ?PERCENT | Rest], Acc) when ?IS_HEX(Lo), ?IS_HEX(Hi) ->
    qs_revdecode(Rest, [(unhexdigit(Lo) bor (unhexdigit(Hi) bsl 4)) | Acc]);
qs_revdecode([C | Rest], Acc) ->
    qs_revdecode(Rest, [C | Acc]).

%% @spec urlsplit(Url) -> {Scheme, Netloc, Path, Query, Fragment}
%% @doc Return a 5-tuple, does not expand % escapes. Only supports HTTP style
%%      URLs.
urlsplit(Url) ->
    {Scheme, Url1} = urlsplit_scheme(Url),
    {Netloc, Url2} = urlsplit_netloc(Url1),
    {Path, Query, Fragment} = urlsplit_path(Url2),
    {Scheme, Netloc, Path, Query, Fragment}.

urlsplit_scheme(Url) ->
    case urlsplit_scheme(Url, []) of
        no_scheme ->
            {"", Url};
        Res ->
            Res
    end.

urlsplit_scheme([C | Rest], Acc) when ((C >= $a andalso C =< $z) orelse
                                       (C >= $A andalso C =< $Z) orelse
                                       (C >= $0 andalso C =< $9) orelse
                                       C =:= $+ orelse C =:= $- orelse
                                       C =:= $.) ->
    urlsplit_scheme(Rest, [C | Acc]);
urlsplit_scheme([$: | Rest], Acc=[_ | _]) ->
    {string:to_lower(lists:reverse(Acc)), Rest};
urlsplit_scheme(_Rest, _Acc) ->
    no_scheme.

urlsplit_netloc("//" ++ Rest) ->
    urlsplit_netloc(Rest, []);
urlsplit_netloc(Path) ->
    {"", Path}.

urlsplit_netloc("", Acc) ->
    {lists:reverse(Acc), ""};
urlsplit_netloc(Rest=[C | _], Acc) when C =:= $/; C =:= $?; C =:= $# ->
    {lists:reverse(Acc), Rest};
urlsplit_netloc([C | Rest], Acc) ->
    urlsplit_netloc(Rest, [C | Acc]).


%% @spec path_split(string()) -> {Part, Rest}
%% @doc Split a path starting from the left, as in URL traversal.
%%      path_split("foo/bar") = {"foo", "bar"},
%%      path_split("/foo/bar") = {"", "foo/bar"}.
path_split(S) ->
    path_split(S, []).

path_split("", Acc) ->
    {lists:reverse(Acc), ""};
path_split("/" ++ Rest, Acc) ->
    {lists:reverse(Acc), Rest};
path_split([C | Rest], Acc) ->
    path_split(Rest, [C | Acc]).


%% @spec urlunsplit({Scheme, Netloc, Path, Query, Fragment}) -> string()
%% @doc Assemble a URL from the 5-tuple. Path must be absolute.
urlunsplit({Scheme, Netloc, Path, Query, Fragment}) ->
    lists:flatten([case Scheme of "" -> "";  _ -> [Scheme, "://"] end,
                   Netloc,
                   urlunsplit_path({Path, Query, Fragment})]).

%% @spec urlunsplit_path({Path, Query, Fragment}) -> string()
%% @doc Assemble a URL path from the 3-tuple.
urlunsplit_path({Path, Query, Fragment}) ->
    lists:flatten([Path,
                   case Query of "" -> ""; _ -> [$? | Query] end,
                   case Fragment of "" -> ""; _ -> [$# | Fragment] end]).

%% @spec urlsplit_path(Url) -> {Path, Query, Fragment}
%% @doc Return a 3-tuple, does not expand % escapes. Only supports HTTP style
%%      paths.
urlsplit_path(Path) ->
    urlsplit_path(Path, []).

urlsplit_path("", Acc) ->
    {lists:reverse(Acc), "", ""};
urlsplit_path("?" ++ Rest, Acc) ->
    {Query, Fragment} = urlsplit_query(Rest),
    {lists:reverse(Acc), Query, Fragment};
urlsplit_path("#" ++ Rest, Acc) ->
    {lists:reverse(Acc), "", Rest};
urlsplit_path([C | Rest], Acc) ->
    urlsplit_path(Rest, [C | Acc]).

urlsplit_query(Query) ->
    urlsplit_query(Query, []).

urlsplit_query("", Acc) ->
    {lists:reverse(Acc), ""};
urlsplit_query("#" ++ Rest, Acc) ->
    {lists:reverse(Acc), Rest};
urlsplit_query([C | Rest], Acc) ->
    urlsplit_query(Rest, [C | Acc]).

%% @spec parse_header(string()) -> {Type, [{K, V}]}
%% @doc  Parse a Content-Type like header, return the main Content-Type
%%       and a property list of options.
parse_header(String) ->
    %% TODO: This is exactly as broken as Python's cgi module.
    %%       Should parse properly like mochiweb_cookies.
    [Type | Parts] = [string:strip(S) || S <- string:tokens(String, ";")],
    F = fun (S, Acc) ->
                case lists:splitwith(fun (C) -> C =/= $= end, S) of
                    {"", _} ->
                        %% Skip anything with no name
                        Acc;
                    {_, ""} ->
                        %% Skip anything with no value
                        Acc;
                    {Name, [$\= | Value]} ->
                        [{string:to_lower(string:strip(Name)),
                          unquote_header(string:strip(Value))} | Acc]
                end
        end,
    {string:to_lower(Type),
     lists:foldr(F, [], Parts)}.

unquote_header("\"" ++ Rest) ->
    unquote_header(Rest, []);
unquote_header(S) ->
    S.

unquote_header("", Acc) ->
    lists:reverse(Acc);
unquote_header("\"", Acc) ->
    lists:reverse(Acc);
unquote_header([$\\, C | Rest], Acc) ->
    unquote_header(Rest, [C | Acc]);
unquote_header([C | Rest], Acc) ->
    unquote_header(Rest, [C | Acc]).

%% @spec record_to_proplist(Record, Fields) -> proplist()
%% @doc calls record_to_proplist/3 with a default TypeKey of '__record'
record_to_proplist(Record, Fields) ->
    record_to_proplist(Record, Fields, '__record').

%% @spec record_to_proplist(Record, Fields, TypeKey) -> proplist()
%% @doc Return a proplist of the given Record with each field in the
%%      Fields list set as a key with the corresponding value in the Record.
%%      TypeKey is the key that is used to store the record type
%%      Fields should be obtained by calling record_info(fields, record_type)
%%      where record_type is the record type of Record
record_to_proplist(Record, Fields, TypeKey)
  when tuple_size(Record) - 1 =:= length(Fields) ->
    lists:zip([TypeKey | Fields], tuple_to_list(Record)).


shell_quote([], Acc) ->
    lists:reverse([$\" | Acc]);
shell_quote([C | Rest], Acc) when C =:= $\" orelse C =:= $\` orelse
                                  C =:= $\\ orelse C =:= $\$ ->
    shell_quote(Rest, [C, $\\ | Acc]);
shell_quote([C | Rest], Acc) ->
    shell_quote(Rest, [C | Acc]).

%% @spec parse_qvalues(string()) -> [qvalue()] | invalid_qvalue_string
%% @type qvalue() = {media_type() | encoding() , float()}.
%% @type media_type() = string().
%% @type encoding() = string().
%%
%% @doc Parses a list (given as a string) of elements with Q values associated
%%      to them. Elements are separated by commas and each element is separated
%%      from its Q value by a semicolon. Q values are optional but when missing
%%      the value of an element is considered as 1.0. A Q value is always in the
%%      range [0.0, 1.0]. A Q value list is used for example as the value of the
%%      HTTP "Accept" and "Accept-Encoding" headers.
%%
%%      Q values are described in section 2.9 of the RFC 2616 (HTTP 1.1).
%%
%%      Example:
%%
%%      parse_qvalues("gzip; q=0.5, deflate, identity;q=0.0") ->
%%          [{"gzip", 0.5}, {"deflate", 1.0}, {"identity", 0.0}]
%%
parse_qvalues(QValuesStr) ->
    try
        lists:map(
            fun(Pair) ->
                [Type | Params] = string:tokens(Pair, ";"),
                NormParams = normalize_media_params(Params),
                {Q, NonQParams} = extract_q(NormParams),
                {string:join([string:strip(Type) | NonQParams], ";"), Q}
            end,
            string:tokens(string:to_lower(QValuesStr), ",")
        )
    catch
        _Type:_Error ->
            invalid_qvalue_string
    end.

normalize_media_params(Params) ->
    {ok, Re} = re:compile("\\s"),
    normalize_media_params(Re, Params, []).

normalize_media_params(_Re, [], Acc) ->
    lists:reverse(Acc);
normalize_media_params(Re, [Param | Rest], Acc) ->
    NormParam = re:replace(Param, Re, "", [global, {return, list}]),
    normalize_media_params(Re, Rest, [NormParam | Acc]).

extract_q(NormParams) ->
    {ok, KVRe} = re:compile("^([^=]+)=([^=]+)$"),
    {ok, QRe} = re:compile("^((?:0|1)(?:\\.\\d{1,3})?)$"),
    extract_q(KVRe, QRe, NormParams, []).

extract_q(_KVRe, _QRe, [], Acc) ->
    {1.0, lists:reverse(Acc)};
extract_q(KVRe, QRe, [Param | Rest], Acc) ->
    case re:run(Param, KVRe, [{capture, [1, 2], list}]) of
        {match, [Name, Value]} ->
            case Name of
            "q" ->
                {match, [Q]} = re:run(Value, QRe, [{capture, [1], list}]),
                QVal = case Q of
                    "0" ->
                        0.0;
                    "1" ->
                        1.0;
                    Else ->
                        list_to_float(Else)
                end,
                case QVal < 0.0 orelse QVal > 1.0 of
                false ->
                    {QVal, lists:reverse(Acc) ++ Rest}
                end;
            _ ->
                extract_q(KVRe, QRe, Rest, [Param | Acc])
            end
    end.

%% @spec pick_accepted_encodings([qvalue()], [encoding()], encoding()) ->
%%    [encoding()]
%%
%% @doc Determines which encodings specified in the given Q values list are
%%      valid according to a list of supported encodings and a default encoding.
%%
%%      The returned list of encodings is sorted, descendingly, according to the
%%      Q values of the given list. The last element of this list is the given
%%      default encoding unless this encoding is explicitly or implicitly
%%      marked with a Q value of 0.0 in the given Q values list.
%%      Note: encodings with the same Q value are kept in the same order as
%%            found in the input Q values list.
%%
%%      This encoding picking process is described in section 14.3 of the
%%      RFC 2616 (HTTP 1.1).
%%
%%      Example:
%%
%%      pick_accepted_encodings(
%%          [{"gzip", 0.5}, {"deflate", 1.0}],
%%          ["gzip", "identity"],
%%          "identity"
%%      ) ->
%%          ["gzip", "identity"]
%%
pick_accepted_encodings(AcceptedEncs, SupportedEncs, DefaultEnc) ->
    SortedQList = lists:reverse(
        lists:sort(fun({_, Q1}, {_, Q2}) -> Q1 < Q2 end, AcceptedEncs)
    ),
    {Accepted, Refused} = lists:foldr(
        fun({E, Q}, {A, R}) ->
            case Q > 0.0 of
                true ->
                    {[E | A], R};
                false ->
                    {A, [E | R]}
            end
        end,
        {[], []},
        SortedQList
    ),
    Refused1 = lists:foldr(
        fun(Enc, Acc) ->
            case Enc of
                "*" ->
                    lists:subtract(SupportedEncs, Accepted) ++ Acc;
                _ ->
                    [Enc | Acc]
            end
        end,
        [],
        Refused
    ),
    Accepted1 = lists:foldr(
        fun(Enc, Acc) ->
            case Enc of
                "*" ->
                    lists:subtract(SupportedEncs, Accepted ++ Refused1) ++ Acc;
                _ ->
                    [Enc | Acc]
            end
        end,
        [],
        Accepted
    ),
    Accepted2 = case lists:member(DefaultEnc, Accepted1) of
        true ->
            Accepted1;
        false ->
            Accepted1 ++ [DefaultEnc]
    end,
    [E || E <- Accepted2, lists:member(E, SupportedEncs),
        not lists:member(E, Refused1)].

make_io(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
make_io(Integer) when is_integer(Integer) ->
    integer_to_list(Integer);
make_io(Io) when is_list(Io); is_binary(Io) ->
    Io.

%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_io_test() ->
    ?assertEqual(
       <<"atom">>,
       iolist_to_binary(make_io(atom))),
    ?assertEqual(
       <<"20">>,
       iolist_to_binary(make_io(20))),
    ?assertEqual(
       <<"list">>,
       iolist_to_binary(make_io("list"))),
    ?assertEqual(
       <<"binary">>,
       iolist_to_binary(make_io(<<"binary">>))),
    ok.

-record(test_record, {field1=f1, field2=f2}).
record_to_proplist_test() ->
    ?assertEqual(
       [{'__record', test_record},
        {field1, f1},
        {field2, f2}],
       record_to_proplist(#test_record{}, record_info(fields, test_record))),
    ?assertEqual(
       [{'typekey', test_record},
        {field1, f1},
        {field2, f2}],
       record_to_proplist(#test_record{},
                          record_info(fields, test_record),
                          typekey)),
    ok.

shell_quote_test() ->
    ?assertEqual(
       "\"foo \\$bar\\\"\\`' baz\"",
       shell_quote("foo $bar\"`' baz")),
    ok.

cmd_port_test_spool(Port, Acc) ->
    receive
        {Port, eof} ->
            Acc;
        {Port, {data, {eol, Data}}} ->
            cmd_port_test_spool(Port, ["\n", Data | Acc]);
        {Port, Unknown} ->
            throw({unknown, Unknown})
    after 1000 ->
            throw(timeout)
    end.

cmd_port_test() ->
    Port = cmd_port(["echo", "$bling$ `word`!"],
                    [eof, stream, {line, 4096}]),
    Res = try lists:append(lists:reverse(cmd_port_test_spool(Port, [])))
          after catch port_close(Port)
          end,
    self() ! {Port, wtf},
    try cmd_port_test_spool(Port, [])
    catch throw:{unknown, wtf} -> ok
    end,
    try cmd_port_test_spool(Port, [])
    catch throw:timeout -> ok
    end,
    ?assertEqual(
       "$bling$ `word`!\n",
       Res).

cmd_test() ->
    ?assertEqual(
       "$bling$ `word`!\n",
       cmd(["echo", "$bling$ `word`!"])),
    ok.

cmd_string_test() ->
    ?assertEqual(
       "\"echo\" \"\\$bling\\$ \\`word\\`!\"",
       cmd_string(["echo", "$bling$ `word`!"])),
    ok.

cmd_status_test() ->
    ?assertEqual(
       {0, <<"$bling$ `word`!\n">>},
       cmd_status(["echo", "$bling$ `word`!"])),
    ok.


parse_header_test() ->
    ?assertEqual(
       {"multipart/form-data", [{"boundary", "AaB03x"}]},
       parse_header("multipart/form-data; boundary=AaB03x")),
    %% This tests (currently) intentionally broken behavior
    ?assertEqual(
       {"multipart/form-data",
        [{"b", ""},
         {"cgi", "is"},
         {"broken", "true\"e"}]},
       parse_header("multipart/form-data;b=;cgi=\"i\\s;broken=true\"e;=z;z")),
    ok.

path_split_test() ->
    {"", "foo/bar"} = path_split("/foo/bar"),
    {"foo", "bar"} = path_split("foo/bar"),
    {"bar", ""} = path_split("bar"),
    ok.

urlsplit_test() ->
    {"", "", "/foo", "", "bar?baz"} = urlsplit("/foo#bar?baz"),
    {"https", "host:port", "/foo", "", "bar?baz"} =
        urlsplit("https://host:port/foo#bar?baz"),
    {"https", "host", "", "", ""} = urlsplit("https://host"),
    {"", "", "/wiki/Category:Fruit", "", ""} =
        urlsplit("/wiki/Category:Fruit"),
    ok.

urlsplit_path_test() ->
    {"/foo/bar", "", ""} = urlsplit_path("/foo/bar"),
    {"/foo", "baz", ""} = urlsplit_path("/foo?baz"),
    {"/foo", "", "bar?baz"} = urlsplit_path("/foo#bar?baz"),
    {"/foo", "", "bar?baz#wibble"} = urlsplit_path("/foo#bar?baz#wibble"),
    {"/foo", "bar", "baz"} = urlsplit_path("/foo?bar#baz"),
    {"/foo", "bar?baz", "baz"} = urlsplit_path("/foo?bar?baz#baz"),
    ok.

urlunsplit_test() ->
    "/foo#bar?baz" = urlunsplit({"", "", "/foo", "", "bar?baz"}),
    "https://host:port/foo#bar?baz" =
        urlunsplit({"https", "host:port", "/foo", "", "bar?baz"}),
    ok.

urlunsplit_path_test() ->
    "/foo/bar" = urlunsplit_path({"/foo/bar", "", ""}),
    "/foo?baz" = urlunsplit_path({"/foo", "baz", ""}),
    "/foo#bar?baz" = urlunsplit_path({"/foo", "", "bar?baz"}),
    "/foo#bar?baz#wibble" = urlunsplit_path({"/foo", "", "bar?baz#wibble"}),
    "/foo?bar#baz" = urlunsplit_path({"/foo", "bar", "baz"}),
    "/foo?bar?baz#baz" = urlunsplit_path({"/foo", "bar?baz", "baz"}),
    ok.

join_test() ->
    ?assertEqual("foo,bar,baz",
                  join(["foo", "bar", "baz"], $,)),
    ?assertEqual("foo,bar,baz",
                  join(["foo", "bar", "baz"], ",")),
    ?assertEqual("foo bar",
                  join([["foo", " bar"]], ",")),
    ?assertEqual("foo bar,baz",
                  join([["foo", " bar"], "baz"], ",")),
    ?assertEqual("foo",
                  join(["foo"], ",")),
    ?assertEqual("foobarbaz",
                  join(["foo", "bar", "baz"], "")),
    ?assertEqual("foo" ++ [<<>>] ++ "bar" ++ [<<>>] ++ "baz",
                 join(["foo", "bar", "baz"], <<>>)),
    ?assertEqual("foobar" ++ [<<"baz">>],
                 join(["foo", "bar", <<"baz">>], "")),
    ?assertEqual("",
                 join([], "any")),
    ok.

quote_plus_test() ->
    "foo" = quote_plus(foo),
    "1" = quote_plus(1),
    "1.1" = quote_plus(1.1),
    "foo" = quote_plus("foo"),
    "foo+bar" = quote_plus("foo bar"),
    "foo%0A" = quote_plus("foo\n"),
    "foo%0A" = quote_plus("foo\n"),
    "foo%3B%26%3D" = quote_plus("foo;&="),
    "foo%3B%26%3D" = quote_plus(<<"foo;&=">>),
    ok.

unquote_test() ->
    ?assertEqual("foo bar",
                 unquote("foo+bar")),
    ?assertEqual("foo bar",
                 unquote("foo%20bar")),
    ?assertEqual("foo\r\n",
                 unquote("foo%0D%0A")),
    ?assertEqual("foo\r\n",
                 unquote(<<"foo%0D%0A">>)),
    ok.

urlencode_test() ->
    "foo=bar&baz=wibble+%0D%0A&z=1" = urlencode([{foo, "bar"},
                                                 {"baz", "wibble \r\n"},
                                                 {z, 1}]),
    ok.

parse_qs_test() ->
    ?assertEqual(
       [{"foo", "bar"}, {"baz", "wibble \r\n"}, {"z", "1"}],
       parse_qs("foo=bar&baz=wibble+%0D%0a&z=1")),
    ?assertEqual(
       [{"", "bar"}, {"baz", "wibble \r\n"}, {"z", ""}],
       parse_qs("=bar&baz=wibble+%0D%0a&z=")),
    ?assertEqual(
       [{"foo", "bar"}, {"baz", "wibble \r\n"}, {"z", "1"}],
       parse_qs(<<"foo=bar&baz=wibble+%0D%0a&z=1">>)),
    ?assertEqual(
       [],
       parse_qs("")),
    ?assertEqual(
       [{"foo", ""}, {"bar", ""}, {"baz", ""}],
       parse_qs("foo;bar&baz")),
    ok.

partition_test() ->
    {"foo", "", ""} = partition("foo", "/"),
    {"foo", "/", "bar"} = partition("foo/bar", "/"),
    {"foo", "/", ""} = partition("foo/", "/"),
    {"", "/", "bar"} = partition("/bar", "/"),
    {"f", "oo/ba", "r"} = partition("foo/bar", "oo/ba"),
    ok.

safe_relative_path_test() ->
    "foo" = safe_relative_path("foo"),
    "foo/" = safe_relative_path("foo/"),
    "foo" = safe_relative_path("foo/bar/.."),
    "bar" = safe_relative_path("foo/../bar"),
    "bar/" = safe_relative_path("foo/../bar/"),
    "" = safe_relative_path("foo/.."),
    "" = safe_relative_path("foo/../"),
    undefined = safe_relative_path("/foo"),
    undefined = safe_relative_path("../foo"),
    undefined = safe_relative_path("foo/../.."),
    undefined = safe_relative_path("foo//"),
    undefined = safe_relative_path("foo\\bar"),
    ok.

parse_qvalues_test() ->
    [] = parse_qvalues(""),
    [{"identity", 0.0}] = parse_qvalues("identity;q=0"),
    [{"identity", 0.0}] = parse_qvalues("identity ;q=0"),
    [{"identity", 0.0}] = parse_qvalues(" identity; q =0 "),
    [{"identity", 0.0}] = parse_qvalues("identity ; q = 0"),
    [{"identity", 0.0}] = parse_qvalues("identity ; q= 0.0"),
    [{"gzip", 1.0}, {"deflate", 1.0}, {"identity", 0.0}] = parse_qvalues(
        "gzip,deflate,identity;q=0.0"
    ),
    [{"deflate", 1.0}, {"gzip", 1.0}, {"identity", 0.0}] = parse_qvalues(
        "deflate,gzip,identity;q=0.0"
    ),
    [{"gzip", 1.0}, {"deflate", 1.0}, {"gzip", 1.0}, {"identity", 0.0}] =
        parse_qvalues("gzip,deflate,gzip,identity;q=0"),
    [{"gzip", 1.0}, {"deflate", 1.0}, {"identity", 0.0}] = parse_qvalues(
        "gzip, deflate , identity; q=0.0"
    ),
    [{"gzip", 1.0}, {"deflate", 1.0}, {"identity", 0.0}] = parse_qvalues(
        "gzip; q=1, deflate;q=1.0, identity;q=0.0"
    ),
    [{"gzip", 0.5}, {"deflate", 1.0}, {"identity", 0.0}] = parse_qvalues(
        "gzip; q=0.5, deflate;q=1.0, identity;q=0"
    ),
    [{"gzip", 0.5}, {"deflate", 1.0}, {"identity", 0.0}] = parse_qvalues(
        "gzip; q=0.5, deflate , identity;q=0.0"
    ),
    [{"gzip", 0.5}, {"deflate", 0.8}, {"identity", 0.0}] = parse_qvalues(
        "gzip; q=0.5, deflate;q=0.8, identity;q=0.0"
    ),
    [{"gzip", 0.5}, {"deflate", 1.0}, {"identity", 1.0}] = parse_qvalues(
        "gzip; q=0.5,deflate,identity"
    ),
    [{"gzip", 0.5}, {"deflate", 1.0}, {"identity", 1.0}, {"identity", 1.0}] =
        parse_qvalues("gzip; q=0.5,deflate,identity, identity "),
    [{"text/html;level=1", 1.0}, {"text/plain", 0.5}] =
        parse_qvalues("text/html;level=1, text/plain;q=0.5"),
    [{"text/html;level=1", 0.3}, {"text/plain", 1.0}] =
        parse_qvalues("text/html;level=1;q=0.3, text/plain"),
    [{"text/html;level=1", 0.3}, {"text/plain", 1.0}] =
        parse_qvalues("text/html; level = 1; q = 0.3, text/plain"),
    [{"text/html;level=1", 0.3}, {"text/plain", 1.0}] =
        parse_qvalues("text/html;q=0.3;level=1, text/plain"),
    invalid_qvalue_string = parse_qvalues("gzip; q=1.1, deflate"),
    invalid_qvalue_string = parse_qvalues("gzip; q=0.5, deflate;q=2"),
    invalid_qvalue_string = parse_qvalues("gzip, deflate;q=AB"),
    invalid_qvalue_string = parse_qvalues("gzip; q=2.1, deflate"),
    invalid_qvalue_string = parse_qvalues("gzip; q=0.1234, deflate"),
    invalid_qvalue_string = parse_qvalues("text/html;level=1;q=0.3, text/html;level"),
    ok.

pick_accepted_encodings_test() ->
    ["identity"] = pick_accepted_encodings(
        [],
        ["gzip", "identity"],
        "identity"
    ),
    ["gzip", "identity"] = pick_accepted_encodings(
        [{"gzip", 1.0}],
        ["gzip", "identity"],
        "identity"
    ),
    ["identity"] = pick_accepted_encodings(
        [{"gzip", 0.0}],
        ["gzip", "identity"],
        "identity"
    ),
    ["gzip", "identity"] = pick_accepted_encodings(
        [{"gzip", 1.0}, {"deflate", 1.0}],
        ["gzip", "identity"],
        "identity"
    ),
    ["gzip", "identity"] = pick_accepted_encodings(
        [{"gzip", 0.5}, {"deflate", 1.0}],
        ["gzip", "identity"],
        "identity"
    ),
    ["identity"] = pick_accepted_encodings(
        [{"gzip", 0.0}, {"deflate", 0.0}],
        ["gzip", "identity"],
        "identity"
    ),
    ["gzip"] = pick_accepted_encodings(
        [{"gzip", 1.0}, {"deflate", 1.0}, {"identity", 0.0}],
        ["gzip", "identity"],
        "identity"
    ),
    ["gzip", "deflate", "identity"] = pick_accepted_encodings(
        [{"gzip", 1.0}, {"deflate", 1.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["gzip", "deflate"] = pick_accepted_encodings(
        [{"gzip", 1.0}, {"deflate", 1.0}, {"identity", 0.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["deflate", "gzip", "identity"] = pick_accepted_encodings(
        [{"gzip", 0.2}, {"deflate", 1.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["deflate", "deflate", "gzip", "identity"] = pick_accepted_encodings(
        [{"gzip", 0.2}, {"deflate", 1.0}, {"deflate", 1.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["deflate", "gzip", "gzip", "identity"] = pick_accepted_encodings(
        [{"gzip", 0.2}, {"deflate", 1.0}, {"gzip", 1.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["gzip", "deflate", "gzip", "identity"] = pick_accepted_encodings(
        [{"gzip", 0.2}, {"deflate", 0.9}, {"gzip", 1.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    [] = pick_accepted_encodings(
        [{"*", 0.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["gzip", "deflate", "identity"] = pick_accepted_encodings(
        [{"*", 1.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["gzip", "deflate", "identity"] = pick_accepted_encodings(
        [{"*", 0.6}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["gzip"] = pick_accepted_encodings(
        [{"gzip", 1.0}, {"*", 0.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["gzip", "deflate"] = pick_accepted_encodings(
        [{"gzip", 1.0}, {"deflate", 0.6}, {"*", 0.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["deflate", "gzip"] = pick_accepted_encodings(
        [{"gzip", 0.5}, {"deflate", 1.0}, {"*", 0.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["gzip", "identity"] = pick_accepted_encodings(
        [{"deflate", 0.0}, {"*", 1.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ["gzip", "identity"] = pick_accepted_encodings(
        [{"*", 1.0}, {"deflate", 0.0}],
        ["gzip", "deflate", "identity"],
        "identity"
    ),
    ok.

-endif.
