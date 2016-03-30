-module(httpc_aws_config).

%% API
-export([]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

%% @private
%% @spec config_file() -> Value
%% @doc Return the configuration file to test using either the value of the
%%      AWS_CONFIG_FILE or the default location where the file is expected to
%%      exist.
%% @where
%%       EnvVar = false | string()
%%       Value = string()
%% @end
%%
config_file() ->
  config_file(os:getenv("AWS_CONFIG_FILE")).

%% @private
%% @spec config_file(EnvVar) -> Value
%% @doc Return the configuration file to test using either the value of the
%%      AWS_CONFIG_FILE or the default location where the file is expected to
%%      exist.
%% @where
%%       EnvVar = false | string()
%%       Value = string()
%% @end
%%
config_file(EnvVar) when EnvVar =:= false ->
  filename:join([home_path(), ".aws", "config"]);
config_file(EnvVar) ->
  EnvVar.

%% @private
%% @spec config_file_data() -> Value
%% @doc Return the values from a configuration file as a proplist by section
%% @where
%%       Value = {ok, proplist()} | {error, atom}
%% @end
%%
config_file_data() ->
  ini_file_data(config_file()).

%% @private
%% @spec config_file() -> Value
%% @doc Return the shared credentials file to test using either the value of the
%%      AWS_SHARED_CREDENTIALS_FILE or the default location where the file
%%      is expected to exist.
%% @where
%%       EnvVar = false | string()
%%       Value = string()
%% @end
%%
credentials_file() ->
  credentials_file(os:getenv("AWS_SHARED_CREDENTIALS_FILE")).

%% @private
%% @spec credentials_file(EnvVar) -> Value
%% @doc Return the shared credentials file to test using either the value of the
%%      AWS_SHARED_CREDENTIALS_FILE_FILE or the default location where the file
%%      is expected to exist.
%% @where
%%       EnvVar = false | string()
%%       Value = string()
%% @end
%%
credentials_file(EnvVar) when EnvVar =:= false ->
  filename:join([home_path(), ".aws", "credentials"]);
credentials_file(EnvVar) ->
  EnvVar.

%% @private
%% @spec config_file_data() -> Value
%% @doc Return the values from a configuration file as a proplist by section
%% @where
%%       Value = {ok, proplist()} | {error, atom}
%% @end
%%
credentials_file_data() ->
  ini_file_data(credentials_file()).

%% @private
%% @spec home_path() -> Value
%% @doc Return the path to the current user's home directory, checking for the
%%      HOME environment variable before returning the current working
%%      directory if it's not set.
%% @where
%%       Value = string()
%% @end
%%
home_path() ->
  case os:getenv("HOME") of
    false -> filename:absname(".");
    Value -> Value
  end.


%% @private
%% @spec ini_file_data(Path) -> Value
%% @doc Return the parsed ini file for the specified path.
%% @where
%%       Path = string()
%%       Error = {ok, proplist()} | {error, atom()}
%% @end
%%
ini_file_data(Path) ->
  ini_file_data(Path, filelib:is_file(Path)).

%% @private
%% @spec ini_file_data(Path, FileExists) -> Value
%% @doc Return the parsed ini file for the specified path.
%% @where
%%       Path = string()
%%       FileExists = bool()
%%       Error = {ok, proplist()} | {error, atom()}
%% @end
%%
ini_file_data(Path, true) ->
  case read_file(Path) of
    {ok, Value} -> parse_file(Value, none, none, []);
    {error, Reason} -> {error, Reason}
  end;
ini_file_data(_, false) -> {error, enoent}.

-spec maybe_convert_number(list()) -> integer()|float().
%% @private
%% @spec maybe_convert_number(list()) -> integer()|float()|string().
%% @doc Returns an integer or float from a string if possible, otherwise
%%      returns the string().
%% @end
%%
maybe_convert_number(null) -> 0;
maybe_convert_number([]) -> 0;
maybe_convert_number(Value) when is_binary(Value) =:= true ->
  maybe_convert_number(binary_to_list(Value));
maybe_convert_number(Value) ->
    case string:to_float(string:strip(Value)) of
        {error,no_float} ->
          try
              list_to_integer(string:strip(Value))
          catch
              error:badarg  -> string:strip(Value)
          end;
        {F,_Rest} -> F
    end.

%% @private
%% @spec parse_file(File, Section, NestedKey, Parsed) -> Value
%% @doc Parse the AWS configuration INI file, returning a proplist
%% @where
%%       File = list()
%%       Section = string() | none
%%       NestedKey = string() | none
%%       Parsed = proplist()
%%       Value = {ok, proplist()} | {error, atom}
%% @end
%%
parse_file([], _, _, Parsed) -> Parsed;
parse_file([H|T], Section, NestedKey, Parsed) ->
  case re:run(H, "\\[([\\w\\s+\\-_]+)\\]", [{capture, all, list}]) of
    {match, [_, NewSection]} -> parse_file(T, NewSection, none, Parsed);
    nomatch ->
      Current = proplists:get_value(Section, Parsed, []),
      case string:tokens(string:strip(H), "=") of
        [Key, Value] ->
          KeyAtom = list_to_atom(string:strip(Key)),
          case NestedKey of
            none ->
              Updated = lists:keystore(KeyAtom, 1, Current, {KeyAtom, maybe_convert_number(Value)}),
              parse_file(T, Section, none, lists:keystore(Section, 1, Parsed, {Section, Updated}));
            _ ->
              Nested = proplists:get_value(NestedKey, Current, []),
              NestedValue = lists:keystore(NestedKey, 1, Nested, {KeyAtom, maybe_convert_number(Value)}),
              Updated = lists:keystore(NestedKey, 1, Current, {NestedKey, NestedValue}),
              parse_file(T, Section, NestedKey, lists:keystore(Section, 1, Parsed, {Section, Updated}))
          end;
        [] -> parse_file(T, Section, none, Parsed);
        KeyOnly ->
          Key = lists:nth(1, KeyOnly),
          KeyAtom = list_to_atom(string:strip(Key)),
          Updated = lists:keystore(KeyAtom, 1, Current, {KeyAtom, []}),
          parse_file(T, Section, KeyAtom, lists:keystore(Section, 1, Parsed, {Section, Updated}))
      end
  end.

%% @private
%% @spec read_file(Path) -> Value
%% @doc Read the specified file, returning the contents as a list of strings.
%% @where
%%       Path = string()
%%       Value = {ok, list()} | {error, atom}
%% @end
%%
read_file(Path) ->
  case file:open(Path, [read]) of
    {ok, Fd} -> read_file(Fd, []);
    {error, Reason} -> {error, Reason}
  end.

%% @private
%% @spec read_file(Fd, Acc) -> Value
%% @doc Read from the open file, accumulating the lines in a list.
%% @where
%%       Fd = pid()
%%       Acc = list()
%%       Value = list()
%% @end
%%
read_file(Fd, Lines) ->
  case file:read_line(Fd) of
    {ok, Value} -> read_file(Fd, lists:append(Lines, [string:strip(Value, right, $\n)]));
    eof -> {ok, Lines};
    {error, Reason} -> {error, Reason}
  end.
