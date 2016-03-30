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
    {ok, Lines}     -> ini_parse_lines(Lines, none, none, []);
    {error, Reason} -> {error, Reason}
  end;
ini_file_data(_, false) -> {error, enoent}.


%% @private
%% @spec maybe_convert_number(string()|atom()) -> atom()|{error, type}.
%% @doc Converts a ini file key to an atom, stripping any leading whitespace
%% @end
%%
ini_format_key(Key) when is_atom(Key) -> Key;
ini_format_key(Key) when is_list(Key) -> list_to_atom(string:strip(Key));
ini_format_key(_) -> {error, type}.


%% @private
%% @spec ini_parse_lines(Lines, SectionName, ParentKey, Settings) -> Value
%% @doc Parse the AWS configuration INI file
%% @where
%%       Lines = list()
%%       SectionName = string() | none
%%       ParentKey = string() | none
%%       Settings = proplist()
%%       Value = {ok, proplist()} | {error, atom}
%% @end
%%
ini_parse_lines([], _, _, Settings) -> Settings;
ini_parse_lines([H|T], SectionName, Parent, Settings) ->
  {ok, NewSectionName} = ini_parse_section_name(SectionName, H),
  {ok, NewParent, NewSettings} = ini_parse_section(H, NewSectionName, Parent, Settings),
  ini_parse_lines(T, NewSectionName, NewParent, NewSettings).


%% @private
%% @spec ini_parse_section_name(CurrentSection, Line) -> Result
%% @doc Attempts to parse a section name from the current line, returning either
%%      the new parsed section name, or the current section name.
%% @where
%%       CurrentSection = list()|none
%%       Line = binary()
%%       Result = {match, string()} | nomatch
%% @end
%%
ini_parse_section_name(CurrentSection, Line) ->
  Value = binary_to_list(Line),
  case re:run(Value, "\\[([\\w\\s+\\-_]+)\\]", [{capture, all, list}]) of
    {match, [_, SectionName]} -> {ok, SectionName};
    nomatch -> {ok, CurrentSection}
  end.


%% @private
%% @spec ini_parse_section(Line, SectionName, Parent, Settings) -> Value
%% @doc Parse a line from the ini file, returning it as part of the appropriate
%%      section.
%% @where
%%       Line = binary()
%%       SectionName = string() | none
%%       Parent = atom() | none
%%       Settings = proplist()
%%       Value = {string(), string(), proplist()}
%% @end
%%
ini_parse_section(Line, SectionName, Parent, Settings) ->
  Section = proplists:get_value(SectionName, Settings, []),
  {NewSection, NewParent} = ini_parse_line(Section, Parent, Line),
  {ok, NewParent, lists:keystore(SectionName, 1, Settings,
                                 {SectionName, NewSection})}.


%% @private
%% @spec ini_parse_line(Section, Parent, Line) -> {NewSection, NewParent}
%% @doc Parse the AWS configuration INI file, returning a proplist
%% @where
%%       Section = string() | none
%%       Parent = atom() | none
%%       Line = binary()
%%       NewSection = string() | none
%%       NewParent = atom() | None
%% @end
%%
ini_parse_line(Section, Parent, <<" ", Line/binary>>) ->
  Child = proplists:get_value(Parent, Section, []),
  {ok, NewChild} = ini_parse_line_parts(Child, ini_split_line(Line)),
  {lists:keystore(Parent, 1, Section, {Parent, NewChild}), Parent};
ini_parse_line(Section, _, Line) ->
  case ini_parse_line_parts(Section, ini_split_line(Line)) of
    {ok, NewSection} -> {NewSection, none};
    {new_parent, Parent} -> {Section, Parent}
  end.


%% @private
%% @spec ini_parse_line_parts(Section, Parts) -> {ok, NewSection}|{new_parent, Parent}
%% @doc Parse the AWS configuration INI file, returning a proplist
%% @where
%%       Section = proplist()
%%       Parts = list()
%%       NewSection = proplist()
%%       NewParent = atom()
%% @end
%%
ini_parse_line_parts(Section, []) -> {ok, Section};
ini_parse_line_parts(Section, [RawKey, Value]) ->
  Key = ini_format_key(RawKey),
  {ok, lists:keystore(Key, 1, Section, {Key, maybe_convert_number(Value)})};
ini_parse_line_parts(_, [RawKey]) ->
  {new_parent, ini_format_key(RawKey)}.


%% @private
%% @spec ini_split_line(binary()) -> list()
%% @doc Split a key value pair delimited by ``=`` to a list of strings.
%% @end
%%
ini_split_line(Line) ->
  string:tokens(string:strip(binary_to_list(Line)), "=").


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
  Stripped = string:strip(Value),
  case string:to_float(Stripped) of
      {error,no_float} ->
        try
            list_to_integer(Stripped)
        catch
            error:badarg -> Stripped
        end;
      {F,_Rest} -> F
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
    {ok, Value} ->
      Line = string:strip(Value, right, $\n),
      read_file(Fd, lists:append(Lines, [list_to_binary(Line)]));
    eof -> {ok, Lines};
    {error, Reason} -> {error, Reason}
  end.
