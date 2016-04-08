%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @private
%% @doc httpc_aws configuration functionality
%% @end
%% ====================================================================
-module(httpc_aws_config).

%% API
-export([credentials/0,
         credentials/1,
         value/2,
         values/1,
         region/0,
         region/1]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("httpc_aws.hrl").

-spec credentials()
    -> {ok, access_key(), secret_access_key(), expiration(), security_token()}
    | {error, undefined}.
%% @spec credentials() -> Result
%% where
%%       Result = {ok, access_key(), secret_access_key(), expiration(), security_token()}
%%                | {error, undefined}
%%
%% @doc Return the credentials from environment variables, configuration or the
%%      EC2 local instance metadata server, if available.
%%
%%      If the ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` environment
%%      variables are set, those values will be returned. If they are not, the
%%      local configuration file or shared credentials file will be consulted.
%%      If either exists and can be checked, they will attempt to return the
%%      authentication credential values for the ``default`` profile if the
%%      ``AWS_DEFAULT_PROFILE`` environment is not set.
%%
%%      When checking for the configuration file, it will attempt to read the
%%      file from ``~/.aws/config`` if the ``AWS_CONFIG_FILE`` environment
%%      variable is not set. If the file is found, and both the access key and
%%      secret access key are set for the profile, they will be returned. If not
%%      it will attempt to consult the shared credentials file.
%%
%%      When checking for the shared credentials file, it will attempt to read
%%      read from ``~/.aws/credentials`` if the ``AWS_SHARED_CREDENTIALS_FILE``
%%      environment variable is not set. If the file is found and the both the
%%      access key and the secret access key are set for the profile, they will
%%      be returned.
%%
%%      If credentials are returned at any point up through this stage, they
%%      will be returned as ``{ok, AccessKey, SecretKey, undefined}``,
%%      indicating the credentials are locally configured, and are not
%%      temporary.
%%
%%      If no credentials could be resolved up until this point, there will be
%%      an attempt to contact a local EC2 instance metadata service for
%%      credentials.
%%
%%      When the EC2 instance metadata server is checked for but does not exist,
%%      the operation will timeout in 100ms (``?INSTANCE_CONNECT_TIMEOUT``).
%%
%%      If the service does exist, it will attempt to use the
%%      ``/meta-data/iam/security-credentials`` endpoint to request expiring
%%      request credentials to use. If they are found, a tuple of
%%      ``{ok, AccessKey, SecretAccessKey, SecurityToken}`` will be returned
%%      indicating the credentials are temporary and require the use of the
%%      ``X-Amz-Security-Token`` header should be used.
%%
%%      Finally, if no credentials are found by this point, an error tuple
%%      will be returned.
%%
%% @end
%%
credentials() ->
  credentials(profile()).

-spec credentials(string())
      -> {ok, access_key(), secret_access_key(), expiration(), security_token()}
      | {error, undefined}.
%% @spec credentials(Profile) -> Result
%% where
%%       Profile = string()
%%       Result = {ok, access_key(), secret_access_key(), expiration(), security_token()}
%%                | {error, undefined}
%%
%% @doc Return the credentials from environment variables, configuration or the
%%      EC2 local instance metadata server, if available.
%%
%%      If the ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` environment
%%      variables are set, those values will be returned. If they are not, the
%%      local configuration file or shared credentials file will be consulted.
%%
%%      When checking for the configuration file, it will attempt to read the
%%      file from ``~/.aws/config`` if the ``AWS_CONFIG_FILE`` environment
%%      variable is not set. If the file is found, and both the access key and
%%      secret access key are set for the profile, they will be returned. If not
%%      it will attempt to consult the shared credentials file.
%%
%%      When checking for the shared credentials file, it will attempt to read
%%      read from ``~/.aws/credentials`` if the ``AWS_SHARED_CREDENTIALS_FILE``
%%      environment variable is not set. If the file is found and the both the
%%      access key and the secret access key are set for the profile, they will
%%      be returned.
%%
%%      If credentials are returned at any point up through this stage, they
%%      will be returned as ``{ok, AccessKey, SecretKey, undefined}``,
%%      indicating the credentials are locally configured, and are not
%%      temporary.
%%
%%      If no credentials could be resolved up until this point, there will be
%%      an attempt to contact a local EC2 instance metadata service for
%%      credentials.
%%
%%      When the EC2 instance metadata server is checked for but does not exist,
%%      the operation will timeout in 100ms (``?INSTANCE_CONNECT_TIMEOUT``).
%%
%%      If the service does exist, it will attempt to use the
%%      ``/meta-data/iam/security-credentials`` endpoint to request expiring
%%      request credentials to use. If they are found, a tuple of
%%      ``{ok, AccessKey, SecretAccessKey, SecurityToken}`` will be returned
%%      indicating the credentials are temporary and require the use of the
%%      ``X-Amz-Security-Token`` header should be used.
%%
%%      Finally, if no credentials are found by this point, an error tuple
%%      will be returned.
%% @end
%%
credentials(Profile) ->
  lookup_credentials(Profile,
                     os:getenv("AWS_ACCESS_KEY_ID"),
                     os:getenv("AWS_SECRET_ACCESS_KEY")).


%% @spec region() -> Result
%% @doc Return the region as configured by ``AWS_DEFAULT_REGION`` environment
%%      variable or as configured in the configuration file using the default
%%      profile or configured ``AWS_DEFAULT_PROFILE`` environment variable.
%%
%%      If the environment variable is not set and a configuration
%%      file is not found, it will try and return the region from the EC2
%%      local instance metadata server.
%% @where
%%       Result = string() | {error, undefined}
%% @end
%%
region() ->
  region(profile()).


%% @spec region(Profile) -> Result
%% @doc Return the region as configured by ``AWS_DEFAULT_REGION`` environment
%%      variable or as configured in the configuration file using the specified
%%      profile.
%%
%%      If the environment variable is not set and a configuration
%%      file is not found, it will try and return the region from the EC2
%%      local instance metadata server.
%% @where
%%       Profile = string()
%%       Result = string() | {error, undefined}
%% @end
%%
region(Profile) ->
  lookup_region(Profile, os:getenv("AWS_DEFAULT_REGION")).


%% @doc Return the configuration data for the specified profile or an error
%%      if the profile is not found.
%% @end
value(Profile, Key) ->
  case values(Profile) of
    {error, Reason} ->
      {error, Reason};
    Settings ->
      proplists:get_value(Key, Settings, {error, undefined})
  end.


%% @spec values(Profile) -> Settings
%% @doc Return the configuration data for the specified profile or an error
%%      if the profile is not found.
%% @where
%%       Profile = string()
%%       Settings = proplist()|{error, enoent}|{error,undefined}
%% @end
%%
values(Profile) ->
  case config_file_data() of
    {error, Reason} ->
      {error, Reason};
    Settings ->
      Prefixed = lists:flatten(["profile ", Profile]),
      proplists:get_value(Profile, Settings,
                          proplists:get_value(Prefixed,
                                              Settings, {error, undefined}))
  end.


%% -----------------------------------------------------------------------------
%% Private / Internal Methods
%% -----------------------------------------------------------------------------


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
config_file(false) ->
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
%%      AWS_SHARED_CREDENTIALS_FILE or the default location where the file
%%      is expected to exist.
%% @where
%%       EnvVar = false | string()
%%       Value = string()
%% @end
%%
credentials_file(false) ->
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


-spec ini_format_key(any()) -> atom() | {error, type}.
%% @doc Converts a ini file key to an atom, stripping any leading whitespace
%% @end
%%
ini_format_key(Key) ->
  case io_lib:printable_list(Key) of
    true -> list_to_atom(string:strip(Key));
    false -> {error, type}
  end.


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
  {ok, NewParent, NewSettings} = ini_parse_section(H, NewSectionName,
                                                   Parent, Settings),
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
%% @spec ini_parse_line_parts(Section, Parts) -> Response
%% @doc Parse the AWS configuration INI file, returning a proplist
%% @where
%%       Section = proplist()
%%       Parts = list()
%%       NewSection = proplist()
%%       NewParent = atom()
%%       Response = {ok, NewSection} | {new_parent, Parent}
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


-spec instance_availability_zone_url() -> string().
%% @spec instance_availability_zone_url() -> string().
%% @doc Return the URL for querying the availability zone from the Instance
%%      Metadata service
%% @end
%%
instance_availability_zone_url() ->
  instance_metadata_url(string:join(lists:append(?INSTANCE_METADATA_BASE,
                                                 ?INSTANCE_AZ), "/")).


%% @private
%% @spec instance_credentials_url(Role) -> string().
%% @doc Return the URL for querying temporary credentials from the Instance
%%      Metadata service for the specified role
%% @where
%%       Role = string()
%% @end
%%
instance_credentials_url(Role) ->
  Base = lists:append(?INSTANCE_METADATA_BASE, ?INSTANCE_CREDENTIALS),
  instance_metadata_url(string:join(lists:append(Base, [Role]), "/")).


instance_metadata_url(Path) ->
  httpc_aws_urilib:build(undefined, undefined, undefined, ?INSTANCE_HOST,
                         undefined, Path, [], undefined).


%% @private
%% @spec instance_role_url() -> string().
%% @doc Return the URL for querying the role associated with the current
%%      instance from the Instance Metadata service
%% @end
%%
instance_role_url() ->
  instance_metadata_url(string:join(lists:append(?INSTANCE_METADATA_BASE,
                                                 ?INSTANCE_CREDENTIALS), "/")).


%% @private
%% @spec lookup_credentials(Profile, AccessKey, SecretAccessKey) -> Result.
%% @doc Return the access key and secret access key if they are set in
%%      environment variables, otherwise lookup the credentials from the config
%%      file for the specified profile.
%% @where
%%       Profile = string()
%%       AccessKey = string()
%%       SecretAccessKey = string()
%%       Result = {ok, AccessKey, SecretAccessKey, Expiration, SecurityToken} |
%%                {error, undefined}
%%       Expiration = string() | undefined
%%       SecurityToken = string() | undefined

%% @end
%%
lookup_credentials(Profile, false, false) ->
  lookup_credentials_from_config(Profile,
                                 value(Profile, aws_access_key_id),
                                 value(Profile, aws_secret_access_key));
lookup_credentials(Profile, false, _) ->
  lookup_credentials(Profile, false, false);
lookup_credentials(Profile, _, false) ->
  lookup_credentials(Profile, false, false);
lookup_credentials(_, AccessKey, SecretKey) ->
  {ok, AccessKey, SecretKey, undefined, undefined}.


%% @private
%% @spec lookup_credentials_from_config(Profile, AccessKey, SecretAccessKey) ->
%%          Result.
%% @doc Return the access key and secret access key if they are set in
%%      for the specified profile in the config file, if it exists. If it does
%%      not exist or the profile is not set or the values are not set in the
%%      profile, look up the values in the shared credentials file
%% @where
%%       Profile = string()
%%       AccessKey = string() | {error, undefined}
%%       SecretAccessKey = string() | {error, undefined}
%%       Result = {ok, AccessKey, SecretAccessKey, Expiration, SecurityToken} |
%%                {error, undefined}
%%       Expiration = string() | undefined
%%       SecurityToken = string() | undefined
%% @end
%%
lookup_credentials_from_config(Profile, {error,_}, {error,_}) ->
  lookup_credentials_from_shared_creds_file(Profile, credentials_file_data());
lookup_credentials_from_config(Profile, {error,_}, _) ->
  lookup_credentials_from_shared_creds_file(Profile, credentials_file_data());
lookup_credentials_from_config(Profile, _, {error,_}) ->
  lookup_credentials_from_shared_creds_file(Profile, credentials_file_data());
lookup_credentials_from_config(_, AccessKey, SecretKey) ->
  {ok, AccessKey, SecretKey, undefined, undefined}.


%% @private
%% @spec lookup_credentials_from_shared_creds_file(Profile, Credentials) ->
%%          Result.
%% @doc Check to see if the shared credentials file exists and if it does,
%%      invoke ``lookup_credentials_from_shared_creds_section/2`` to attempt to
%%      get the credentials values out of it. If the file does not exist,
%%      attempt to lookup the values from the EC2 instance metadata service.
%% @where
%%       Profile = string()
%%       Credentials = proplist() | {error, enoent}
%%       Result = {ok, AccessKey, SecretAccessKey, Expiration, SecurityToken} |
%%                {error, undefined}
%%       AccessKey = string() | {error, undefined}
%%       SecretAccessKey = string() | {error, undefined}
%%       Expiration = string() | undefined
%%       SecurityToken = string() | undefined

%% @end
%%
lookup_credentials_from_shared_creds_file(_, {error,_}) ->
  lookup_credentials_from_instance_metadata();
lookup_credentials_from_shared_creds_file(Profile, Credentials) ->
  lookup_credentials_from_shared_creds_section(proplists:get_value(Profile,
                                                                   Credentials,
                                                                   undefined)).


%% @private
%% @spec lookup_credentials_from_shared_creds_section(Profile, Credentials) ->
%%          Result.
%% @doc Return the access key and secret access key if they are set in
%%      for the specified profile from the shared credentials file. If the
%%      profile is not set or the values are not set in the profile, attempt to
%%      lookup the values from the EC2 instance metadata service.
%% @where
%%       Profile = string()
%%       Credentials = proplist() | {error, enoent}
%%       Result = {ok, AccessKey, SecretAccessKey, Expiration, SecurityToken} |
%%                {error, undefined}
%%       AccessKey = string() | {error, undefined}
%%       SecretAccessKey = string() | {error, undefined}
%%       Expiration = string() | undefined
%%       SecurityToken = string() | undefined
%% @end
%%
lookup_credentials_from_shared_creds_section(undefined) ->
  lookup_credentials_from_instance_metadata();
lookup_credentials_from_shared_creds_section(Credentials) ->
  case {proplists:get_value(aws_access_key_id, Credentials, undefined),
        proplists:get_value(aws_secret_access_key, Credentials, undefined)} of
    {undefined, undefined} -> lookup_credentials_from_instance_metadata();
    {undefined, _} -> lookup_credentials_from_instance_metadata();
    {_, undefined} -> lookup_credentials_from_instance_metadata();
    {AccessKey, SecretKey} -> {ok, AccessKey, SecretKey, undefined, undefined}
  end.


%% @private
%% @spec lookup_credentials_from_instance_metadata() -> Result.
%% @doc Attempt to lookup the values from the EC2 instance metadata service.
%% @where
%%       Result = {ok, AccessKey, SecretAccessKey, Expiration, SecurityToken} |
%%                {error, undefined}
%%       AccessKey = string()
%%       SecretAccessKey = string()
%%       Expiration = string()
%%       SecurityToken = string()
%% @end
%%
lookup_credentials_from_instance_metadata() ->
  case maybe_get_role_from_instance_metadata() of
    undefined -> {error, undefined};
    Role -> maybe_get_credentials_from_instance_metadata(Role)
  end.


%% @private
%% @spec lookup_region(Profile, Region) -> Result.
%% @doc If Region is false, lookup the region from the config or the EC2
%%      instance metadata service.
%% @where
%%       Profile = string()
%%       Region = string() | false
%%       Result = {ok, string()} | {error, undefined}
%% @end
%%
lookup_region(Profile, false) ->
  lookup_region_from_config(values(Profile));
lookup_region(_, Region) -> {ok, Region}.


-spec lookup_region_from_config(list() | tuple())
  -> {ok, nonempty_string()}
  | {error, undefined}.
%% @doc Return the region from the local configuration file. If local config
%%      settings are not found, try to lookup the region from the EC2 instance
%%      metadata service.
%% @end
%% ;
lookup_region_from_config({error,enoent}) ->
  maybe_get_region_from_instance_metadata();
lookup_region_from_config(Settings) ->
  case proplists:get_value(region, Settings) of
    undefined -> lookup_region_from_config({error,enoent});
    Region -> {ok, Region}
  end.


-spec maybe_convert_number(string()) -> integer() | float().
%% @spec maybe_convert_number(list()) -> integer()|float()|string().
%% @doc Returns an integer or float from a string if possible, otherwise
%%      returns the string().
%% @end
%%
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
%% @spec maybe_get_credentials_from_instance_metadata(Role) -> Result
%% @doc Try to query the EC2 local instance metadata service to get temporary
%%      authentication credentials.
%% @where
%%       Role = string()
%%       Result = {ok, string()} | undefined
%% @end
%%
maybe_get_credentials_from_instance_metadata({ok, Role}) ->
  Response = httpc:request(get, {instance_credentials_url(Role), []},
                           [{connect_timeout, ?INSTANCE_CONNECT_TIMEOUT}], []),
  parse_credentials_response(Response).


-spec maybe_get_region_from_instance_metadata() -> tuple().
%% @doc Try to query the EC2 local instance metadata service to get the region
%% @end
%%
maybe_get_region_from_instance_metadata() ->
  case httpc:request(get, {instance_availability_zone_url(), []},
                     [{connect_timeout, ?INSTANCE_CONNECT_TIMEOUT}], []) of
    {ok, {_Status, _Headers, Body}} -> {ok, region_from_availability_zone(Body)};
    {error, _} -> {error, undefined}
  end.


%% @private
%% @spec maybe_get_role_from_instance_metadata() -> Result
%% @doc Try to query the EC2 local instance metadata service to get the role
%%      assigned to the instance.
%% @where
%%       Result = {ok, string()} | undefined
%% @end
%%
maybe_get_role_from_instance_metadata() ->
  case httpc:request(get, {instance_role_url(), []},
                     [{connect_timeout, ?INSTANCE_CONNECT_TIMEOUT}], []) of
    {ok, {_Status, _Headers, Body}} -> {ok, Body};
    {error, _} -> undefined
  end.


-spec parse_credentials_response(tuple())
  -> undefined
  | {ok, string(), string(), string(), string()}.
%% @spec parse_credentials_response(Response) -> Result
%% @doc Try to query the EC2 local instance metadata service to get the role
%%      assigned to the instance.
%% @where
%%       Result = {ok, string()} | undefined
%% @end
%%
parse_credentials_response({error, _}) -> {error, undefined};
parse_credentials_response({ok, {{_, 404, _}, _, _}}) -> {error, undefined};
parse_credentials_response({ok, {{_, 200, _}, _, Body}}) ->
  Parsed = jsx:decode(list_to_binary(Body)),
  {ok,
   binary_to_list(proplists:get_value(<<"AccessKeyId">>, Parsed)),
   binary_to_list(proplists:get_value(<<"SecretAccessKey">>, Parsed)),
   binary_to_list(proplists:get_value(<<"Expiration">>, Parsed)),
   binary_to_list(proplists:get_value(<<"Token">>, Parsed))}.


%% @private
%% @spec profile() -> string()
%% @doc Return the value of the AWS_DEFAULT_PROFILE environment variable or the
%%      "default" profile.
%% @end
%%
profile() ->
  case os:getenv("AWS_DEFAULT_PROFILE") of
    false -> ?DEFAULT_PROFILE;
    Value -> Value
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


%% @private
%% @spec region_from_availability_zone(string()) -> string()
%% @doc Strip the availability zone suffix from the region.
%% @end
%%
region_from_availability_zone(Value) ->
  string:sub_string(Value, 1, length(Value) - 1).
