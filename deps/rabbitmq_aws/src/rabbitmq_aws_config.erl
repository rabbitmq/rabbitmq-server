%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @copyright 2016-2022 VMware, Inc. or its affiliates.
%% @private
%% @doc rabbitmq_aws configuration functionality
%% @end
%% ====================================================================
-module(rabbitmq_aws_config).

%% API
-export([credentials/0,
         credentials/1,
         value/2,
         values/1,
         instance_metadata_url/1,
         instance_credentials_url/1,
         instance_availability_zone_url/0,
         instance_role_url/0,
         instance_id_url/0,
         instance_id/0,
         load_imdsv2_token/0,
         instance_metadata_request_headers/0,
         region/0,
         region/1]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("rabbitmq_aws.hrl").

-spec credentials() -> security_credentials().
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
%%      the operation will timeout in ``?DEFAULT_HTTP_TIMEOUT``ms.
%%
%%      When the EC2 instance metadata server exists, but data is not returned
%%      quickly, the operation will timeout in ``?DEFAULT_HTTP_TIMEOUT``ms.
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
credentials() ->
  credentials(profile()).

-spec credentials(string()) -> security_credentials().
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
%%      the operation will timeout in ``?DEFAULT_HTTP_TIMEOUT``ms.
%%
%%      When the EC2 instance metadata server exists, but data is not returned
%%      quickly, the operation will timeout in ``?DEFAULT_HTTP_TIMEOUT``ms.
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
credentials(Profile) ->
  lookup_credentials(Profile,
                     os:getenv("AWS_ACCESS_KEY_ID"),
                     os:getenv("AWS_SECRET_ACCESS_KEY")).


-spec region() -> {ok, string()}.
%% @doc Return the region as configured by ``AWS_DEFAULT_REGION`` environment
%%      variable or as configured in the configuration file using the default
%%      profile or configured ``AWS_DEFAULT_PROFILE`` environment variable.
%%
%%      If the environment variable is not set and a configuration
%%      file is not found, it will try and return the region from the EC2
%%      local instance metadata server.
%% @end
region() ->
  region(profile()).


-spec region(Region :: string()) -> {ok, region()}.
%% @doc Return the region as configured by ``AWS_DEFAULT_REGION`` environment
%%      variable or as configured in the configuration file using the specified
%%      profile.
%%
%%      If the environment variable is not set and a configuration
%%      file is not found, it will try and return the region from the EC2
%%      local instance metadata server.
%% @end
region(Profile) ->
  case lookup_region(Profile, os:getenv("AWS_DEFAULT_REGION")) of
    {ok, Region} -> {ok, Region};
    _ -> {ok, ?DEFAULT_REGION}
  end.


-spec instance_id() -> string() | error.
%% @doc Return the instance ID from the EC2 metadata service.
%% @end
instance_id() ->
  URL = instance_id_url(),
  parse_body_response(perform_http_get_instance_metadata(URL)).


-spec value(Profile :: string(), Key :: atom())
  -> Value :: any() | {error, Reason :: atom()}.
%% @doc Return the configuration data for the specified profile or an error
%%      if the profile is not found.
%% @end
value(Profile, Key) ->
  get_value(Key, values(Profile)).


-spec values(Profile :: string())
  -> Settings :: list()
  | {error, Reason :: atom()}.
%% @doc Return the configuration data for the specified profile or an error
%%      if the profile is not found.
%% @end
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


-spec config_file() -> string().
%% @doc Return the configuration file to test using either the value of the
%%      AWS_CONFIG_FILE or the default location where the file is expected to
%%      exist.
%% @end
config_file() ->
  config_file(os:getenv("AWS_CONFIG_FILE")).


-spec config_file(Path :: false | string()) -> string().
%% @doc Return the configuration file to test using either the value of the
%%      AWS_CONFIG_FILE or the default location where the file is expected to
%%      exist.
%% @end
config_file(false) ->
  filename:join([home_path(), ".aws", "config"]);
config_file(EnvVar) ->
  EnvVar.


-spec config_file_data() -> list() | {error, Reason :: atom()}.
%% @doc Return the values from a configuration file as a proplist by section
%% @end
config_file_data() ->
  ini_file_data(config_file()).


-spec credentials_file() -> string().
%% @doc Return the shared credentials file to test using either the value of the
%%      AWS_SHARED_CREDENTIALS_FILE or the default location where the file
%%      is expected to exist.
%% @end
credentials_file() ->
  credentials_file(os:getenv("AWS_SHARED_CREDENTIALS_FILE")).


-spec credentials_file(Path :: false | string()) -> string().
%% @doc Return the shared credentials file to test using either the value of the
%%      AWS_SHARED_CREDENTIALS_FILE or the default location where the file
%%      is expected to exist.
%% @end
credentials_file(false) ->
  filename:join([home_path(), ".aws", "credentials"]);
credentials_file(EnvVar) ->
  EnvVar.

-spec credentials_file_data() -> list() | {error, Reason :: atom()}.
%% @doc Return the values from a configuration file as a proplist by section
%% @end
credentials_file_data() ->
  ini_file_data(credentials_file()).


-spec get_value(Key :: atom(), Settings :: list()) -> any();
               (Key :: atom(), {error, Reason :: atom()}) -> {error, Reason :: atom()}.
%% @doc Get the value for a key from a settings proplist.
%% @end
get_value(Key, Settings) when is_list(Settings) ->
  proplists:get_value(Key, Settings, {error, undefined});
get_value(_, {error, Reason}) -> {error, Reason}.


-spec home_path() -> string().
%% @doc Return the path to the current user's home directory, checking for the
%%      HOME environment variable before returning the current working
%%      directory if it's not set.
%% @end
home_path() ->
  home_path(os:getenv("HOME")).


-spec home_path(Value :: string() | false) -> string().
%% @doc Return the path to the current user's home directory, checking for the
%%      HOME environment variable before returning the current working
%%      directory if it's not set.
%% @end
home_path(false) -> filename:absname(".");
home_path(Value) -> Value.


-spec ini_file_data(Path :: string())
  -> {ok, list()} | {error, atom()}.
%% @doc Return the parsed ini file for the specified path.
%% @end
ini_file_data(Path) ->
  ini_file_data(Path, filelib:is_file(Path)).


-spec ini_file_data(Path :: string(), FileExists :: true | false)
  -> {ok, list()} | {error, atom()}.
%% @doc Return the parsed ini file for the specified path.
%% @end
ini_file_data(Path, true) ->
  case read_file(Path) of
    {ok, Lines}     -> ini_parse_lines(Lines, none, none, []);
    {error, Reason} -> {error, Reason}
  end;
ini_file_data(_, false) -> {error, enoent}.


-spec ini_format_key(any()) -> atom() | {error, type}.
%% @doc Converts a ini file key to an atom, stripping any leading whitespace
%% @end
ini_format_key(Key) ->
  case io_lib:printable_list(Key) of
    true -> list_to_atom(string:strip(Key));
    false -> {error, type}
  end.


-spec ini_parse_line(Section :: list(),
                     Key :: atom(),
                     Line :: binary())
  -> {Section :: list(), Key :: string() | none}.
%% @doc Parse the AWS configuration INI file, returning a proplist
%% @end
ini_parse_line(Section, Parent, <<" ", Line/binary>>) ->
  Child = proplists:get_value(Parent, Section, []),
  {ok, NewChild} = ini_parse_line_parts(Child, ini_split_line(Line)),
  {lists:keystore(Parent, 1, Section, {Parent, NewChild}), Parent};
ini_parse_line(Section, _, Line) ->
  case ini_parse_line_parts(Section, ini_split_line(Line)) of
    {ok, NewSection} -> {NewSection, none};
    {new_parent, Parent} -> {Section, Parent}
  end.


-spec ini_parse_line_parts(Section :: list(),
                           Parts :: list())
  -> {ok, list()} | {new_parent, atom()}.
%% @doc Parse the AWS configuration INI file, returning a proplist
%% @end
ini_parse_line_parts(Section, []) -> {ok, Section};
ini_parse_line_parts(Section, [RawKey, Value]) ->
  Key = ini_format_key(RawKey),
  {ok, lists:keystore(Key, 1, Section, {Key, maybe_convert_number(Value)})};
ini_parse_line_parts(_, [RawKey]) ->
  {new_parent, ini_format_key(RawKey)}.


-spec ini_parse_lines(Lines::[binary()],
                      SectionName :: string() | atom(),
                      Parent :: atom(),
                      Accumulator :: list())
  -> list().
%% @doc Parse the AWS configuration INI file
%% @end
ini_parse_lines([], _, _, Settings) -> Settings;
ini_parse_lines([H|T], SectionName, Parent, Settings) ->
  {ok, NewSectionName} = ini_parse_section_name(SectionName, H),
  {ok, NewParent, NewSettings} = ini_parse_section(H, NewSectionName,
                                                   Parent, Settings),
  ini_parse_lines(T, NewSectionName, NewParent, NewSettings).


-spec ini_parse_section(Line :: binary(),
                        SectionName :: string(),
                        Parent :: atom(),
                        Section :: list())
  -> {ok, NewParent :: atom(), Section :: list()}.
%% @doc Parse a line from the ini file, returning it as part of the appropriate
%%      section.
%% @end
ini_parse_section(Line, SectionName, Parent, Settings) ->
  Section = proplists:get_value(SectionName, Settings, []),
  {NewSection, NewParent} = ini_parse_line(Section, Parent, Line),
  {ok, NewParent, lists:keystore(SectionName, 1, Settings,
                                 {SectionName, NewSection})}.


-spec ini_parse_section_name(CurrentSection :: string() | atom(),
                             Line :: binary())
  -> {ok, SectionName :: string()}.
%% @doc Attempts to parse a section name from the current line, returning either
%%      the new parsed section name, or the current section name.
%% @end
ini_parse_section_name(CurrentSection, Line) ->
  Value = binary_to_list(Line),
  case re:run(Value, "\\[([\\w\\s+\\-_]+)\\]", [{capture, all, list}]) of
    {match, [_, SectionName]} -> {ok, SectionName};
    nomatch -> {ok, CurrentSection}
  end.


-spec ini_split_line(binary()) -> list().
%% @doc Split a key value pair delimited by ``=`` to a list of strings.
%% @end
ini_split_line(Line) ->
  string:tokens(string:strip(binary_to_list(Line)), "=").


-spec instance_availability_zone_url() -> string().
%% @doc Return the URL for querying the availability zone from the Instance
%%      Metadata service
%% @end
instance_availability_zone_url() ->
  instance_metadata_url(string:join([?INSTANCE_METADATA_BASE, ?INSTANCE_AZ], "/")).


-spec instance_credentials_url(string()) -> string().
%% @doc Return the URL for querying temporary credentials from the Instance
%%      Metadata service for the specified role
%% @end
instance_credentials_url(Role) ->
  instance_metadata_url(string:join([?INSTANCE_METADATA_BASE, ?INSTANCE_CREDENTIALS, Role], "/")).


-spec instance_metadata_url(string()) -> string().
%% @doc Build the Instance Metadata service URL for the specified path
%% @end
instance_metadata_url(Path) ->
  rabbitmq_aws_urilib:build(#uri{scheme = "http",
                                 authority = {undefined, ?INSTANCE_HOST, undefined},
                                 path = Path, query = []}).


-spec instance_role_url() -> string().
%% @doc Return the URL for querying the role associated with the current
%%      instance from the Instance Metadata service
%% @end
instance_role_url() ->
  instance_metadata_url(string:join([?INSTANCE_METADATA_BASE, ?INSTANCE_CREDENTIALS], "/")).

-spec imdsv2_token_url() -> string().
%% @doc Return the URL for obtaining EC2 IMDSv2 token from the Instance Metadata service.
%% @end
imdsv2_token_url() ->
  instance_metadata_url(?TOKEN_URL).

-spec instance_id_url() -> string().
%% @doc Return the URL for querying the id of the current instance from the Instance Metadata service.
%% @end
instance_id_url() ->
  instance_metadata_url(string:join([?INSTANCE_METADATA_BASE, ?INSTANCE_ID], "/")).


-spec lookup_credentials(Profile :: string(),
                         AccessKey :: string() | false,
                         SecretKey :: string() | false)
    -> security_credentials().
%% @doc Return the access key and secret access key if they are set in
%%      environment variables, otherwise lookup the credentials from the config
%%      file for the specified profile.
%% @end
lookup_credentials(Profile, false, _) ->
  lookup_credentials_from_config(Profile,
                                 value(Profile, aws_access_key_id),
                                 value(Profile, aws_secret_access_key));
lookup_credentials(Profile, _, false) ->
  lookup_credentials_from_config(Profile,
                                 value(Profile, aws_access_key_id),
                                 value(Profile, aws_secret_access_key));
lookup_credentials(_, AccessKey, SecretKey) ->
  {ok, AccessKey, SecretKey, undefined, undefined}.


-spec lookup_credentials_from_config(Profile :: string(),
                                     access_key() | {error, Reason :: atom()},
                                     secret_access_key()| {error, Reason :: atom()})
    -> security_credentials().
%% @doc Return the access key and secret access key if they are set in
%%      for the specified profile in the config file, if it exists. If it does
%%      not exist or the profile is not set or the values are not set in the
%%      profile, look up the values in the shared credentials file
%% @end
lookup_credentials_from_config(Profile, {error,_}, _) ->
  lookup_credentials_from_file(Profile, credentials_file_data());
lookup_credentials_from_config(_, AccessKey, SecretKey) ->
  {ok, AccessKey, SecretKey, undefined, undefined}.


-spec lookup_credentials_from_file(Profile :: string(),
                                   Credentials :: list())
    -> security_credentials().
%% @doc Check to see if the shared credentials file exists and if it does,
%%      invoke ``lookup_credentials_from_shared_creds_section/2`` to attempt to
%%      get the credentials values out of it. If the file does not exist,
%%      attempt to lookup the values from the EC2 instance metadata service.
%% @end
lookup_credentials_from_file(_, {error,_}) ->
  lookup_credentials_from_instance_metadata();
lookup_credentials_from_file(Profile, Credentials) ->
  Section = proplists:get_value(Profile, Credentials),
  lookup_credentials_from_section(Section).


-spec lookup_credentials_from_section(Credentials :: list() | undefined)
    -> security_credentials().
%% @doc Return the access key and secret access key if they are set in
%%      for the specified profile from the shared credentials file. If the
%%      profile is not set or the values are not set in the profile, attempt to
%%      lookup the values from the EC2 instance metadata service.
%% @end
lookup_credentials_from_section(undefined) ->
  lookup_credentials_from_instance_metadata();
lookup_credentials_from_section(Credentials) ->
  AccessKey = proplists:get_value(aws_access_key_id, Credentials, undefined),
  SecretKey = proplists:get_value(aws_secret_access_key, Credentials, undefined),
  lookup_credentials_from_proplist(AccessKey, SecretKey).


-spec lookup_credentials_from_proplist(AccessKey :: access_key(),
                                       SecretAccessKey :: secret_access_key())
    -> security_credentials().
%% @doc Process the contents of the Credentials proplists checking if the
%%      access key and secret access key are both set.
%% @end
lookup_credentials_from_proplist(undefined, _) ->
  lookup_credentials_from_instance_metadata();
lookup_credentials_from_proplist(_, undefined) ->
  lookup_credentials_from_instance_metadata();
lookup_credentials_from_proplist(AccessKey, SecretKey) ->
  {ok, AccessKey, SecretKey, undefined, undefined}.


-spec lookup_credentials_from_instance_metadata()
    -> security_credentials().
%% @spec lookup_credentials_from_instance_metadata() -> Result.
%% @doc Attempt to lookup the values from the EC2 instance metadata service.
%% @end
lookup_credentials_from_instance_metadata() ->
  Role = maybe_get_role_from_instance_metadata(),
  maybe_get_credentials_from_instance_metadata(Role).


-spec lookup_region(Profile :: string(),
                    Region :: false | string())
  -> {ok, string()} | {error, undefined}.
%% @doc If Region is false, lookup the region from the config or the EC2
%%      instance metadata service.
%% @end
lookup_region(Profile, false) ->
  lookup_region_from_config(values(Profile));
lookup_region(_, Region) -> {ok, Region}.


-spec lookup_region_from_config(Settings :: list() | {error, enoent})
  -> {ok, string()} | {error, undefined}.
%% @doc Return the region from the local configuration file. If local config
%%      settings are not found, try to lookup the region from the EC2 instance
%%      metadata service.
%% @end
lookup_region_from_config({error, enoent}) ->
  maybe_get_region_from_instance_metadata();
lookup_region_from_config(Settings) ->
  lookup_region_from_settings(proplists:get_value(region, Settings)).


-spec lookup_region_from_settings(any() | undefined)
  -> {ok, string()} | {error, undefined}.
%% @doc Decide if the region should be loaded from the Instance Metadata service
%%      of if it's already set.
%% @end
lookup_region_from_settings(undefined) ->
  maybe_get_region_from_instance_metadata();
lookup_region_from_settings(Region) ->
  {ok, Region}.


-spec maybe_convert_number(string()) -> integer() | float().
%% @doc Returns an integer or float from a string if possible, otherwise
%%      returns the string().
%% @end
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


-spec maybe_get_credentials_from_instance_metadata({ok, Role :: string()} |
                                                   {error, undefined})
  ->  security_credentials().
%% @doc Try to query the EC2 local instance metadata service to get temporary
%%      authentication credentials.
%% @end
maybe_get_credentials_from_instance_metadata({error, undefined}) ->
  {error, undefined};
maybe_get_credentials_from_instance_metadata({ok, Role}) ->
  URL = instance_credentials_url(Role),
  parse_credentials_response(perform_http_get_instance_metadata(URL)).


-spec maybe_get_region_from_instance_metadata()
  -> {ok, Region :: string()} | {error, Reason :: atom()}.
%% @doc Try to query the EC2 local instance metadata service to get the region
%% @end
maybe_get_region_from_instance_metadata() ->
  URL = instance_availability_zone_url(),
  parse_az_response(perform_http_get_instance_metadata(URL)).


%% @doc Try to query the EC2 local instance metadata service to get the role
%%      assigned to the instance.
%% @end
maybe_get_role_from_instance_metadata() ->
  URL = instance_role_url(),
  parse_body_response(perform_http_get_instance_metadata(URL)).


-spec parse_az_response(httpc_result())
  -> {ok, Region :: string()} | {error, Reason :: atom()}.
%% @doc Parse the response from the Availability Zone query to the
%%      Instance Metadata service, returning the Region if successful.
%% end.
parse_az_response({error, _}) -> {error, undefined};
parse_az_response({ok, {{_, 200, _}, _, Body}})
  -> {ok, region_from_availability_zone(Body)};
parse_az_response({ok, {{_, _, _}, _, _}}) -> {error, undefined}.


-spec parse_body_response(httpc_result())
 -> {ok, Value :: string()} | {error, Reason :: atom()}.
%% @doc Parse the return response from the Instance Metadata Service where the
%%      body value is the string to process.
%% end.
parse_body_response({error, _}) -> {error, undefined};
parse_body_response({ok, {{_, 200, _}, _, Body}}) -> {ok, Body};
parse_body_response({ok, {{_, 401, _}, _, _}}) ->
  rabbit_log:error(get_instruction_on_instance_metadata_error("Unauthorized instance metadata service request.")),
  {error, undefined};
parse_body_response({ok, {{_, 403, _}, _, _}}) ->
  rabbit_log:error(get_instruction_on_instance_metadata_error("The request is not allowed or the instance metadata service is turned off.")),
  {error, undefined};
parse_body_response({ok, {{_, _, _}, _, _}}) -> {error, undefined}.


-spec parse_credentials_response(httpc_result()) -> security_credentials().
%% @doc Try to query the EC2 local instance metadata service to get the role
%%      assigned to the instance.
%% @end
parse_credentials_response({error, _}) -> {error, undefined};
parse_credentials_response({ok, {{_, 404, _}, _, _}}) -> {error, undefined};
parse_credentials_response({ok, {{_, 200, _}, _, Body}}) ->
  Parsed = rabbitmq_aws_json:decode(Body),
  {ok,
   proplists:get_value("AccessKeyId", Parsed),
   proplists:get_value("SecretAccessKey", Parsed),
   parse_iso8601_timestamp(proplists:get_value("Expiration", Parsed)),
   proplists:get_value("Token", Parsed)}.


-spec perform_http_get_instance_metadata(string()) -> httpc_result().
%% @doc Wrap httpc:get/4 to simplify Instance Metadata service v2 requests
%% @end
perform_http_get_instance_metadata(URL) ->
  rabbit_log:debug("Querying instance metadata service: ~p", [URL]),
  httpc:request(get, {URL, instance_metadata_request_headers()},
    [{timeout, ?DEFAULT_HTTP_TIMEOUT}], []).

-spec get_instruction_on_instance_metadata_error(string()) -> string().
%% @doc Return error message on failures related to EC2 Instance Metadata Service with a reference to AWS document.
%% end
get_instruction_on_instance_metadata_error(ErrorMessage) ->
  ErrorMessage ++
  " Please refer to the AWS documentation for details on how to configure the instance metadata service: "
  "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html.".


-spec parse_iso8601_timestamp(Timestamp :: string() | binary()) -> calendar:datetime().
%% @doc Parse a ISO8601 timestamp, returning a datetime() value.
%% @end
parse_iso8601_timestamp(Timestamp) when is_binary(Timestamp) ->
  parse_iso8601_timestamp(binary_to_list(Timestamp));
parse_iso8601_timestamp(Timestamp) ->
  [Date, Time] = string:tokens(Timestamp, "T"),
  [Year, Month, Day] = string:tokens(Date, "-"),
  [Hour, Minute, Second] = string:tokens(Time, ":"),
  {{list_to_integer(Year), list_to_integer(Month), list_to_integer(Day)},
   {list_to_integer(Hour), list_to_integer(Minute), list_to_integer(string:left(Second,2))}}.


-spec profile() -> string().
%% @doc Return the value of the AWS_DEFAULT_PROFILE environment variable or the
%%      "default" profile.
%% @end
profile() -> profile(os:getenv("AWS_DEFAULT_PROFILE")).


-spec profile(false | string()) -> string().
%% @doc Process the value passed in to determine if we will return the default
%%      profile or the value from the environment variable.
%% @end
profile(false) -> ?DEFAULT_PROFILE;
profile(Value) -> Value.


-spec read_file(string()) -> list() | {error, Reason :: atom()}.
%% @doc Read the specified file, returning the contents as a list of strings.
%% @end
read_file(Path) ->
  read_from_file(file:open(Path, [read])).


-spec read_from_file({ok, file:fd()} | {error, Reason :: atom()})
  -> list() | {error, Reason :: atom()}.
%% @doc Read the specified file, returning the contents as a list of strings.
%% @end
read_from_file({ok, Fd}) ->
  read_file(Fd, []);
read_from_file({error, Reason}) ->
  {error, Reason}.


-spec read_file(Fd :: file:io_device(), Lines :: list())
  -> list() | {error, Reason :: atom()}.
%% @doc Read from the open file, accumulating the lines in a list.
%% @end
read_file(Fd, Lines) ->
  case file:read_line(Fd) of
    {ok, Value} ->
      Line = string:strip(Value, right, $\n),
      read_file(Fd, lists:append(Lines, [list_to_binary(Line)]));
    eof -> {ok, Lines};
    {error, Reason} -> {error, Reason}
  end.


-spec region_from_availability_zone(Value :: string()) -> string().
%% @doc Strip the availability zone suffix from the region.
%% @end
region_from_availability_zone(Value) ->
  string:sub_string(Value, 1, length(Value) - 1).


-spec load_imdsv2_token() -> security_token().
%% @doc Attempt to obtain EC2 IMDSv2 token.
%% @end
load_imdsv2_token() ->
  TokenUrl = imdsv2_token_url(),
  rabbit_log:info("Attempting to obtain EC2 IMDSv2 token from ~p ...", [TokenUrl]),
  case httpc:request(put, {TokenUrl, [{?METADATA_TOKEN_TTL_HEADER, integer_to_list(?METADATA_TOKEN_TTL_SECONDS)}]},
    [{timeout, ?DEFAULT_HTTP_TIMEOUT}], []) of
    {ok, {{_, 200, _}, _, Value}} ->
      rabbit_log:debug("Successfully obtained EC2 IMDSv2 token."),
      Value;
    {error, {{_, 400, _}, _, _}} ->
      rabbit_log:warning("Failed to obtain EC2 IMDSv2 token: Missing or Invalid Parameters – The PUT request is not valid."),
      undefined;
    Other ->
      rabbit_log:warning(
        get_instruction_on_instance_metadata_error("Failed to obtain EC2 IMDSv2 token: ~p. "
        "Falling back to EC2 IMDSv1 for now. It is recommended to use EC2 IMDSv2."), [Other]),
      undefined
  end.


-spec instance_metadata_request_headers() -> headers().
%% @doc Return headers used for instance metadata service requests.
%% @end
instance_metadata_request_headers() ->
  case application:get_env(rabbit, aws_prefer_imdsv2) of
    {ok, false} -> [];
    _           -> %% undefined or {ok, true}
                   rabbit_log:debug("EC2 Instance Metadata Service v2 (IMDSv2) is preferred."),
                   maybe_imdsv2_token_headers()
  end.

-spec maybe_imdsv2_token_headers() -> headers().
%% @doc Construct http request headers from Imdsv2Token to use with GET requests submitted to the EC2 Instance Metadata Service.
%% @end
maybe_imdsv2_token_headers() ->
  case rabbitmq_aws:ensure_imdsv2_token_valid() of
    undefined -> [];
    Value     -> [{?METADATA_TOKEN, Value}]
  end.
