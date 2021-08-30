%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @type close_reason(Type) = {shutdown, amqp_reason(Type)}.
%% @type amqp_reason(Type) = {Type, Code, Text}
%%      Code = non_neg_integer()
%%      Text = binary().
%% @doc This module is responsible for maintaining a connection to an AMQP
%% broker and manages channels within the connection. This module is used to
%% open and close connections to the broker as well as creating new channels
%% within a connection.<br/>
%% The connections and channels created by this module are supervised under
%% amqp_client's supervision tree. Please note that connections and channels
%% do not get restarted automatically by the supervision tree in the case of a
%% failure. If you need robust connections and channels, we recommend you use
%% Erlang monitors on the returned connection and channel PIDs.<br/>
%% <br/>
%% In case of a failure or an AMQP error, the connection process exits with a
%% meaningful exit reason:<br/>
%% <br/>
%% <table>
%%   <tr>
%%     <td><strong>Cause</strong></td>
%%     <td><strong>Exit reason</strong></td>
%%   </tr>
%%   <tr>
%%     <td>Any reason, where Code would have been 200 otherwise</td>
%%     <td>```normal'''</td>
%%   </tr>
%%   <tr>
%%     <td>User application calls amqp_connection:close/3</td>
%%     <td>```close_reason(app_initiated_close)'''</td>
%%   </tr>
%%   <tr>
%%     <td>Server closes connection (hard error)</td>
%%     <td>```close_reason(server_initiated_close)'''</td>
%%   </tr>
%%   <tr>
%%     <td>Server misbehaved (did not follow protocol)</td>
%%     <td>```close_reason(server_misbehaved)'''</td>
%%   </tr>
%%   <tr>
%%     <td>AMQP client internal error - usually caused by a channel exiting
%%         with an unusual reason. This is usually accompanied by a more
%%         detailed error log from the channel</td>
%%     <td>```close_reason(internal_error)'''</td>
%%   </tr>
%%   <tr>
%%     <td>Other error</td>
%%     <td>(various error reasons, causing more detailed logging)</td>
%%   </tr>
%% </table>
%% <br/>
%% See type definitions below.
-module(amqp_connection).

-include("amqp_client_internal.hrl").

-export([open_channel/1, open_channel/2, open_channel/3, register_blocked_handler/2]).
-export([start/1, start/2, close/1, close/2, close/3, close/4]).
-export([error_atom/1]).
-export([info/2, info_keys/1, info_keys/0]).
-export([connection_name/1, update_secret/3]).
-export([socket_adapter_info/2]).

-define(DEFAULT_CONSUMER, {amqp_selective_consumer, []}).

-define(PROTOCOL_SSL_PORT, (?PROTOCOL_PORT - 1)).

%%---------------------------------------------------------------------------
%% Type Definitions
%%---------------------------------------------------------------------------

%% @type amqp_adapter_info() = #amqp_adapter_info{}.
%% @type amqp_params_direct() = #amqp_params_direct{}.
%% As defined in amqp_client.hrl. It contains the following fields:
%% <ul>
%% <li>username :: binary() - The name of a user registered with the broker,
%%     defaults to &lt;&lt;guest"&gt;&gt;</li>
%% <li>password :: binary() - The password of user, defaults to 'none'</li>
%% <li>virtual_host :: binary() - The name of a virtual host in the broker,
%%     defaults to &lt;&lt;"/"&gt;&gt;</li>
%% <li>node :: atom() - The node the broker runs on (direct only)</li>
%% <li>adapter_info :: amqp_adapter_info() - Extra management information for if
%%     this connection represents a non-AMQP network connection.</li>
%% <li>client_properties :: [{binary(), atom(), binary()}] - A list of extra
%%     client properties to be sent to the server, defaults to []</li>
%% </ul>
%%
%% @type amqp_params_network() = #amqp_params_network{}.
%% As defined in amqp_client.hrl. It contains the following fields:
%% <ul>
%% <li>username :: binary() - The name of a user registered with the broker,
%%     defaults to &lt;&lt;guest"&gt;&gt;</li>
%% <li>password :: binary() - The user's password, defaults to
%%     &lt;&lt;"guest"&gt;&gt;</li>
%% <li>virtual_host :: binary() - The name of a virtual host in the broker,
%%     defaults to &lt;&lt;"/"&gt;&gt;</li>
%% <li>host :: string() - The hostname of the broker,
%%     defaults to "localhost" (network only)</li>
%% <li>port :: integer() - The port the broker is listening on,
%%     defaults to 5672 (network only)</li>
%% <li>channel_max :: non_neg_integer() - The channel_max handshake parameter,
%%     defaults to 0</li>
%% <li>frame_max :: non_neg_integer() - The frame_max handshake parameter,
%%     defaults to 0 (network only)</li>
%% <li>heartbeat :: non_neg_integer() - The heartbeat interval in seconds,
%%     defaults to 0 (turned off) (network only)</li>
%% <li>connection_timeout :: non_neg_integer() | 'infinity'
%%     - The connection timeout in milliseconds,
%%     defaults to 30000 (network only)</li>
%% <li>ssl_options :: term() - The second parameter to be used with the
%%     ssl:connect/2 function, defaults to 'none' (network only)</li>
%% <li>client_properties :: [{binary(), atom(), binary()}] - A list of extra
%%     client properties to be sent to the server, defaults to []</li>
%% <li>socket_options :: [any()] - Extra socket options.  These are
%%     appended to the default options.  See
%%     <a href="https://www.erlang.org/doc/man/inet.html#setopts-2">inet:setopts/2</a>
%%     and <a href="https://www.erlang.org/doc/man/gen_tcp.html#connect-4">
%%     gen_tcp:connect/4</a> for descriptions of the available options.</li>
%% </ul>


%%---------------------------------------------------------------------------
%% Starting a connection
%%---------------------------------------------------------------------------

%% @spec (Params) -> {ok, Connection} | {error, Error}
%% where
%%      Params = amqp_params_network() | amqp_params_direct()
%%      Connection = pid()
%% @doc same as {@link amqp_connection:start/2. start(Params, undefined)}
start(AmqpParams) ->
    start(AmqpParams, undefined).

%% @spec (Params, ConnectionName) -> {ok, Connection} | {error, Error}
%% where
%%      Params = amqp_params_network() | amqp_params_direct()
%%      ConnectionName = undefined | binary()
%%      Connection = pid()
%% @doc Starts a connection to an AMQP server. Use network params to
%% connect to a remote AMQP server or direct params for a direct
%% connection to a RabbitMQ server, assuming that the server is
%% running in the same process space.  If the port is set to 'undefined',
%% the default ports will be selected depending on whether this is a
%% normal or an SSL connection.
%% If ConnectionName is binary - it will be added to client_properties as
%% user specified connection name.
start(AmqpParams, ConnName) when ConnName == undefined; is_binary(ConnName) ->
    ensure_started(),
    AmqpParams0 =
        case AmqpParams of
            #amqp_params_direct{password = Password} ->
                AmqpParams#amqp_params_direct{password = credentials_obfuscation:encrypt(Password)};
            #amqp_params_network{password = Password} ->
                AmqpParams#amqp_params_network{password = credentials_obfuscation:encrypt(Password)}
        end,
    AmqpParams1 =
        case AmqpParams0 of
            #amqp_params_network{port = undefined, ssl_options = none} ->
                AmqpParams0#amqp_params_network{port = ?PROTOCOL_PORT};
            #amqp_params_network{port = undefined, ssl_options = _} ->
                AmqpParams0#amqp_params_network{port = ?PROTOCOL_SSL_PORT};
            _ ->
                AmqpParams0
        end,
    AmqpParams2 = set_connection_name(ConnName, AmqpParams1),
    AmqpParams3 = amqp_ssl:maybe_enhance_ssl_options(AmqpParams2),
    ok = ensure_safe_call_timeout(AmqpParams3, amqp_util:call_timeout()),
    {ok, _Sup, Connection} = amqp_sup:start_connection_sup(AmqpParams3),
    amqp_gen_connection:connect(Connection).

set_connection_name(undefined, Params) -> Params;
set_connection_name(ConnName,
                    #amqp_params_network{client_properties = Props} = Params) ->
    Params#amqp_params_network{
        client_properties = [
            {<<"connection_name">>, longstr, ConnName} | Props
        ]};
set_connection_name(ConnName,
                    #amqp_params_direct{client_properties = Props} = Params) ->
    Params#amqp_params_direct{
        client_properties = [
            {<<"connection_name">>, longstr, ConnName} | Props
        ]}.

%% Usually the amqp_client application will already be running. We
%% check whether that is the case by invoking an undocumented function
%% which does not require a synchronous call to the application
%% controller. That way we don't risk a dead-lock if, say, the
%% application controller is in the process of shutting down the very
%% application which is making this call.
ensure_started() ->
    [ensure_started(App) || App <- [syntax_tools, compiler, xmerl,
                                    rabbit_common, amqp_client, credentials_obfuscation]].

ensure_started(App) ->
    case is_pid(application_controller:get_master(App)) andalso amqp_sup:is_ready() of
        true  -> ok;
        false -> case application:ensure_all_started(App) of
                     {ok, _}        -> ok;
                     {error, _} = E -> throw(E)
                 end
    end.

%%---------------------------------------------------------------------------
%% Commands
%%---------------------------------------------------------------------------

%% @doc Invokes open_channel(ConnectionPid, none,
%% {amqp_selective_consumer, []}).  Opens a channel without having to
%% specify a channel number. This uses the default consumer
%% implementation.
open_channel(ConnectionPid) ->
    open_channel(ConnectionPid, none, ?DEFAULT_CONSUMER).

%% @doc Invokes open_channel(ConnectionPid, none, Consumer).
%% Opens a channel without having to specify a channel number.
open_channel(ConnectionPid, {_, _} = Consumer) ->
    open_channel(ConnectionPid, none, Consumer);

%% @doc Invokes open_channel(ConnectionPid, ChannelNumber,
%% {amqp_selective_consumer, []}).  Opens a channel, using the default
%% consumer implementation.
open_channel(ConnectionPid, ChannelNumber)
        when is_number(ChannelNumber) orelse ChannelNumber =:= none ->
    open_channel(ConnectionPid, ChannelNumber, ?DEFAULT_CONSUMER).

%% @spec (ConnectionPid, ChannelNumber, Consumer) -> Result
%% where
%%      ConnectionPid = pid()
%%      ChannelNumber = pos_integer() | 'none'
%%      Consumer = {ConsumerModule, ConsumerArgs}
%%      ConsumerModule = atom()
%%      ConsumerArgs = [any()]
%%      Result = {ok, ChannelPid} | {error, Error}
%%      ChannelPid = pid()
%% @doc Opens an AMQP channel.<br/>
%% Opens a channel, using a proposed channel number and a specific consumer
%% implementation.<br/>
%% ConsumerModule must implement the amqp_gen_consumer behaviour. ConsumerArgs
%% is passed as parameter to ConsumerModule:init/1.<br/>
%% This function assumes that an AMQP connection (networked or direct)
%% has already been successfully established.<br/>
%% ChannelNumber must be less than or equal to the negotiated
%% max_channel value, or less than or equal to ?MAX_CHANNEL_NUMBER
%% (65535) if the negotiated max_channel value is 0.<br/>
%% In the direct connection, max_channel is always 0.
open_channel(ConnectionPid, ChannelNumber,
             {_ConsumerModule, _ConsumerArgs} = Consumer) ->
    amqp_gen_connection:open_channel(ConnectionPid, ChannelNumber, Consumer).

%% @spec (ConnectionPid) -> ok | Error
%% where
%%      ConnectionPid = pid()
%% @doc Closes the channel, invokes
%% close(Channel, 200, &lt;&lt;"Goodbye"&gt;&gt;).
close(ConnectionPid) ->
    close(ConnectionPid, 200, <<"Goodbye">>).

%% @spec (ConnectionPid, Timeout) -> ok | Error
%% where
%%      ConnectionPid = pid()
%%      Timeout = integer()
%% @doc Closes the channel, using the supplied Timeout value.
close(ConnectionPid, Timeout) ->
    close(ConnectionPid, 200, <<"Goodbye">>, Timeout).

%% @spec (ConnectionPid, Code, Text) -> ok | closing
%% where
%%      ConnectionPid = pid()
%%      Code = integer()
%%      Text = binary()
%% @doc Closes the AMQP connection, allowing the caller to set the reply
%% code and text.
close(ConnectionPid, Code, Text) ->
    close(ConnectionPid, Code, Text, amqp_util:call_timeout()).

%% @spec (ConnectionPid, Code, Text, Timeout) -> ok | closing
%% where
%%      ConnectionPid = pid()
%%      Code = integer()
%%      Text = binary()
%%      Timeout = integer()
%% @doc Closes the AMQP connection, allowing the caller to set the reply
%% code and text, as well as a timeout for the operation, after which the
%% connection will be abruptly terminated.
close(ConnectionPid, Code, Text, Timeout) ->
    Close = #'connection.close'{reply_text = Text,
                                reply_code = Code,
                                class_id   = 0,
                                method_id  = 0},
    amqp_gen_connection:close(ConnectionPid, Close, Timeout).

register_blocked_handler(ConnectionPid, BlockHandler) ->
    amqp_gen_connection:register_blocked_handler(ConnectionPid, BlockHandler).

-spec update_secret(pid(), term(), binary()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

update_secret(ConnectionPid, NewSecret, Reason) ->
  Update = #'connection.update_secret'{new_secret = NewSecret,
                                       reason = Reason},
  amqp_gen_connection:update_secret(ConnectionPid, Update).

%%---------------------------------------------------------------------------
%% Other functions
%%---------------------------------------------------------------------------

%% @spec (Code) -> atom()
%% where
%%      Code = integer()
%% @doc Returns a descriptive atom corresponding to the given AMQP
%% error code.
error_atom(Code) -> ?PROTOCOL:amqp_exception(Code).

%% @spec (ConnectionPid, Items) -> ResultList
%% where
%%      ConnectionPid = pid()
%%      Items = [Item]
%%      ResultList = [{Item, Result}]
%%      Item = atom()
%%      Result = term()
%% @doc Returns information about the connection, as specified by the Items
%% list. Item may be any atom returned by info_keys/1:
%%<ul>
%%<li>type - returns the type of the connection (network or direct)</li>
%%<li>server_properties - returns the server_properties fields sent by the
%%    server while establishing the connection</li>
%%<li>is_closing - returns true if the connection is in the process of closing
%%    and false otherwise</li>
%%<li>amqp_params - returns the #amqp_params{} structure used to start the
%%    connection</li>
%%<li>num_channels - returns the number of channels currently open under the
%%    connection (excluding channel 0)</li>
%%<li>channel_max - returns the channel_max value negotiated with the
%%    server</li>
%%<li>heartbeat - returns the heartbeat value negotiated with the server
%%    (only for the network connection)</li>
%%<li>frame_max - returns the frame_max value negotiated with the
%%    server (only for the network connection)</li>
%%<li>sock - returns the socket for the network connection (for use with
%%    e.g. inet:sockname/1) (only for the network connection)</li>
%%<li>any other value - throws an exception</li>
%%</ul>
info(ConnectionPid, Items) ->
    amqp_gen_connection:info(ConnectionPid, Items).

%% @spec (ConnectionPid) -> Items
%% where
%%      ConnectionPid = pid()
%%      Items = [Item]
%%      Item = atom()
%% @doc Returns a list of atoms that can be used in conjunction with info/2.
%% Note that the list differs from a type of connection to another (network vs.
%% direct). Use info_keys/0 to get a list of info keys that can be used for
%% any connection.
info_keys(ConnectionPid) ->
    amqp_gen_connection:info_keys(ConnectionPid).

%% @spec () -> Items
%% where
%%      Items = [Item]
%%      Item = atom()
%% @doc Returns a list of atoms that can be used in conjunction with info/2.
%% These are general info keys, which can be used in any type of connection.
%% Other info keys may exist for a specific type. To get the full list of
%% atoms that can be used for a certain connection, use info_keys/1.
info_keys() ->
    amqp_gen_connection:info_keys().

%% @doc Takes a socket and a protocol, returns an #amqp_adapter_info{}
%% based on the socket for the protocol given.
socket_adapter_info(Sock, Protocol) ->
    amqp_direct_connection:socket_adapter_info(Sock, Protocol).

%% @spec (ConnectionPid) -> ConnectionName
%% where
%%      ConnectionPid = pid()
%%      ConnectionName = binary()
%% @doc Returns user specified connection name from client properties
connection_name(ConnectionPid) ->
    ClientProperties = case info(ConnectionPid, [amqp_params]) of
        [{_, #amqp_params_network{client_properties = Props}}] -> Props;
        [{_, #amqp_params_direct{client_properties = Props}}] -> Props
    end,
    case lists:keyfind(<<"connection_name">>, 1, ClientProperties) of
        {<<"connection_name">>, _, ConnName} -> ConnName;
        false                                -> undefined
    end.

ensure_safe_call_timeout(#amqp_params_network{connection_timeout = ConnTimeout}, CallTimeout) ->
    maybe_update_call_timeout(ConnTimeout, CallTimeout);
ensure_safe_call_timeout(#amqp_params_direct{}, CallTimeout) ->
    case net_kernel:get_net_ticktime() of
        NetTicktime when is_integer(NetTicktime) ->
            maybe_update_call_timeout(tick_or_direct_timeout(NetTicktime * 1000),
                CallTimeout);
        {ongoing_change_to, NetTicktime} ->
            maybe_update_call_timeout(tick_or_direct_timeout(NetTicktime * 1000),
                CallTimeout);
        ignored ->
            maybe_update_call_timeout(?DIRECT_OPERATION_TIMEOUT, CallTimeout)
    end.

maybe_update_call_timeout(BaseTimeout, CallTimeout)
  when is_integer(BaseTimeout), CallTimeout > BaseTimeout ->
    ok;
maybe_update_call_timeout(BaseTimeout, CallTimeout) ->
    EffectiveSafeCallTimeout = amqp_util:safe_call_timeout(BaseTimeout),
    ?LOG_WARN("AMQP 0-9-1 client call timeout was ~p ms, is updated to a safe effective "
              "value of ~p ms", [CallTimeout, EffectiveSafeCallTimeout]),
    amqp_util:update_call_timeout(EffectiveSafeCallTimeout),
    ok.

tick_or_direct_timeout(Timeout) when Timeout >= ?DIRECT_OPERATION_TIMEOUT -> Timeout;
tick_or_direct_timeout(_Timeout) -> ?DIRECT_OPERATION_TIMEOUT.
