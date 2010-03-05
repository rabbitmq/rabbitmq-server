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
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.

%% @doc This module is responsible for maintaining a connection to an AMQP
%% broker and manages channels within the connection. This module is used to
%% open and close connections to the broker as well as creating new channels
%% within a connection. Each amqp_connection process maintains a mapping of
%% the channels that were created by that connection process. Each resulting
%% amqp_channel process is linked to the parent connection process.
-module(amqp_connection).

-include("amqp_client.hrl").

-export([open_channel/1, open_channel/2]).
-export([start_direct/0, start_direct/1, start_direct_link/0, start_direct_link/1]).
-export([start_network/0, start_network/1, start_network_link/0, start_network_link/1]).
-export([close/1, close/3]).
-export([info/2, info_keys/1, info_keys/0]).

-define(COMMON_INFO_KEYS,
        [server_properties, is_closing, amqp_params, num_channels]).

%%---------------------------------------------------------------------------
%% Type Definitions
%%---------------------------------------------------------------------------

%% @type amqp_params() = #amqp_params{}.
%% As defined in amqp_client.hrl. It contains the following fields:
%% <ul>
%% <li>username :: binary() - The name of a user registered with the broker, 
%%     defaults to &lt;&lt;guest"&gt;&gt;</li>
%% <li>password :: binary() - The user's password, defaults to 
%%     &lt;&lt;"guest"&gt;&gt;</li>
%% <li>virtual_host :: binary() - The name of a virtual host in the broker,
%%     defaults to &lt;&lt;"/"&gt;&gt;</li>
%% <li>host :: string() - The hostname of the broker,
%%     defaults to "localhost"</li>
%% <li>port :: integer() - The port the broker is listening on,
%%     defaults to 5672</li>
%% </ul>

%%---------------------------------------------------------------------------
%% AMQP Connection API Methods
%%---------------------------------------------------------------------------

%% @spec () -> [Connection]
%% where
%%     Connection = pid()
%% @doc Starts a direct connection to a RabbitMQ server, assuming that
%% the server is running in the same process space, and with a default
%% set of amqp_params. If a different vhost or credential set is required,
%% start_direct/1 should be used.
start_direct() ->
    start_direct(#amqp_params{}).

%% @spec (amqp_params()) -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a direct connection to a RabbitMQ server, assuming that
%% the server is running in the same process space.
start_direct(Params) ->
    start_direct_internal(Params, false).

%% @spec () -> [Connection]
%% where
%%     Connection = pid()
%% @doc Starts a direct connection to a RabbitMQ server, assuming that
%% the server is running in the same process space, and with a default
%% set of amqp_params. If a different vhost or credential set is required,
%% start_direct_link/1 should be used. The resulting
%% process is linked to the invoking process.
start_direct_link() ->
    start_direct_link(#amqp_params{}).

%% @spec (amqp_params()) -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a direct connection to a RabbitMQ server, assuming that
%% the server is running in the same process space. The resulting process
%% is linked to the invoking process.
start_direct_link(Params) ->
    start_direct_internal(Params, true).

start_direct_internal(#amqp_params{} = Params, ProcLink) ->
    {ok, Pid} = start_internal(Params, amqp_direct_connection, ProcLink),
    Pid.

%% @spec () -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a networked conection to a remote AMQP server. Default
%% connection settings are used, meaning that the server is expected
%% to be at localhost:5672, with a vhost of "/" authorising a user
%% guest/guest.
start_network() ->
    start_network(#amqp_params{}).

%% @spec (amqp_params()) -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a networked conection to a remote AMQP server.
start_network(Params) ->
    start_network_internal(Params, false).

%% @spec () -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a networked conection to a remote AMQP server. Default
%% connection settings are used, meaning that the server is expected
%% to be at localhost:5672, with a vhost of "/" authorising a user
%% guest/guest. The resulting process is linked to the invoking process.
start_network_link() ->
    start_network_link(#amqp_params{}).

%% @spec (amqp_params()) -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a networked connection to a remote AMQP server. The resulting 
%% process is linked to the invoking process.
start_network_link(Params) ->
    start_network_internal(Params, true).

start_network_internal(#amqp_params{} = AmqpParams, ProcLink) ->
    {ok, Pid} = start_internal(AmqpParams, amqp_network_connection, ProcLink),
    Pid.

start_internal(Params, Module, _Link = true) when is_atom(Module) ->
    gen_server:start_link(Module, Params, []);
start_internal(Params, Module, _Link = false) when is_atom(Module) ->
    gen_server:start(Module, Params, []).

%%---------------------------------------------------------------------------
%% Commands
%%---------------------------------------------------------------------------

%% @doc Invokes open_channel(ConnectionPid, none, &lt;&lt;&gt;&gt;). 
%% Opens a channel without having to specify a channel number.
open_channel(ConnectionPid) ->
    open_channel(ConnectionPid, none).

%% @spec (ConnectionPid, ChannelNumber) -> ChannelPid
%% where
%%      ChannelNumber = integer()
%%      ConnectionPid = pid()
%%      ChannelPid = pid()
%% @doc Opens an AMQP channel.
%% This function assumes that an AMQP connection (networked or direct)
%% has already been successfully established.
open_channel(ConnectionPid, ChannelNumber) ->
    command(ConnectionPid, {open_channel, ChannelNumber}).

%% @spec (ConnectionPid) -> ok | Error
%% where
%%      ConnectionPid = pid()
%% @doc Closes the channel, invokes close(Channel, 200, &lt;&lt;"Goodbye">>).
close(ConnectionPid) ->
    close(ConnectionPid, 200, <<"Goodbye">>).

%% @spec (ConnectionPid, Code, Text) -> ok | closing
%% where
%%      ConnectionPid = pid()
%%      Code = integer()
%%      Text = binary()
%% @doc Closes the AMQP connection, allowing the caller to set the reply
%% code and text.
close(ConnectionPid, Code, Text) -> 
    Close = #'connection.close'{reply_text =  Text,
                                reply_code = Code,
                                class_id   = 0,
                                method_id  = 0},
    command(ConnectionPid, {close, Close}).

%%---------------------------------------------------------------------------
%% Other functions
%%---------------------------------------------------------------------------

%% @spec (ConnectionPid, Items) -> ResultList
%% where
%%      ConnectionPid = pid()
%%      Items = [Item]
%%      ResultList = [{Item, Result}]
%%      Item = atom()
%%      Result = term()
%% @doc Returns information about the connection, as specified by the Items
%% list. Item may be any atom returned by info_keys/1:
%%      server_properties - returns the server_properties fiels sent by the
%%          server while establishing the connection
%%      is_closing - returns true if the connection is in the process of closing
%%          and false otherwise
%%      amqp_params - returns the #amqp_params{} structure used to start the
%%          connection
%%      num_channels - returns the number of channels currently open under the
%%          connection (excluding channel 0)
%%      max_channel - returns the max_channel value negotiated with the server
%%          (only for the network connection)
%%      heartbeat - returns the heartbeat value negotiated with the server
%%          (only for the network connection)
%%      any other value - throws an exception
info(ConnectionPid, Items) ->
    gen_server:call(ConnectionPid, {info, Items}, infinity).

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
    gen_server:call(ConnectionPid, info_keys, infinity).

%% @spec () -> Items
%% where
%%      Items = [Item]
%%      Item = atom()
%% @doc Returns a list of atoms that can be used in conjunction with info/2.
%% These are general info keys, which can be used in any type of connection.
%% Other info keys may exist for a specific type. To get the full list of
%% atoms that can be used for a certain connection, use info_keys/1.
info_keys() ->
    ?COMMON_INFO_KEYS.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

command(ConnectionPid, Command) ->
    gen_server:call(ConnectionPid, {command, Command}, infinity).
