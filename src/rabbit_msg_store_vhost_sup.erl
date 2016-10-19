-module(rabbit_msg_store_vhost_sup).

-behaviour(supervisor2).

-export([start_link/3, init/1, add_vhost/2, delete_vhost/2,
         client_init/5, successfully_recovered_state/2]).

%% Internal
-export([start_vhost/4]).

start_link(Name, ClientRefs, StartupFunState) ->
    supervisor2:start_link({local, Name}, ?MODULE,
                           [Name, ClientRefs, StartupFunState]).

init([Name, ClientRefs, StartupFunState]) ->
    {ok, {{simple_one_for_one, 0, 1},
        [{rabbit_msg_store_vhost, {rabbit_msg_store_vhost_sup, start_vhost,
                                   [Name, ClientRefs, StartupFunState]},
           transient, infinity, supervisor, [rabbit_msg_store]}]}}.


add_vhost(Name, VHost) ->
    supervisor2:start_child(Name, [VHost]).

start_vhost(Name, ClientRefs, StartupFunState, VHost) ->
    VHostName = vhost_store_name(Name, VHost),
    VHostDir = vhost_store_dir(VHost),
    ok = rabbit_file:ensure_dir(VHostDir),
    rabbit_msg_store:start_link(VHostName, VHostDir,
                                ClientRefs, StartupFunState).

delete_vhost(Name, VHost) ->
    VHostName = vhost_store_name(Name, VHost),
    case whereis(VHostName) of
        undefined -> ok;
        Pid       -> supervisor2:terminate_child(Name, Pid)
    end,
    ok.

client_init(Server, Ref, MsgOnDiskFun, CloseFDsFun, VHost) ->
    VHostName = maybe_start_vhost(Server, VHost),
    rabbit_msg_store:client_init(VHostName, Ref, MsgOnDiskFun, CloseFDsFun).

maybe_start_vhost(Server, VHost) ->
    VHostName = vhost_store_name(Server, VHost),
    case whereis(VHostName) of
        undefined -> add_vhost(Server, VHost);
        _         -> ok
    end,
    VHostName.

vhost_store_name(Name, VHost) ->
    VhostEncoded = rabbit_vhost:dir(VHost),
    binary_to_atom(<<(atom_to_binary(Name, utf8))/binary, "_",
                     VhostEncoded/binary>>,
                   utf8).

vhost_store_dir(VHost) ->
    Dir = rabbit_mnesia:dir(),
    VhostEncoded = rabbit_vhost:dir(VHost),
    binary_to_list(filename:join([Dir, VhostEncoded])).

successfully_recovered_state(Name, VHost) ->
    VHostName = vhost_store_name(Name, VHost),
    rabbit_msg_store:successfully_recovered_state(VHostName).

% force_recovery
% transform_dir