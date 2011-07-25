-module(rabbit_amqp1_0_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

-export([listener_started/2, listener_stopped/2, start_client/1]).

start_link(Listeners) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Listeners]).

%% supervisor callback

init([Listeners]) ->
    ChildSpecs = [
                  {rabbit_amqp1_0_client_sup_sup,
                   {rabbit_client_sup, start_link,
                    [{local, rabbit_amqp1_0_client_sup_sup},
                     {rabbit_amqp1_0_client_sup, start_link, []}]},
                   transient,
                   infinity,
                   supervisor,
                   [rabbit_client_sup]} | make_listener_specs(Listeners)
                  ],
    {ok, {{one_for_all, 10, 10}, ChildSpecs}}.

make_listener_specs(Listeners) ->
    [make_listener_spec(Spec)
     || Spec <- lists:append([rabbit_networking:check_tcp_listener_address(
                                rabbit_amqp1_0_listener_sup, Listener)
                              || Listener <- Listeners])].

make_listener_spec({IPAddress, Port, Family, Name}) ->
    {Name,
     {tcp_listener_sup, start_link,
      [IPAddress, Port,
       [Family | tcp_opts()],
       {?MODULE, listener_started, []},
       {?MODULE, listener_stopped, []},
       {?MODULE, start_client, []}, "AMQP 1.0 Listener"]},
     transient, infinity, supervisor, [tcp_listener_sup]}.

listener_started(IPAddress, Port) ->
    rabbit_networking:tcp_listener_started('amqp 1.0', IPAddress, Port).

listener_stopped(IPAddress, Port) ->
    rabbit_networking:tcp_listener_stopped('amqp 1.0', IPAddress, Port).

start_client(Sock) ->
    {ok, SupPid, ReaderPid} =
        supervisor:start_child(rabbit_amqp1_0_client_sup_sup, []),
    ok = gen_tcp:controlling_process(Sock, ReaderPid),
    ReaderPid ! {go, Sock, fun (S) -> {ok, S} end},
    SupPid.

tcp_opts() ->
    {ok, Opts} = application:get_env(rabbitmq_amqp1_0, tcp_listen_options),
    Opts.
