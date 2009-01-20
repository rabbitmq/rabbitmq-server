-module(rabbit_capability).


-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-compile(export_all).

-record('delegate.create', {capability,
                            command, content}).
-record('delegate.create_ok', {forwarding_facet, revoking_facet}).
-record('delegate.revoke', {capability}).
-record('delegate.revoke_ok', {}).

%% This is an experimental hack for the fact that the exchange.bind_ok and
%% queue.bind_ok are empty commands - all it does is to carry a securely
%% generated capability
-record('secure.ok', {capability}).

-record(state, {caps = dict:new()}).

%%    This is a test case to for creating and revoking forwarding capabilites,
%%    which follows the following steps:
%%
%%    1. There is a root capability to create exchanges;
%%    2. Root creates a delegate to this functionality and gives the forwarding
%%       facet to Alice;
%%    3. Alice now has the capability C to a delegate that can execute the
%%       exchange.declare command. To declare an exchange, Alice does the following:
%%        * Sends an exchange.declare command as she would in a world without
%%        * capabilities with the exception that she adds the capability C as an
%%        * argument to the command;
%%           * The channel detects the presence of the capability argument,
%%           * resolves the delegate function and executes it with the
%%           * exchange.declare command from Alice in situ;
%%           * The result is returned to Alice; 
%%    4. If Alice wants to delegate the ability to create exchanges to Bob, she
%%       can either:
%%           * Create a delegate that forwards to the delegate for which Alice
%%           * has the capability C;
%%           * Just give Bob the capability C;

exchange_declare_test() ->
    %% Create the root state
    RootState = root_state(),
    %% Assert that root can create an exchange
    RootExchangeDeclare = #'exchange.declare'{arguments = [exchange_root]},
    {#'secure.ok'{}, State0}
        = run_command(RootExchangeDeclare, RootState),
    %% Create a delegate to create exchanges
    {#'delegate.create_ok'{forwarding_facet = AlicesForward,
                           revoking_facet   = RootsRevoke}, State1}
        = run_command(#'delegate.create'{capability = delegate_create_root,
                                         command    = RootExchangeDeclare},
                      State0),
    %% Use the forwarding facet to create an exchange
    AlicesExchangeDeclare = #'exchange.declare'{arguments = [AlicesForward]},
    {#'secure.ok'{}, State2}
        = run_command(AlicesExchangeDeclare, State1),
    %% Use the revoking facet to revoke the capability to create exchanges
    RevocationByRoot = #'delegate.revoke'{capability = RootsRevoke},
    {#'delegate.revoke_ok'{}, State3}
        = run_command(RevocationByRoot, State2),
    %% Assert the forwarding facet no longer works
    {access_denied, State4}
        = run_command(AlicesExchangeDeclare, State3),

    %% -------------------------------------------------------------------
    %% Create a second delegate that forwards to the first

    {#'delegate.create_ok'{forwarding_facet = BobsForward,
                           revoking_facet   = AlicesRevoke}, State5}
        = run_command(#'delegate.create'{capability = delegate_create_root,
                                         command    = AlicesExchangeDeclare},
                      State1),
    %% Use the delegated forwarding facet to create an exchange
    BobsExchangeDeclare = #'exchange.declare'{arguments = [BobsForward]},
    {#'secure.ok'{}, State6}
        = run_command(BobsExchangeDeclare, State5),
    %% Use the original revoking facet to revoke the capability to create
    %% exchanges in a cascading fashion
    {#'delegate.revoke_ok'{}, State7}
        = run_command(RevocationByRoot, State6),
    %% Assert the delegated forwarding facet no longer works
    {access_denied, State8}
        = run_command(BobsExchangeDeclare, State7),

    ok.
    
bind_test() ->
    %% Create the root state
    RootState = root_state(),
    %% Assert that root can issue bind and publish commands
    RootsBind = #'queue.bind'{arguments = [bind_root]},
    {#'secure.ok'{}, State0}
        = run_command(RootsBind, RootState),
    RootsPublish = #'basic.publish'{},
    Cont = #content{class_id = 60, %% Hardcoded :-)
                    properties = #'P_basic'{headers = [publish_root]},
                    properties_bin = none,
                    %% Define as undefined to make a distinction
                    payload_fragments_rev = undefined},
    {noreply, State0} = run_command(RootsPublish, Cont, RootState),

    %% Create a delegate to create exchanges
    RootExchangeDeclare = #'exchange.declare'{arguments = [exchange_root]},
    {#'delegate.create_ok'{forwarding_facet = AlicesExDecForward,
                           revoking_facet   = RootsExDecRevoke}, State1}
        = run_command(#'delegate.create'{capability = delegate_create_root,
                                         command    = RootExchangeDeclare},
                      State0),
    %% Use the forwarding facet to create an exchange
    AlicesExDec = #'exchange.declare'{arguments = [AlicesExDecForward]},
    {#'secure.ok'{capability = AlicesExCap}, State2}
        = run_command(AlicesExDec, State1),

    %% The important observation here is the Alice now has the capability to
    %% whatever she wants with the exchange - so let's see her do something
    %% useful with it
    
    %% Create a delegate to issue bind commands
    {#'delegate.create_ok'{forwarding_facet = AlicesBindForward,
                           revoking_facet   = RootsBindRevoke}, State3}
        = run_command(#'delegate.create'{capability = delegate_create_root,
                                         command    = RootsBind},
                      State2),
    
    %% Use the forwarding facet to bind something
    AlicesBind = #'queue.bind'{arguments = [AlicesBindForward]},
    {#'secure.ok'{capability = AlicesBindCap}, State4}
                          = run_command(AlicesBind, State3),

    %% This is where it gets tricky - to be able to bind to an exchange,
    %% Alice not only needs the capability to bind, but she also requires
    %% the capability to the exchange object that she is binding to........

    %% The bind command is a join between an exchange and a queue
    BobsBindDelegate = #'queue.bind'{queue         = undefined,
                                     routing_key   = undefined,
                             %% undefined will be filled in by the compiler
                             %% just making the destinction between trusted
                             %% and untrusted clear
                                     exchange  = AlicesExCap,
                                     arguments = [AlicesBindForward]},
    {#'delegate.create_ok'{forwarding_facet = BobsBindForward,
                           revoking_facet   = AlicesBindRevoke}, State5}
        = run_command(#'delegate.create'{capability = delegate_create_root,
                                         command    = BobsBindDelegate},
                      State4),
    
    BobsBind = #'queue.bind'{queue = <<"untrusted">>,
                             routing_key = <<"also untrusted">>,
                             arguments = [BobsBindForward]},
    {#'secure.ok'{capability = BobsBindCap}, State6}
                             = run_command(BobsBindDelegate, State5),

    %% Create a delegate to issue publish commands
    {#'delegate.create_ok'{forwarding_facet = AlicesPublishForward,
                           revoking_facet   = RootsPublishRevoke}, State7}
     = run_command(#'delegate.create'{capability = delegate_create_root,
                                      command    = RootsPublish},
                   State6),

    %% Create a delegate to give to Carol so that she can send messages
    ContentDelegate
      = #content{properties = #'P_basic'{headers = [AlicesPublishForward]}},
    {#'delegate.create_ok'{forwarding_facet = CarolsPublishForward,
                           revoking_facet   = AlicesPublishRevoke}, State8}
       = run_command(#'delegate.create'{capability = delegate_create_root,
                                        command    = #'basic.publish'{}
                                        },
                     ContentDelegate,
                     State7),

    ok.
    
%% ---------------------------------------------------------------------------
%% These functions intercept the AMQP command set - basically this is a typed
%% wrapper around the underlying execute_delegate/3 function
%% ---------------------------------------------------------------------------

run_command(Command = #'exchange.declare'{arguments = [Cap|_]}, State) ->
    execute_delegate(Command, Cap, State);

run_command(Command = #'queue.bind'{arguments = [Cap|_]}, State) ->
    execute_delegate(Command, Cap, State);

run_command(Command = #'delegate.create'{capability = Cap}, State) ->
    execute_delegate(Command, Cap, State);

run_command(Command = #'delegate.revoke'{capability = Cap}, State) ->
    execute_delegate(Command, Cap, State).

run_command(Command = #'delegate.create'{capability = Cap},
            Content, State) ->
    execute_delegate(Command, Content, Cap, State);

run_command(Command = #'basic.publish'{},
            Content = #content{properties = #'P_basic'{headers = [Cap|_]}},
            State) ->
    execute_delegate(Command, Content, Cap, State).

%% ---------------------------------------------------------------------------
%% Internal plumbing
%% ---------------------------------------------------------------------------
execute_delegate(Command, Cap, State) ->
    case resolve_capability(Cap, State) of
        {ok, Fun} -> Fun(Command, State);
        error     -> {access_denied, State}
    end.

execute_delegate(Command, Content, Cap, State) ->
    case resolve_capability(Cap, State) of
        {ok, Fun} -> Fun(Command, Content, State);
        error     -> {access_denied, State}
    end.

resolve_capability(Capability, #state{caps = Caps}) ->
    dict:find(Capability, Caps).

add_capability(Capability, Delegate, State = #state{caps = Caps}) ->
    State#state{ caps = dict:store(Capability, Delegate, Caps) }.

remove_capability(Capability, State = #state{caps = Caps}) ->
    State#state{ caps = dict:erase(Capability, Caps) }.

uuid() ->
    {A, B, C} = now(),
    <<A:32,B:32,C:32>>.

%% ---------------------------------------------------------------------------
%% This is how the chains of delegation are rooted - essentially this is known
%% set of root capabilities that the super user would have to configure the
%% system with
%% ---------------------------------------------------------------------------

root_state() ->
    State0 = #state{},
    %% The root capability to create exchanges
    State1 = add_capability(exchange_root,
                            fun(Command = #'exchange.declare'{}, State) ->
                                handle_method(Command, State)
                            end, State0),
    %% The root capability to create delegates
    State2 = add_capability(delegate_create_root,
                            fun(Command = #'delegate.create'{}, State) ->
                                handle_method(Command, State)
                            end, State1),
    %% The root capability to bind queues to exchanges
    State3 = add_capability(bind_root,
                            fun(Command = #'queue.bind'{}, State) ->
                                handle_method(Command, State)
                            end, State2),
    %% The root capability to create publish messages
    State4 = add_capability(publish_root,
                            fun(Command = #'basic.publish'{},
                                Content, State) ->
                                handle_method(Command, Content, State)
                            end, State3),
    State4.


%% ---------------------------------------------------------------------------
%% The internal API, which has *little* knowledge of capabilities.
%% This is roughly analogous the current channel API in Rabbit.
%% ---------------------------------------------------------------------------

handle_method(Delegate = #'delegate.create'{}, State) ->
    handle_method(Delegate, none, State);

handle_method(Command = #'exchange.declare'{}, State) ->
    Cap = uuid(),
    %% TODO Do something with this
    {#'secure.ok'{capability = Cap}, State};

handle_method(Command = #'queue.bind'{queue = Q, 
                                      exchange = X, 
                                      routing_key = K}, State) ->
    Cap = uuid(),
    %% TODO Do something with this
    {#'secure.ok'{capability = Cap}, State}.

handle_method(#'delegate.create'{capability = Cap,
                                 command    = Command}, Content, State) ->
    true = is_valid(Command),

    ForwardCapability = uuid(),
    RevokeCapability = uuid(),

    %% If the command types do not match up, then throw an error
    Check = fun(X) ->
                if
                    element(1, X) =:= element(1, Command) -> ok;
                    true -> exit(command_mismatch)
                end
            end,

    ForwardingFacet
        = case Content of
              none -> fun(_Command, _State) ->
                           Check(_Command),
                           run_command(Command, _State)
                      end;
              _    ->
                      fun(_Command, _Content, _State) ->
                           Check(_Command),
                           run_command(Command, _Content, _State)
                      end
          end,

    RevokingFacet = fun(_Command, _State) ->
                        NewState = remove_capability(ForwardCapability,
                                                     _State),
                        {#'delegate.revoke_ok'{}, NewState}
                    end,

    NewState  = add_capability(ForwardCapability, ForwardingFacet, State),
    NewState2 = add_capability(RevokeCapability, RevokingFacet, NewState),
    {#'delegate.create_ok'{forwarding_facet = ForwardCapability,
                           revoking_facet   = RevokeCapability}, NewState2};

handle_method(Command = #'basic.publish'{}, Content, State) ->
    {noreply, State}.


is_valid(_Command) ->
    true.

