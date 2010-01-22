-module(rabbit_capability).


-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-compile(export_all).

-record('delegate.create', {capability, command, content}).
-record('delegate.create_ok', {forwarding_facet, revoking_facet}).
-record('delegate.revoke', {capability}).
-record('delegate.revoke_ok', {}).

%% This is an experimental hack for the fact that the exchange.bind_ok and
%% queue.bind_ok are empty commands - all it does is to carry a securely
%% generated capability
-record('secure.ok', {capability}).

%% This is a new version of the basic.publish command that carries a
%% capability in the command -  NB you *could* put this into the message
%% arguments but it makes the usage cmplicated and ambiguous
-record('basic.publish2', {capability}).

-record(state, {caps = dict:new()}).

test() ->
    ok = exchange_declare_test(),
    ok = bogus_intent_test().

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

%%    This is a test case to for creating and forwarding capabilites on the
%%    same exchange entity. This demonstrates how different delegates
%%    encapsulate different intents in a way that is specified by the owner
%%    of the underlying entity:
%%
%%    1. There is a root capability to create exchanges and bindings
%%       as well as to publish messages;
%%    2. Root creates a delegate to these functionalities and gives 
%%       the forwarding facets to Alice;
%%    3. Alice creates an exchange that she would like to protect;
%%    4. Alice creates a delegate to allow Bob to bind queues to her exchange
%%       and a delegate to allow Carol to publish messages to her exchange
%%    5. After this has been verified, Bob and Carol try to be sneaky with
%%       the delegates they have been given. Each one of them tries to misuse
%%       the capability to perform a different action to the delegate they
%%       possess, i.e. Bob tries to send a message whilst Carol tries to bind
%%       a queue to the exchange - they both find out that their respective
%%       capabilities have been bound by intent :-)
%%    
bogus_intent_test() ->
    %% Create the root state
    RootState = root_state(),
    %% Assert that root can issue bind and publish commands
    RootsBind = #'queue.bind'{arguments = [bind_root]},
    {#'secure.ok'{}, State0}
        = run_command(RootsBind, RootState),
    RootsPublish = #'basic.publish2'{capability = publish_root},
    {noreply, State0} = run_command(RootsPublish, #content{}, RootState),

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
    CarolsPublishDelegate
        = #'basic.publish2'{capability = AlicesPublishForward},
        
    {#'delegate.create_ok'{forwarding_facet = CarolsPublishForward,
                           revoking_facet   = AlicesPublishRevoke}, State8}
       = run_command(#'delegate.create'{capability = delegate_create_root,
                                        command    = CarolsPublishDelegate},
                     State7),
    
    %% Then have Carol publish a message
    CarolsPublish = #'basic.publish2'{capability = CarolsPublishForward},
    {noreply, _} = run_command(CarolsPublish, #content{}, State8),
    
    %% Carol then tries to bind a queue to the exchange that she *knows* about
    CarolsBind = #'queue.bind'{queue = <<"untrusted">>,
                               routing_key = <<"also untrusted">>,
                               arguments = [CarolsPublishForward]},
    {access_denied, _} = run_command(CarolsBind, State8),
    
    %% Alternatively let Bob try to publish a message to
    %% the exchange that he *knows* about
    BobsPublish = #'basic.publish2'{capability = BobsBindForward},
    {access_denied, _} = run_command(BobsPublish, #content{}, State8),
    
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
    
run_command(Command = #'basic.publish2'{capability = Cap}, Content, State) ->
    execute_delegate(Command, Content, Cap, State).

%% ---------------------------------------------------------------------------
%% Internal plumbing
%% ---------------------------------------------------------------------------
execute_delegate(Command, Cap, State) ->
    case resolve_capability(Cap, State) of
        {ok, Fun} -> case catch Fun(Command, State) of
                        %% Put this in case an f/3 delegate is resolved
                        {'EXIT', _} -> {access_denied, State};
                        X           -> X
                     end;
        error     -> {access_denied, State}
    end.

execute_delegate(Command, Content, Cap, State) ->
    case resolve_capability(Cap, State) of
        {ok, Fun} -> case catch Fun(Command, Content, State) of
                        %% Put this in case an f/2 delegate is resolved
                        {'EXIT', _} -> {access_denied, State};
                        X           -> X
                     end;
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
                            fun(Command = #'basic.publish2'{},
                                Content, State) ->
                                handle_method(Command, Content, State)
                            end, State3),
    State4.


%% ---------------------------------------------------------------------------
%% The internal API, which has *little* knowledge of capabilities.
%% This is roughly analogous the current channel API in Rabbit.
%% ---------------------------------------------------------------------------

handle_method(#'delegate.create'{capability = Cap,
                                 command    = Command}, State) ->
    true = is_valid(Command),
    ForwardCapability = uuid(),
    RevokeCapability = uuid(),

    ForwardingFacet =
    case contains_content(Command) of 
        false ->
            fun(_Command, _State) ->
                %% If the command types do not match up, then throw an error
                if
                    element(1, _Command) =:= element(1, Command) ->
                        run_command(Command, _State);
                    true -> 
                        exit(command_mismatch)
                end
            end;
        %% This is copy and paste, could be better factored :-(
        true ->
            fun(_Command, _Content, _State) ->
                %% If the command types do not match up, then throw an error
                if
                    element(1, _Command) =:= element(1, Command) ->
                        run_command(Command, _Content, _State);
                    true -> 
                        exit(command_mismatch)
                end
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

handle_method(Command = #'exchange.declare'{}, State) ->
    Cap = uuid(), %% TODO Do something with this
    {#'secure.ok'{capability = Cap}, State};

handle_method(Command = #'queue.bind'{queue = Q, 
                                      exchange = X, 
                                      routing_key = K}, State) ->
    Cap = uuid(), %% TODO Do something with this
    {#'secure.ok'{capability = Cap}, State}.

handle_method(Command = #'basic.publish2'{}, Content, State) ->
    {noreply, State}.

contains_content(#'basic.publish2'{}) -> true;
contains_content(_) -> false.

is_valid(_Command) -> true.

