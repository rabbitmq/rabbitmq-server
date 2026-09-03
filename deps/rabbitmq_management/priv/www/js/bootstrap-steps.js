// Bootstrap pipeline for the login -> application-initialisation sequence.
//
// A step is an object with a required run(ctx) and an optional
// rollback(ctx). Steps are executed in registration order across two
// phases:
//
//   login gates  may veto with a user-facing reason: returning
//                {ok: false, error: "..."} stops the pipeline.
//   init steps   build the application. They are not expected to veto;
//                a failure here is a bug or a server problem and
//                surfaces as a thrown exception.
//
// Both phases share one "completed" stack, so a failure in an init step
// also unwinds the login gates that already ran (e.g. a session created
// by a gate is deleted when extension loading later fails).
//
// Unwinding calls rollback() for completed steps only, in reverse order.
// A step that vetoes is NOT rolled back: the contract is that a step
// which rejects leaves nothing behind.

var loginGateRegistry = new Map();
var initStepRegistry = new Map();

// Steps completed by the last successful bootstrap(), so that logout can
// unwind the same set through unwind_active_steps().
var activeCompletedSteps = [];

function register_step(registry, kind, name, step) {
    if (name == null || (typeof name === 'string' && name.trim() === '')) return false;
    if (step == null || typeof step.run !== 'function') return false;
    if (step.rollback != null && typeof step.rollback !== 'function') return false;
    if (registry.has(name)) return false;
    registry.set(name, {name: name, kind: kind, run: step.run, rollback: step.rollback});
    return true;
}

function registerLoginGate(name, step) {
    return register_step(loginGateRegistry, 'LoginGate', name, step);
}

function unregisterLoginGate(name) {
    return loginGateRegistry.delete(name);
}

function registerInitStep(name, step) {
    return register_step(initStepRegistry, 'InitStep', name, step);
}

function unregisterInitStep(name) {
    return initStepRegistry.delete(name);
}

function clear_bootstrap_steps() {
    loginGateRegistry.clear();
    initStepRegistry.clear();
    activeCompletedSteps = [];
}

// Runs one phase, appending each successful step to `completed`.
// Returns {ok: true} or {ok: false, error: "..."} with error always
// populated, so callers never need their own fallback message.
function run_phase(registry, ctx, completed, defaultError) {
    for (const [name, step] of registry) {
        console.debug(`Running ${step.kind} ${name}`);
        var res;
        try {
            res = step.run(ctx);
        } catch (err) {
            console.error(`${step.kind} ${name} failed due to ${err}`);
            return {ok: false, error: `${step.kind} ${name} failed due to exception`};
        }
        if (res && res.ok === false) {
            return {ok: false,
                    error: res.error || defaultError || `${step.kind} ${name} rejected the request`};
        }
        completed.push(step);
    }
    return {ok: true};
}

function unwind(completed, ctx) {
    for (const step of completed.slice().reverse()) {
        if (typeof step.rollback !== 'function') continue;
        console.debug(`Rolling back ${step.kind} ${step.name}`);
        try {
            step.rollback(ctx);
        } catch (err) {
            console.error(`Rollback of ${step.kind} ${step.name} failed due to ${err}`);
        }
    }
}

function bootstrap(ctx, defaultError) {
    var completed = [];

    var gates = run_phase(loginGateRegistry, ctx, completed, defaultError);
    if (gates.ok === false) {
        unwind(completed, ctx);
        return gates;
    }

    var init = run_phase(initStepRegistry, ctx, completed, defaultError);
    if (init.ok === false) {
        unwind(completed, ctx);
        return init;
    }

    activeCompletedSteps = completed;
    return {ok: true, completed: completed};
}

// Unwinds whatever the last successful bootstrap() completed. Used by
// logout, which needs the same teardown as a failed bootstrap.
function unwind_active_steps(ctx) {
    var completed = activeCompletedSteps;
    activeCompletedSteps = [];
    unwind(completed, ctx);
}

export {
    registerLoginGate,
    unregisterLoginGate,
    registerInitStep,
    unregisterInitStep,
    clear_bootstrap_steps,
    bootstrap,
    unwind_active_steps
};

if (typeof window !== 'undefined') {
    Object.assign(window, {
        registerLoginGate,
        unregisterLoginGate,
        registerInitStep,
        unregisterInitStep,
        clear_bootstrap_steps,
        bootstrap,
        unwind_active_steps
    });
}
