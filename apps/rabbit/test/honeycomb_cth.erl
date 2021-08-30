-module(honeycomb_cth).

-export([id/1]).
-export([init/2]).

-export([pre_init_per_testcase/4]).
-export([post_end_per_testcase/5]).

-record(state, {directory, github_workflow, github_run_id,
                github_repository, github_sha, github_ref,
                base_rmq_ref, secondary_umbrella,
                erlang_version, elixir_version,
                otp_release, cpu_topology, schedulers,
                system_architecture, system_memory_data,
                start_times = #{}}).

id(Opts) ->
    proplists:get_value(directory, Opts, "/tmp/honeycomb").

init(Id, _Opts) ->
    application:ensure_all_started(os_mon),
    {ok, #state{directory = Id,
                github_workflow = os:getenv("GITHUB_WORKFLOW", "unknown"),
                github_run_id = os:getenv("GITHUB_RUN_ID", "unknown"),
                github_repository = os:getenv("GITHUB_REPOSITORY", "unknown"),
                github_sha = os:getenv("GITHUB_SHA", "unknown"),
                github_ref = os:getenv("GITHUB_REF", "unknown"),
                base_rmq_ref = os:getenv("BASE_RMQ_REF", "unknown"),
                secondary_umbrella = os:getenv("SECONDARY_UMBRELLA", "none"),
                erlang_version = os:getenv("ERLANG_VERSION", "unknown"),
                elixir_version = os:getenv("ELIXIR_VERSION", "unknown"),
                otp_release = erlang:system_info(otp_release),
                cpu_topology = erlang:system_info(cpu_topology),
                schedulers = erlang:system_info(schedulers),
                system_architecture = erlang:system_info(system_architecture),
                system_memory_data = memsup:get_system_memory_data()}}.

pre_init_per_testcase(Suite, TC, Config, #state{start_times = StartTimes} = State) ->
    SuiteTimes = maps:get(Suite, StartTimes, #{}),
    {Config, State#state{start_times =
                             StartTimes#{Suite =>
                                             SuiteTimes#{TC => erlang:timestamp()}}}}.

post_end_per_testcase(Suite, TC, _Config, Return, #state{github_workflow = GithubWorkflow,
                                                         github_run_id = GithubRunId,
                                                         github_repository = GithubRepository,
                                                         github_sha = GithubSha,
                                                         github_ref = GithubRef,
                                                         base_rmq_ref = BaseRmqRef,
                                                         secondary_umbrella = SecondaryUmbrella,
                                                         erlang_version = ErlangVersion,
                                                         elixir_version = ElixirVersion,
                                                         otp_release = OtpRelease,
                                                         cpu_topology = CpuTopology,
                                                         schedulers = Schedulers,
                                                         system_architecture = SystemArchitecture,
                                                         system_memory_data = SystemMemoryData,
                                                         start_times = StartTimes} = State) ->
    EndTime = erlang:timestamp(),
    SuiteTimes = maps:get(Suite, StartTimes),
    {StartTime, SuiteTimes1} = maps:take(TC, SuiteTimes),
    DurationMicroseconds = timer:now_diff(EndTime, StartTime),

    File = filename(Suite, TC, State),
    ok = filelib:ensure_dir(File),
    {ok, F} = file:open(File, [write]),

    Json = jsx:encode([{<<"ci">>, <<"GitHub Actions">>},
                       {<<"github_workflow">>, list_to_binary(GithubWorkflow)},
                       {<<"github_run_id">>, list_to_binary(GithubRunId)},
                       {<<"github_repository">>, list_to_binary(GithubRepository)},
                       {<<"github_sha">>, list_to_binary(GithubSha)},
                       {<<"github_ref">>, list_to_binary(GithubRef)},
                       {<<"base_rmq_ref">>, list_to_binary(BaseRmqRef)},
                       {<<"secondary_umbrella">>, list_to_binary(SecondaryUmbrella)},
                       {<<"erlang_version">>, list_to_binary(ErlangVersion)},
                       {<<"elixir_version">>, list_to_binary(ElixirVersion)},
                       {<<"otp_release">>, list_to_binary(OtpRelease)},
                       {<<"cpu_topology">>, cpu_topology_json_term(CpuTopology)},
                       {<<"schedulers">>, Schedulers},
                       {<<"system_architecture">>, list_to_binary(SystemArchitecture)},
                       {<<"system_memory_data">>, memory_json_term(SystemMemoryData)},
                       {<<"suite">>, list_to_binary(atom_to_list(Suite))},
                       {<<"testcase">>, list_to_binary(atom_to_list(TC))},
                       {<<"duration_seconds">>, DurationMicroseconds / 1000000},
                       {<<"result">>, list_to_binary(io_lib:format("~p", [Return]))}]),

    file:write(F, Json),
    file:close(F),
    {Return, State#state{start_times = StartTimes#{Suite := SuiteTimes1}}}.

filename(Suite, TC, #state{directory = Dir}) ->
    filename:join(Dir,
                  integer_to_list(erlang:system_time())
                  ++ "_" ++ atom_to_list(Suite)
                  ++ "_" ++ atom_to_list(TC)
                  ++ ".json").

memory_json_term(SystemMemoryData) when is_list(SystemMemoryData) ->
    [{list_to_binary(atom_to_list(K)), V} || {K, V} <- SystemMemoryData].

cpu_topology_json_term([{processor, Cores}]) when is_list(Cores) ->
    [{<<"processor">>, [begin
                            [{<<"core">>, [{list_to_binary(atom_to_list(Kind)), Index}]}]
                        end || {core, {Kind, Index}} <- Cores]}].
