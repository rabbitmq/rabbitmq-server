{application, mock_rabbitmq_plugin_for_3_9, [
	{description, "New project"},
	{vsn, "0.1.0"},
	{modules, ['mock_rabbitmq_plugins_01_app','mock_rabbitmq_plugins_01_sup']},
	{registered, [mock_rabbitmq_plugins_01_sup]},
	{applications, [kernel,stdlib,rabbit]},
	{mod, {mock_rabbitmq_plugins_01_app, []}},
	{env, []},
	{broker_version_requirements, ["3.8.0", "3.9.0", "3.10.0"]}
]}.
