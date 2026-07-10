# Shared Makefile fragment for management UI plugins that ship EJS templates.
#
# Include this in a plugin's DEP_PLUGINS to get the compile-ejs-templates
# target, which precompiles all *.ejs files in priv/www/js/tmpl/ into a
# single JavaScript file served as part of the management UI.
#
# The output file name is derived from the plugin PROJECT name by stripping the
# rabbitmq_ prefix and appending -ejs.js (e.g. management-ejs.js, top-ejs.js).
# The -ejs.js suffix avoids collisions with each plugin's existing main UI
# JavaScript file (e.g. top.js in rabbitmq_top, tracing.js in rabbitmq_tracing).
#
# Usage example (in a plugin Makefile):
#   DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk \
#                 rabbitmq_management/mk/rabbitmq-management-plugin.mk
#
# ---------------------------------------------------------------------------
# Build workflow and developer guidance
# ---------------------------------------------------------------------------
#
# The generated *-ejs.js files are build artifacts and are gitignored.  They
# are regenerated automatically as a side effect of gmake app (via the
# `app:: $(EJS_OUTPUT)` hook below) so a fresh checkout followed by a regular
# build produces a working management UI with no manual steps.
#
# To regenerate manually after editing a template:
#
#   gmake compile-ejs-templates
#
# Node.js sanity check (optional but recommended for UI developers):
#
#   If the `node` binary is available the target also runs
#   scripts/verify_ejs_templates.js, which compiles every template with the
#   original ejs-1.0.min.js library inside a vm sandbox, renders both the
#   EJS-compiled and the Erlang-precompiled versions against a stub context,
#   and fails the build if any rendered output differs.
#
#   What the sanity check catches:
#     - Structural compilation differences (wrong escaping, missing tokens,
#       mis-matched EJS delimiters)
#     - JavaScript syntax errors in code blocks (the generated file fails to
#       parse, causing a non-zero exit)
#
#   What the sanity check does NOT catch:
#     - Calls to functions defined elsewhere in the UI (fmt_escape_html,
#       message_rates, paginate_ui, etc.); the stub context swallows them
#     - Runtime type errors (null dereference, wrong argument types)
#     - Missing HTML close tags
#     - Logic errors whose output depends on real data
#
#   Runtime errors of the above kind are caught by the Selenium test suite,
#   which exercises the UI against a live RabbitMQ node with real data.
#
# Summary by developer profile:
#
#   Erlang/backend developer (no Node.js):
#     gmake compile-ejs-templates  ->  escript runs, <plugin>-ejs.js regenerated,
#                                      no cross-check (sufficient for non-UI work)
#
#   UI developer (no Node.js):
#     Same as above; encouraged to install Node.js for the additional check.
#
#   UI developer (with Node.js):
#     gmake compile-ejs-templates  ->  escript runs + sanity check runs,
#                                      structural equivalence verified

# Use ?= so that rabbitmq_management (which provides these scripts) can override
# with relative paths before including erlang.mk, avoiding a circular
# self-reference through $(DEPS_DIR)/rabbitmq_management.
EJS_COMPILE ?= $(DEPS_DIR)/rabbitmq_management/scripts/precompile_ejs_templates
EJS_VERIFY  ?= $(DEPS_DIR)/rabbitmq_management/scripts/verify_ejs_templates.js
EJS_TMPL_DIR ?= priv/www/js/tmpl

# The output file name is derived from the plugin PROJECT name by stripping the
# rabbitmq_ prefix and appending -ejs.js:
#   rabbitmq_management        -> priv/www/js/management-ejs.js
#   rabbitmq_stream_management -> priv/www/js/stream_management-ejs.js
#   rabbitmq_top               -> priv/www/js/top-ejs.js
# The -ejs.js suffix avoids collisions with each plugin's existing main UI
# JavaScript file (e.g. top.js in rabbitmq_top, tracing.js in rabbitmq_tracing).
EJS_OUTPUT ?= priv/www/js/$(subst rabbitmq_,,$(PROJECT))-ejs.js

# File-based target: regenerate only when a .ejs template is newer than the
# output file, so incremental builds stay fast.
$(EJS_OUTPUT): $(wildcard $(EJS_TMPL_DIR)/*.ejs)
	$(EJS_COMPILE) $(EJS_TMPL_DIR) $(EJS_OUTPUT)
	@if command -v node > /dev/null 2>&1; then \
		echo "Running EJS sanity check..."; \
		node $(EJS_VERIFY) $(EJS_TMPL_DIR) $(EJS_OUTPUT) || (echo "EJS sanity check failed" && exit 1); \
	else \
		echo "node not found; skipping EJS sanity check"; \
	fi

.PHONY: compile-ejs-templates

# Convenience alias for manual invocation.
compile-ejs-templates: $(EJS_OUTPUT)

# Hook into the standard build so the generated file is always present and
# up to date when the Erlang app is built.
app:: $(EJS_OUTPUT)
