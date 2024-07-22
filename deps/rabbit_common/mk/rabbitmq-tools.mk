# --------------------------------------------------------------------
# erlang.mk query-deps* formatting.
# --------------------------------------------------------------------

# We need to provide a repo mapping for deps resolved via git_rmq fetch method
query_repo_git_rmq = https://github.com/rabbitmq/$(call rmq_cmp_repo_name,$(1))
