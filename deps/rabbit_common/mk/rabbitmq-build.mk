# Add Lager parse_transform module and our default Lager extra sinks.
LAGER_EXTRA_SINKS += rabbit_log \
		     rabbit_channel \
		     rabbit_connection \
		     rabbit_mirroring \
		     rabbit_queue \
		     rabbit_federation
lager_extra_sinks = $(subst $(space),$(comma),$(LAGER_EXTRA_SINKS))

RMQ_ERLC_OPTS += +'{parse_transform,lager_transform}' \
		 +'{lager_extra_sinks,[$(lager_extra_sinks)]}'

# Push our compilation options to both the normal and test ERLC_OPTS.
ERLC_OPTS += $(RMQ_ERLC_OPTS)
TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)
