# Erlang-Memory-Allocators

Erlang VM memory utilisation from erts_alloc perspective

## Categories

* RabbitMQ

## README

Understand Erlang VM memory breakdown across all allocators & schedulers.

Metrics displayed:

* Resident Set Size - as captured by `rabbitmq_process_resident_memory_bytes`

* Allocated
  * Total
  * Used
  * Unused

* Allocated by Allocator Type (Min / Max / Avg / Current)
  * binary_alloc
  * driver_alloc
  * eheap_alloc
  * ets_alloc
  * exec_alloc
  * fix_alloc
  * literal_alloc
  * ll_alloc
  * sl_alloc
  * std_alloc
  * temp_alloc

For each allocator type:

* Multiblock
    * Used
        * Block
        * Carrier
    * Unused

* Multiblock Pool
    * Used
        * Block
        * Carrier
    * Unused

* Singleblock
    * Used
        * Block
        * Carrier
    * Unused

Filter by:

* RabbitMQ Cluster
* RabbitMQ Node
* Erlang Memory Allocator (Multi-value + All)

Depends on `rabbitmq-prometheus` plugin, built-in since [RabbitMQ v3.8.0](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v3.8.0)

Learn more about [RabbitMQ built-in Prometheus support](https://www.rabbitmq.com/prometheus.html)

To get it working locally with RabbitMQ in 3 simple steps, follow this [Quick Start guide](https://www.rabbitmq.com/prometheus.html#quick-start)
