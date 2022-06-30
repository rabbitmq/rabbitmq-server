-type bucket() :: non_neg_integer().

-record(chx_hash_ring, {
  exchange :: rabbit_exchange:name(),
  bucket_map :: #{bucket() => rabbit_types:binding_destination()},
  next_bucket_number :: bucket()
}).
