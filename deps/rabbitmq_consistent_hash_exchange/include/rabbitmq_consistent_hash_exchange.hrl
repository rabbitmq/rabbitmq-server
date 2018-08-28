-record(chx_hash_ring, {
  %% a resource
  exchange,
  %% a map of bucket => queue | exchange
  bucket_map,
  next_bucket_number
}).
