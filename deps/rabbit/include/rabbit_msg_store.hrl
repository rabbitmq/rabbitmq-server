-record(msg_location, {msg_id, ref_count, file, offset, total_size}).

-record(client_msstate,
        { server,
          client_ref,
          reader,
          index_ets,
          dir,
          file_handles_ets,
          cur_file_cache_ets,
          flying_ets,
          credit_disc_bound,
          last_v1_file
        }).
