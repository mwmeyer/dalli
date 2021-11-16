# frozen_string_literal: true

module Dalli
  ##
  # This class handles operations on multiple keys at once, that may span multiple
  # servers in the set.
  ##
  class MultiKeyProcessor
    def initialize(ring, key_manager)
      @ring = ring
      @key_manager = key_manager
    end

    def process(keys, &block)
      return {} if keys.empty?

      @ring.lock do
        servers_with_keys = keys_to_servers(keys)
        return if servers_with_keys.empty?

        # TODO: How does this exit on a NetworkError
        process_response_in_chunks(setup_for_response(servers_with_keys), &block)
      end
    rescue NetworkError => e
      Dalli.logger.debug { e.inspect }
      Dalli.logger.debug { 'retrying multi yielder because of timeout' }
      retry
    end

    # TODO: Figure out if this is really the appropriate
    # value to use as a timeout for the whole of response
    # processing
    def timeout
      @ring.socket_timeout
    end

    # Map the keys to servers, and pass the multiget
    # requests to the servers.  Returns the list
    # of servers.
    def keys_to_servers(keys)
      groups = groups_for_keys(keys)
      if (unfound_keys = groups.delete(nil))
        Dalli.logger.debug do
          "unable to get keys for #{unfound_keys.length} keys "\
            'because no matching server was found'
        end
      end
      make_multiget_requests(groups)
      groups.keys
    end

    def make_multiget_requests(groups)
      groups.each do |server, keys_for_server|
        server.request(:send_multiget, keys_for_server)
      rescue DalliError, NetworkError => e
        # TODO: Is this error potentially fatal (because the failed
        # server is never removed from the list)?  Or is the
        # socket always marked nil on failure?
        Dalli.logger.debug { e.inspect }
        Dalli.logger.debug { "unable to get keys for server #{server.name}" }
      end
    end

    def setup_for_response(servers)
      deleted = []
      servers.each do |server|
        next unless server.alive?

        deleted.append(server) unless start_multi_for_server(server)
      end

      servers.delete_if { |server| deleted.include?(server) }
    rescue Dalli::NetworkError
      # If we get a NetworkError here (for example if the underlying socket has
      # timed out) then we abort the entirety of the response and
      # retry at the top level
      abort_multi_response(servers)
      raise
    end

    def start_multi_for_server(server)
      server.multi_response_start
      true
    rescue Dalli::DalliError => e
      # TODO: Determine if we can actually raise this error
      Dalli.logger.debug { e.inspect }
      Dalli.logger.debug { 'results from this server will be missing' }
      false
    end

    # Swallows Dalli::NetworkError
    def abort_multi_response(servers)
      servers.each(&:multi_response_abort)
    end

    def process_response_in_chunks(remaining_servers, &block)
      start_time = Time.now
      loop do
        # Remove any dead servers
        # TODO: Is this well behaved in a multi-threaded environment?
        # I think it's ok given the ring.lock, but it's not really
        # ideal from an encapsulation perspective
        remaining_servers.delete_if { |s| s.sock.nil? }
        break if remaining_servers.empty?

        # Read responses from all servers in the pool and return
        # the list of servers whose responses aren't complete
        remaining_servers = read_available_server_responses(remaining_servers, start_time, &block)
      end
    end

    def read_available_server_responses(servers, start_time, &block)
      readable_servers = servers_with_data(servers, start_time)
      if readable_servers.empty?
        # All remaining servers timed out while reading
        # results
        #
        # In this case we simply return the <key, value> results
        # we have already retrieved to the caller, and
        # the set may be incomplete.
        abort_multi_connections_w_timeout(servers)
        return []
      end

      readable_servers.each { |s| servers.delete(s) if done_after_process_results?(s, &block) }
      servers
    rescue NetworkError
      abort_multi_response(servers)
      raise
    end

    def remaining_time(start)
      elapsed = Time.now - start
      return 0 if elapsed > timeout

      timeout - elapsed
    end

    # Swallows Dalli::NetworkError
    def abort_multi_connections_w_timeout(servers)
      abort_multi_response(servers)
      servers.each do |server|
        Dalli.logger.debug { "memcached at #{server.name} did not response within timeout" }
      end

      true # Required to simplify caller
    end

    def done_after_process_results?(server)
      server.multi_response_nonblock.each_pair do |key, value_list|
        yield @key_manager.key_without_namespace(key), value_list
      end

      server.multi_response_completed?
    end

    def servers_with_data(servers, start_time)
      time_left = remaining_time(start_time)
      readable, = IO.select(servers.map(&:sock), nil, nil, time_left)
      return [] if readable.nil?

      readable.map(&:server)
    end

    def groups_for_keys(*keys)
      keys.flatten!
      keys.map! { |a| @key_manager.validate_key(a.to_s) }
      @ring.keys_grouped_by_server(keys)
    end
  end
end
