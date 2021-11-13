# frozen_string_literal: true

require 'rack/session/abstract/id'
require 'dalli'
require 'connection_pool'
require 'English'

module Rack
  module Session
    # Rack::Session::Dalli provides memcached based session management.
    class Dalli < Abstract::PersistedSecure
      attr_reader :pool

      # Don't freeze this until we fix the specs/implementation
      # rubocop:disable Style/MutableConstant
      DEFAULT_DALLI_OPTIONS = {
        namespace: 'rack:session',
        memcache_server: 'localhost:11211'
      }
      # rubocop:enable Style/MutableConstant

      # Brings in a new Rack::Session::Dalli middleware with the given
      # `:memcache_server`. The server is either a hostname, or a
      # host-with-port string in the form of "host_name:port", or an array of
      # such strings. For example:
      #
      #   use Rack::Session::Dalli,
      #     :memcache_server => "mc.example.com:1234"
      #
      # If no `:memcache_server` option is specified, Rack::Session::Dalli will
      # connect to localhost, port 11211 (the default memcached port). If
      # `:memcache_server` is set to nil, Dalli::Client will look for
      # ENV['MEMCACHE_SERVERS'] and use that value if it is available, or fall
      # back to the same default behavior described above.
      #
      # Rack::Session::Dalli is intended to be a drop-in replacement for
      # Rack::Session::Memcache. It accepts additional options that control the
      # behavior of Rack::Session, Dalli::Client, and an optional
      # ConnectionPool. First and foremost, if you wish to instantiate your own
      # Dalli::Client (or ConnectionPool) and use that instead of letting
      # Rack::Session::Dalli instantiate it on your behalf, simply pass it in
      # as the `:cache` option. Please note that you will be responsible for
      # setting the namespace and any other options on Dalli::Client.
      #
      # Secondly, if you're not using the `:cache` option, Rack::Session::Dalli
      # accepts the same options as Dalli::Client, so it's worth reviewing its
      # documentation. Perhaps most importantly, if you don't specify a
      # `:namespace` option, Rack::Session::Dalli will default to using
      # "rack:session".
      #
      # Whether you are using the `:cache` option or not, it is not recommend
      # to set `:expires_in`. Instead, use `:expire_after`, which will control
      # both the expiration of the client cookie as well as the expiration of
      # the corresponding entry in memcached.
      #
      # Rack::Session::Dalli also accepts a host of options that control how
      # the sessions and session cookies are managed, including the
      # aforementioned `:expire_after` option. Please see the documentation for
      # Rack::Session::Abstract::Persisted for a detailed explanation of these
      # options and their default values.
      #
      # Finally, if your web application is multithreaded, the
      # Rack::Session::Dalli middleware can become a source of contention. You
      # can use a connection pool of Dalli clients by passing in the
      # `:pool_size` and/or `:pool_timeout` options. For example:
      #
      #   use Rack::Session::Dalli,
      #     :memcache_server => "mc.example.com:1234",
      #     :pool_size => 10
      #
      # You must include the `connection_pool` gem in your project if you wish
      # to use pool support. Please see the documentation for ConnectionPool
      # for more information about it and its default options (which would only
      # be applicable if you supplied one of the two options, but not both).
      #
      def initialize(app, options = {})
        # Parent uses DEFAULT_OPTIONS to build @default_options for Rack::Session
        super

        # Determine the default TTL for newly-created sessions
        @default_ttl = ttl(@default_options[:expire_after])

        # Normalize and validate passed options
        mserv, mopts, popts = extract_dalli_options(options)

        @pool = ConnectionPool.new(popts || {}) { ::Dalli::Client.new(mserv, mopts) }
      end

      def get_session(_env, sid)
        with_dalli_client([nil, {}]) do |dc|
          existing_session = existing_session_for_sid(dc, sid)
          return [sid, existing_session] unless existing_session.nil?

          [create_sid_with_empty_session(dc), {}]
        end
      end

      def set_session(_env, session_id, new_session, options)
        return false unless session_id

        with_dalli_client(false) do |dc|
          dc.set(memcached_key_from_sid(session_id), new_session, ttl(options[:expire_after]))
          session_id
        end
      end

      def destroy_session(_env, session_id, options)
        with_dalli_client do |dc|
          dc.delete(memcached_key_from_sid(session_id))
          generate_sid_with(dc) unless options[:drop]
        end
      end

      def find_session(req, sid)
        get_session(req.env, sid)
      end

      def write_session(req, sid, session, options)
        set_session(req.env, sid, session, options)
      end

      def delete_session(req, sid, options)
        destroy_session(req.env, sid, options)
      end

      private

      def memcached_key_from_sid(sid)
        return sid.private_id if sid.respond_to?(:private_id)

        sid
      end

      def existing_session_for_sid(client, sid)
        return nil unless sid && !sid.empty?

        client.get(memcached_key_from_sid(sid))
      end

      def create_sid_with_empty_session(client)
        loop do
          sid = generate_sid_with(client)
          break sid if client.add(memcached_key_from_sid(sid), {}, @default_ttl)
        end
      end

      def extract_dalli_options(options)
        raise 'Rack::Session::Dalli no longer supports the :cache option.' if options[:cache]

        # Filter out Rack::Session-specific options and apply our defaults
        filtered_opts = options.reject { |k, _| DEFAULT_OPTIONS.key? k }
        mopts = DEFAULT_DALLI_OPTIONS.merge(filtered_opts)
        mserv = mopts.delete :memcache_server

        popts = pool_options(mopts)
        [mserv, mopts, popts]
      end

      def pool_options(client_options)
        return {} unless client_options[:pool_size] || client_options[:pool_timeout]

        pool_opts = {}
        pool_opts[:size] = client_options.delete(:pool_size) if client_options[:pool_size]
        pool_opts[:timeout] = client_options.delete(:pool_timeout) if client_options[:pool_timeout]

        # Set to false, since there is a connection pool to enforce access
        client_options[:threadsafe] = false

        pool_opts
      end

      def generate_sid_with(client)
        loop do
          sid = generate_sid
          break sid unless client.get(sid)
        end
      end

      def with_dalli_client(result_on_error = nil, &block)
        @pool.with(&block)
      rescue ::Dalli::DalliError, Errno::ECONNREFUSED
        raise if /undefined class/.match?($ERROR_INFO.message)

        if $VERBOSE
          warn "#{self} is unable to find memcached server."
          warn $ERROR_INFO.inspect
        end
        result_on_error
      end

      def ttl(expire_after)
        expire_after.nil? ? 0 : expire_after + 1
      end
    end
  end
end
