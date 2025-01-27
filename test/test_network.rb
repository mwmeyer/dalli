# frozen_string_literal: true

require_relative 'helper'

describe 'Network' do
  describe 'assuming a bad network' do
    it 'handle no server available' do
      assert_raises Dalli::RingError, message: 'No server available' do
        dc = Dalli::Client.new 'localhost:19333'
        dc.get 'foo'
      end
    end

    describe 'with a fake server' do
      it 'handle connection reset' do
        memcached_mock(->(sock) { sock.close }) do
          assert_raises Dalli::RingError, message: 'No server available' do
            dc = Dalli::Client.new('localhost:19123')
            dc.get('abc')
          end
        end
      end

      it 'handle connection reset with unix socket' do
        socket_path = MemcachedMock::UNIX_SOCKET_PATH
        memcached_mock(->(sock) { sock.close }, :start_unix, socket_path) do
          assert_raises Dalli::RingError, message: 'No server available' do
            dc = Dalli::Client.new(socket_path)
            dc.get('abc')
          end
        end
      end

      it 'handle malformed response' do
        memcached_mock(->(sock) { sock.write('123') }) do
          assert_raises Dalli::RingError, message: 'No server available' do
            dc = Dalli::Client.new('localhost:19123')
            dc.get('abc')
          end
        end
      end

      it 'handle connect timeouts' do
        memcached_mock(lambda { |sock|
                         sleep(0.6)
                         sock.close
                       }, :delayed_start) do
          assert_raises Dalli::RingError, message: 'No server available' do
            dc = Dalli::Client.new('localhost:19123')
            dc.get('abc')
          end
        end
      end

      it 'handle read timeouts' do
        memcached_mock(lambda { |sock|
                         sleep(0.6)
                         sock.write('giraffe')
                       }) do
          assert_raises Dalli::RingError, message: 'No server available' do
            dc = Dalli::Client.new('localhost:19123')
            dc.get('abc')
          end
        end
      end
    end
  end
end
