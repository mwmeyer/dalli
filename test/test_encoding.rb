# frozen_string_literal: true

require_relative 'helper'

describe 'Encoding' do
  describe 'using a live server' do
    it 'support i18n content' do
      memcached_persistent do |dc|
        key = 'foo'
        utf_key = utf8 = 'ƒ©åÍÎ'

        assert dc.set(key, utf8)
        assert_equal utf8, dc.get(key)

        dc.set(utf_key, utf8)
        assert_equal utf8, dc.get(utf_key)
      end
    end

    it 'support content expiry' do
      memcached_persistent do |dc|
        key = 'foo'
        assert dc.set(key, 'bar', 1)
        assert_equal 'bar', dc.get(key)
        sleep 1.2
        assert_nil dc.get(key)
      end
    end
  end
end
