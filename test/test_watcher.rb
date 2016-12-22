# frozen_string_literal: true

require_relative 'helper'
require 'triglav/agent/vertica/watcher'

# This test requires a real connection to vertica, now
# Configure .env to set proper connection_info of test/config.yml
#
# TRIGLAV_URL=http://localhost:3000
# TRIGLAV_USERNAME=triglav_test
# TRIGLAV_PASSWORD=triglav_test
# VERTICA_HOST=vertica
# VERTICA_PORT=5433
# VERTICA_DATABASE=vdb
# VERTICA_USER=dbread
# VERTICA_PASSWORD=xxxxxxx
if File.exist?(File.join(ROOT, '.env'))
  class TestWatcher < Test::Unit::TestCase
    def connection
      return @connection if @connection
      connection_info = $setting.dig(:vertica, :connection_info)[:'vertica://']
      @connection ||= Triglav::Agent::Vertica::Connection.new(connection_info)
    end

    def test_get_events_hourly
      resource = TriglavClient::ResourceResponse.new({
        uri: 'vertica://localhost/vdb/mobage_jp_12019106/raw_log_010101',
        unit: 'hourly',
        timezone: '+09:00',
        span_in_days: 2,
        consumable: true,
        notifiable: false,
      })

      watcher = Triglav::Agent::Vertica::Watcher.new(connection)
      events = watcher.get_events(resource, last_epoch: 0)
      assert { events.size > resource.span_in_days * 24 }
      event = events.first
      assert { event.keys == %i[resource_uri resource_unit resource_time resource_timezone payload] }
      assert { event[:resource_uri] == resource.uri }
      assert { event[:resource_unit] == resource.unit }
      assert { event[:resource_timezone] == resource.timezone }
    end

    def test_get_events_daily
      resource = TriglavClient::ResourceResponse.new({
        uri: 'vertica://localhost/vdb/mobage_jp_12019106/raw_log_010101',
        unit: 'daily',
        timezone: '+09:00',
        span_in_days: 2,
        consumable: true,
        notifiable: false,
      })

      watcher = Triglav::Agent::Vertica::Watcher.new(connection)
      events = watcher.get_events(resource, last_epoch: 0)
      assert { events.size <= resource.span_in_days }
      event = events.first
      assert { event.keys == %i[resource_uri resource_unit resource_time resource_timezone payload] }
      assert { event[:resource_uri] == resource.uri }
      assert { event[:resource_unit] == resource.unit }
      assert { event[:resource_timezone] == resource.timezone }
    end

    def test_get_events_daily_hourly
      resource = TriglavClient::ResourceResponse.new({
        uri: 'vertica://localhost/vdb/mobage_jp_12019106/raw_log_010101',
        unit: 'daily,hourly',
        timezone: '+09:00',
        span_in_days: 2,
        consumable: true,
        notifiable: false,
      })

      watcher = Triglav::Agent::Vertica::Watcher.new(connection)
      events = watcher.get_events(resource, last_epoch: 0)
      assert { events.size <= resource.span_in_days * 24 + 2 }
      assert { events.first[:resource_unit] == 'hourly' }
      assert { events.last[:resource_unit] == 'daily' }
    end
  end
end
