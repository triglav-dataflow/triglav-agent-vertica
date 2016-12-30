# frozen_string_literal: true

require_relative 'helper'
require_relative 'support/create_table'
require 'triglav/agent/vertica/monitor'

# This test requires a real connection to vertica, now
# Configure .env to set proper connection_info of test/config.yml
#
# TRIGLAV_URL=http://localhost:7800
# TRIGLAV_USERNAME=triglav_test
# TRIGLAV_PASSWORD=triglav_test
# VERTICA_HOST=vertica
# VERTICA_PORT=5433
# VERTICA_DATABASE=vdb
# VERTICA_USER=dbwrite
# VERTICA_PASSWORD=xxxxxxx
if File.exist?(File.join(ROOT, '.env'))
  class TestMonitor < Test::Unit::TestCase
    include CreateTable

    class << self
      def startup
        Timecop.travel(Time.parse("2016-12-30 23:00:00 +09:00"))
        create_table
        insert_data
      end

      def shutdown
        drop_table
        Timecop.return
      end
    end

    def build_resource(params = {})
      TriglavClient::ResourceResponse.new({
        uri: "vertica://localhost/vdb/#{schema}/#{table}",
        unit: 'daily',
        timezone: '+09:00',
        span_in_days: 2,
        consumable: true,
        notifiable: false,
      }.merge(params))
    end

    def test_process
      resource = build_resource
      monitor = Triglav::Agent::Vertica::Monitor.new(connection, resource)
      assert_nothing_raised { monitor.process }
    end

    def test_get_events_hourly
      resource = build_resource(unit: 'hourly')
      monitor = Triglav::Agent::Vertica::Monitor.new(connection, resource, last_epoch: 0)
      events, new_last_epoch = monitor.get_events
      assert { events != nil}
      assert { events.size >= resource.span_in_days * 24 }
      event = events.first
      assert { event.keys == %i[resource_uri resource_unit resource_time resource_timezone payload] }
      assert { event[:resource_uri] == resource.uri }
      assert { event[:resource_unit] == resource.unit }
      assert { event[:resource_timezone] == resource.timezone }
    end

    def test_get_events_daily
      resource = build_resource(unit: 'daily')
      monitor = Triglav::Agent::Vertica::Monitor.new(connection, resource, last_epoch: 0)
      events, new_last_epoch = monitor.get_events
      assert { events != nil}
      assert { events.size <= resource.span_in_days }
      event = events.first
      assert { event.keys == %i[resource_uri resource_unit resource_time resource_timezone payload] }
      assert { event[:resource_uri] == resource.uri }
      assert { event[:resource_unit] == resource.unit }
      assert { event[:resource_timezone] == resource.timezone }
    end

    def test_get_events_daily_hourly
      resource = build_resource(unit: 'daily,hourly')
      monitor = Triglav::Agent::Vertica::Monitor.new(connection, resource, last_epoch: 0)
      events, new_last_epoch = monitor.get_events
      assert { events != nil}
      assert { events.size <= resource.span_in_days * 24 + 2 }
      assert { events.first[:resource_unit] == 'hourly' }
      assert { events.last[:resource_unit] == 'daily' }
    end
  end
end
