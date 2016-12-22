require 'triglav/agent/vertica/connection'
require 'vertica'
require 'uri'

module Triglav::Agent::Vertica
  class Watcher
    attr_reader :connection

    def initialize(connection)
      @connection = connection
      # IMPORTANT ASSUMPTION: other processes does not modify status file
      @status = Triglav::Agent::StorageFile.load($setting.status_file)
    end

    def process(resource)
      events = get_events(resource, last_epoch: $setting.debug? ? 0 : nil)
      return if events.nil? || events.empty?
      yield(events) # send_message
      Triglav::Agent::StorageFile.open($setting.status_file) do |fp|
        @status = fp.load # reload during locking
        events.each {|event| update_status(event) }
        fp.dump(@status)
      end
    end

    # @param [TriglavClient::ResourceResponse] resource
    # resource:
    #   uri: vertica://host/database/schema/table
    #   unit: 'daily' or 'hourly', or 'daily,hourly'
    #   timezone: '+09:00'
    #   span_in_days: 32
    # @param [Ineger] last_epoch (for debug)
    def get_events(resource, last_epoch: nil)
      if !%w[daily hourly daily,hourly].include?(resource.unit) ||
          resource.timezone.nil? || resource.span_in_days.nil?
        $logger.warn { "Broken resource: #{resource.to_s}" }
        return nil
      end

      now = Time.now.localtime(resource.timezone)
      dates = resource.span_in_days.times.map do |i|
        (now - (i * 86000)).strftime('%Y-%m-%d')
      end

      last_epoch ||= get_last_epoch(resource.uri)
      q_last_epoch = Vertica.quote(last_epoch)

      _, schema, table = URI.parse(resource.uri).path[1..-1].split('/')
      q_schema = Vertica.quote_identifier(schema)
      q_table  = Vertica.quote_identifier(table)

      q_date = Vertica.quote_identifier(date_column)
      q_timestamp = Vertica.quote_identifier(timestamp_column)

      # 'daily,hourly': qeury in hourly way, then merge events into daily in ruby
      unit = resource.unit == 'daily,hourly' ? 'hourly' : resource.unit

      begin
        case unit
        when 'hourly'
          sql = "select " \
            "#{q_date} AS d, DATE_PART('hour', #{q_timestamp}) AS h, max(epoch) " \
            "from #{q_schema}.#{q_table} " \
            "where #{q_date} IN ('#{dates.join("','")}') " \
            "group by d, h having max(epoch) > #{q_last_epoch} " \
            "order by d, h"
        when 'daily'
          sql = "select " \
            "#{q_date} AS d, 0 AS h, max(epoch) " \
            "from #{q_schema}.#{q_table} " \
            "where #{q_date} IN ('#{dates.join("','")}') " \
            "group by d having max(epoch) > #{q_last_epoch} " \
            "order by d"
        end

        $logger.debug { "Query: #{sql}" }
        result = connection.query(sql)

        events = build_events(result, resource, unit)
        if resource.unit == 'daily,hourly'
          daily_events = build_daily_events_from_hourly(result, resource)
          events.concat(daily_events)
        end
        events
      rescue Vertica::Error::QueryError => e
        $logger.warn { "#{e.class} #{e.message}" }
        nil
      end
    end

    private

    def build_events(result, resource, unit = resource.unit)
      result.map do |row|
        date, hour, epoch = row[0], row[1], row[2]
        {
          resource_uri: resource.uri,
          resource_unit: unit,
          resource_time: date_hour_to_i(date, hour, resource.timezone),
          resource_timezone: resource.timezone,
          payload: {epoch: epoch},
        }
      end
    end

    def build_daily_events_from_hourly(result, resource)
      max_epoch_of = {}
      result.each do |row|
        date, hour, epoch = row[0], row[1], row[2]
        max_epoch_of[date] = [epoch, max_epoch_of[date] || 0].max
      end
      daily_events = max_epoch_of.map do |date, epoch|
        {
          resource_uri: resource.uri,
          resource_unit: 'daily',
          resource_time: date_hour_to_i(date, 0, resource.timezone),
          resource_timezone: resource.timezone,
          payload: {epoch: epoch},
        }
      end
    end

    def date_hour_to_i(date, hour, timezone)
      Time.strptime("#{date.to_s} #{hour.to_i} #{timezone}", '%Y-%m-%d %H %z').to_i
    end

    def update_status(event)
      (@status[:last_epoch] ||= {})[event[:resource_uri].to_sym] = event.dig(:payload, :epoch)
    end

    def get_last_epoch(resource_uri)
      @status.dig(:last_epoch, resource_uri.to_sym) || get_current_epoch
    end

    def get_current_epoch
      connection.query('select GET_CURRENT_EPOCH()').first.first
    end

    def date_column
      $setting.dig(:vertica, :date_column) || 'd'
    end

    def timestamp_column
      $setting.dig(:vertica, :timestamp_column) || 't'
    end
  end
end
