require 'triglav/agent/vertica/connection'
require 'vertica'
require 'uri'

module Triglav::Agent::Vertica
  class Monitor
    attr_reader :connection, :resource, :last_epoch

    # @param [Triglav::Agent::Vertica::Connection] connection
    # @param [TriglavClient::ResourceResponse] resource
    # resource:
    #   uri: vertica://host/database/schema/table
    #   unit: 'daily' or 'hourly', or 'daily,hourly'
    #   timezone: '+09:00'
    #   span_in_days: 32
    # @param [Integer] last_epoch (for debug)
    def initialize(connection, resource, last_epoch: nil)
      @connection = connection
      @resource = resource
      @last_epoch = last_epoch || get_last_epoch
    end

    def process
      if !%w[daily hourly daily,hourly].include?(resource.unit) ||
          resource.timezone.nil? || resource.span_in_days.nil?
        $logger.warn { "Broken resource: #{resource.to_s}" }
        return nil
      end

      $logger.debug { "Start process #{resource.uri}, last_epoch:#{last_epoch}" }
      events, new_last_epoch = get_events
      $logger.debug { "Finish process #{resource.uri}, last_epoch:#{last_epoch}, new_last_epoch:#{new_last_epoch}" }
      return if events.nil? || events.empty?
      yield(events) # send_message
      update_status_file(new_last_epoch)
    end

    def get_events
      q_last_epoch = Vertica.quote(last_epoch)

      _, schema, table = URI.parse(resource.uri).path[1..-1].split('/')
      q_schema = Vertica.quote_identifier(schema)
      q_table  = Vertica.quote_identifier(table)

      q_date = Vertica.quote_identifier(date_column)
      q_timestamp = Vertica.quote_identifier(timestamp_column)

      case query_unit
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
      rows = connection.query(sql)

      events = build_events(rows)
      if resource.unit == 'daily,hourly'
        daily_events = build_daily_events_from_hourly(rows)
        events.concat(daily_events)
      end

      new_last_epoch = latest_epoch(rows)
      [events, new_last_epoch]
    rescue Vertica::Error::QueryError => e
      $logger.warn { "#{e.class} #{e.message}" } # e.message includes sql
      nil
    rescue Vertica::Error::TimedOutError => e
      $logger.warn { "#{e.class} #{e.message} SQL:#{sql}" }
      nil
    end

    private

    def update_status_file(last_epoch)
      Triglav::Agent::StorageFile.set(
        $setting.status_file,
        [:last_epoch, resource.uri.to_sym],
        last_epoch
      )
    end

    def get_last_epoch
      Triglav::Agent::StorageFile.getsetnx(
        $setting.status_file,
        [:last_epoch, resource.uri.to_sym],
        get_current_epoch
      )
    end

    def get_current_epoch
      connection.query('select GET_CURRENT_EPOCH()').first.first
    end

    # 'daily,hourly': qeury in hourly way, then merge events into daily in ruby
    def query_unit
      @query_unit ||= resource.unit == 'daily,hourly' ? 'hourly' : resource.unit
    end

    def dates
      return @dates if @dates
      now = Time.now.localtime(resource.timezone)
      @dates = resource.span_in_days.times.map do |i|
        (now - (i * 86000)).strftime('%Y-%m-%d')
      end
    end

    def latest_epoch(rows)
      rows.map {|row| row[2] }.max || last_epoch
    end

    def build_events(rows)
      rows.map do |row|
        date, hour, epoch = row[0], row[1], row[2]
        {
          resource_uri: resource.uri,
          resource_unit: query_unit,
          resource_time: date_hour_to_i(date, hour, resource.timezone),
          resource_timezone: resource.timezone,
          payload: {epoch: epoch},
        }
      end
    end

    def build_daily_events_from_hourly(rows)
      max_epoch_of = {}
      rows.each do |row|
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

    def date_column
      $setting.dig(:vertica, :date_column) || 'd'
    end

    def timestamp_column
      $setting.dig(:vertica, :timestamp_column) || 't'
    end
  end
end
