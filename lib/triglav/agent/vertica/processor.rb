require 'triglav/agent/vertica/monitor'
require 'triglav/agent/vertica/connection'
require 'triglav/agent/vertica/error'

module Triglav::Agent
  module Vertica
    class Processor
      attr_reader :worker, :resource_uri_prefix

      def initialize(worker, resource_uri_prefix)
        @worker = worker
        @resource_uri_prefix = resource_uri_prefix
      end

      def self.max_consecuitive_error_count
        3
      end

      def process
        success_count = 0
        consecutive_error_count = 0
        resources.each do |resource|
          break if stopped?
          events = nil
          begin
            monitor = Monitor.new(connection, resource, last_epoch: $setting.debug? ? 0 : nil)
            monitor.process {|_events| events = _events; api_client.send_messages(events) }
            success_count += 1
            consecutive_error_count = 0
          rescue => e
            log_error(e)
            $logger.info { "failed_events:#{events.to_json}" } if events
            raise TooManyError if (consecutive_error_count += 1) > self.class.max_consecuitive_error_count
          end
        end
        success_count
      end

      def total_count
        resources.size
      end

      private

      def api_client
        @api_client ||= ApiClient.new # renew connection
      end

      def resources
        return @resources if @resources
        @resources = api_client.list_aggregated_resources(resource_uri_prefix) || []
        $logger.debug { "resource_uri_prefix:#{resource_uri_prefix} resources.size:#{@resources.size}" }
        @resources
      end

      def connection
        @connection ||= Connection.new(get_connection_info(resource_uri_prefix))
      end

      def get_connection_info(resource_uri_prefix)
        $setting.dig(:vertica, :connection_info)[resource_uri_prefix]
      end

      def log_error(e)
        $logger.error { "#{e.class} #{e.message} #{e.backtrace.join("\\n")}" } # one line
      end

      def stopped?
        worker.stopped? if worker
      end
    end
  end
end
