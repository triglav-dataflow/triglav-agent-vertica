require 'triglav/agent/vertica/monitor'
require 'triglav/agent/vertica/connection'
require 'triglav/agent/vertica/error'
require 'parallel'
require 'connection_pool'

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
        mutex = Mutex.new
        Parallel.each(resources, in_threads: 8) do |resource|
          raise Parallel::Break if stopped?
          events = nil
          begin
            connection_pool.with do |connection|
              monitor = Monitor.new(connection, resource, last_epoch: $setting.debug? ? 0 : nil)
              monitor.process do |_events|
                events = _events
                $logger.info { "send_messages:#{events.map(&:to_hash).to_json}" }
                api_client_pool.with {|api_client| api_client.send_messages(events) }
              end
            end
            mutex.synchronize do
              success_count += 1
              consecutive_error_count = 0
            end
          rescue => e
            log_error(e)
            $logger.info { "failed_events:#{events.map(&:to_hash).to_json}" } if events
            mutex.synchronize do
              raise TooManyError if (consecutive_error_count += 1) > self.class.max_consecuitive_error_count
            end
          end
        end
        success_count
      end

      def total_count
        resources.size
      end

      private

      def resources
        return @resources if @resources
        @resources = ApiClient.new.list_aggregated_resources(resource_uri_prefix) || []
        $logger.debug { "resource_uri_prefix:#{resource_uri_prefix} resources.size:#{@resources.size}" }
        @resources
      end

      def api_client_pool
        @api_client_pool ||= ConnectionPool.new(size: 8, timeout: 5) {
          ApiClient.new # renew connection
        }
      end

      def connection_pool
        @connection_pool ||= ConnectionPool.new(size: 8, timeout: 5) {
          Connection.new(get_connection_info(resource_uri_prefix))
        }
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
