require 'triglav/agent/vertica/monitor'
require 'triglav/agent/vertica/connection'

module Triglav::Agent
  module Vertica
    module Worker
      # serverengine interface
      def initialize
        @timer = Timer.new
      end

      # serverengine interface
      def reload
        $logger.info { "Worker#reload worker_id:#{worker_id}" }
        $setting.reload
      end

      # serverengine interface
      def run
        $logger.info { "Worker#run worker_id:#{worker_id}" }
        start
        until @stop
          @timer.wait(monitor_interval) { process }
        end
      end

      def process
        $logger.info { "Start Worker#process worker_id:#{worker_id}" }
        api_client = ApiClient.new # renew connection

        # It is possible to seperate agent process by prefixes of resource uris
        count = 0
        resource_uri_prefixes.each do |resource_uri_prefix|
          break if stopped?
          # list_aggregated_resources returns unique resources which we have to monitor
          next unless aggregated_resources = api_client.list_aggregated_resources(resource_uri_prefix)
          $logger.debug { "resource_uri_prefix:#{resource_uri_prefix} aggregated_resources.size:#{aggregated_resources.size}" }
          connection = Connection.new(get_connection_info(resource_uri_prefix))
          monitor = Monitor.new(connection)
          aggregated_resources.each do |resource|
            break if stopped?
            count += 1
            monitor.process(resource) {|events| api_client.send_messages(events) }
          end
        end
        $logger.info { "Finish Worker#process worker_id:#{worker_id} count:#{count}" }
      end

      def start
        @timer.start
        @stop = false
      end

      # serverengine interface
      def stop
        $logger.info { "Worker#stop worker_id:#{worker_id}" }
        @stop = true
        @timer.stop
      end

      def stopped?
        @stop
      end

      private

      def monitor_interval
        $setting.dig(:vertica, :monitor_interval) || 60
      end

      def resource_uri_prefixes
        $setting.dig(:vertica, :connection_info).keys
      end

      def get_connection_info(resource_uri_prefix)
        $setting.dig(:vertica, :connection_info)[resource_uri_prefix]
      end
    end
  end
end
