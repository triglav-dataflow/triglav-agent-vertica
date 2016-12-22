require 'triglav/agent/vertica/watcher'
require 'triglav/agent/vertica/connection'

module Triglav::Agent
  module Vertica
    module Worker
      def initialize
        @timer = Timer.new
      end

      def reload
        $setting.reload
      end

      def run
        until @stop
          @timer.wait(watcher_interval) { process }
        end
      end

      def process
        $logger.debug { "Start Processing #{worker_id}" }
        api_client = ApiClient.new # renew connection

        # It is possible to seperate agent process by prefixes of resource uris
        resource_uri_prefixes.each do |resource_uri_prefix|
          # list_aggregated_resources returns unique resources which we have to monitor
          if aggregated_resources = api_client.list_aggregated_resources(resource_uri_prefix)
            $logger.debug { "resource_uri_prefix:#{resource_uri_prefix} #{aggregated_resources.size}" }
            connection = Connection.new(get_connection_info(resource_uri_prefix))
            watcher = Watcher.new(connection)
            aggregated_resources.each do |resource|
              watcher.process(resource) {|events| api_client.send_messages(events) }
            end
          end
        end
        $logger.debug { "End Processing #{worker_id}" }
      end

      def stop
        $logger.info { "Stop processing #{worker_id}" }
        @stop = true
        @timer.signal
      end

      private

      def watcher_interval
        $setting.dig(:vertica, :watcher_interval) || 60
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
