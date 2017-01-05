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
      rescue => e
        # ServerEngine.dump_uncaught_error does not tell me e.class
        log_error(e)
        raise e
      end

      MAX_CONSECUTIVE_ERROR_COUNT = 3

      def process
        started = Time.now
        $logger.info { "Start Worker#process worker_id:#{worker_id}" }
        api_client = ApiClient.new # renew connection

        count = 0
        consecutive_error_count = 0
        catch(:break) do
          # It is possible to seperate agent process by prefixes of resource uris
          resource_uri_prefixes.each do |resource_uri_prefix|
            break if stopped?
            # list_aggregated_resources returns unique resources which we have to monitor
            next unless resources = api_client.list_aggregated_resources(resource_uri_prefix)
            $logger.debug { "resource_uri_prefix:#{resource_uri_prefix} resources.size:#{resources.size}" }
            connection = Connection.new(get_connection_info(resource_uri_prefix))
            resources.each do |resource|
              throw(:break) if stopped?
              count += 1
              monitor = Monitor.new(connection, resource, last_epoch: $setting.debug? ? 0 : nil)
              begin
                monitor.process {|events| api_client.send_messages(events) }
                consecutive_error_count = 0
              rescue => e
                log_error(e)
                throw(:break) if (consecutive_error_count += 1) >= MAX_CONSECUTIVE_ERROR_COUNT
              end
            end
          end
        end
        elapsed = Time.now - started
        $logger.info { "Finish Worker#process worker_id:#{worker_id} count:#{count} elapsed:#{elapsed.to_f}sec" }
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

      def log_error(e)
        $logger.error { "#{e.class} #{e.message} #{e.backtrace.join("\\n")}" } # one line
      end

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
