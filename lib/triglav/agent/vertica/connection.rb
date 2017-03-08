require 'triglav/agent/base/connection'
require 'vertica'
require 'uri'

module Triglav::Agent
  module Vertica
    class Connection < Base::Connection
      attr_reader :connection_info

      def initialize(connection_info)
        @connection_info = connection_info
      end

      def close
        @connection.close rescue nil if @connection
      end

      def query(sql)
        connection.query(sql)
      end

      private

      def connection
        return @connection if @connection
        begin
          @connection = ::Vertica.connect(connection_params)
        rescue => e
          $logger.error { "Failed to connect #{connection_info[:host]}:#{connection_info[:port]} with #{connection_info[:user]}" }
          raise e
        end
        $logger.info { "Connected to #{connection_info[:host]}:#{connection_info[:port]}" }
        set_resource_pool
        set_memorycap
        @connection
      end

      def set_resource_pool
        if @connection_info[:resource_pool] and !@connection_info[:resource_pool].empty?
          @connection.query("set session resource_pool = '#{@connection_info[:resource_pool]}'")
        end
      end

      def set_memorycap
        if @connection_info[:memorycap] and !@connection_info[:memorycap].empty?
          @connection.query("set session memorycap = '#{@connection_info[:memorycap]}'")
        end
      end

      def connection_params
        params = @connection_info.dup
        params.delete(:resource_pool)
        params.delete(:memorycap)
        params.merge!(global_connection_params)
      end

      def global_connection_params
        params = {}
        params[:interruptable] = $setting.dig(:vertica, :interruptable) if $setting[:vertica].has_key?(:interruptable)
        params[:read_timeout] = $setting.dig(:vertica, :read_timeout) if $setting[:vertica].has_key?(:read_timeout)
        params
      end
    end
  end
end
