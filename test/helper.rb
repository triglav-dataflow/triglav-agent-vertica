# frozen_string_literal: true

require 'test/unit'
require 'test/unit/rr'
require 'pry'
require 'timecop'
require 'triglav-agent-vertica'
require 'triglav/agent/vertica/configuration'

TEST_ROOT = __dir__
ROOT = File.dirname(__dir__)

opts = {
  config: File.join(TEST_ROOT, 'config.yml'),
  status: File.join(TEST_ROOT, 'tmp', 'status.yml'),
  token: File.join(TEST_ROOT, 'tmp', 'token.yml'),
  dotenv: true,
  debug: true,
}
$setting = Triglav::Agent::Configuration.setting_class.new(opts)
$logger = $setting.logger
