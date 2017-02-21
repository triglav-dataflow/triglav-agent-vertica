# frozen_string_literal: true

require 'triglav/agent/vertica/processor'
require_relative 'helper'
require_relative 'support/stub_api_client'
require_relative 'support/stub_monitor'

class TestProcessor < Test::Unit::TestCase
  include StubApiClient
  include StubMonitor
  Processor = Triglav::Agent::Vertica::Processor

  def setup
    stub_api_client
    stub_monitor
  end

  def processor
    Processor.new(nil, 'vertica://')
  end

  def test_process_with_success
    success_count = processor.process
    assert { processor.total_count == success_count }
  end

  def test_process_with_error
    stub_error_monitor
    success_count = processor.process
    assert { processor.total_count > success_count }
  end

  def test_process_with_too_many_error
    stub_error_monitor
    stub(Processor).max_consecuitive_error_count { 0 }
    assert_raise(Triglav::Agent::Vertica::TooManyError) { processor.process }
  end
end
