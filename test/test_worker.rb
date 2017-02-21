# frozen_string_literal: true

require 'triglav/agent/vertica/worker'
require_relative 'helper'
require_relative 'support/stub_processor'

class TestWorker < Test::Unit::TestCase
  include StubProcessor

  class Worker
    include Triglav::Agent::Vertica::Worker
    define_method(:worker_id) { 1 }
  end

  def setup
    stub_processor
  end

  def worker
    Worker.new
  end

  def test_process_with_success
    assert_nothing_raised { worker.process }
  end
end
