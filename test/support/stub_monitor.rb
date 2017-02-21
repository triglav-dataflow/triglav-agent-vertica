module StubMonitor
  def self.included(klass)
    klass.extend(self)
  end

  def stub_monitor
    stub.proxy(Triglav::Agent::Vertica::Monitor).new do |obj|
      stub(obj).process
    end
  end

  def stub_error_monitor
    stub.proxy(Triglav::Agent::Vertica::Monitor).new do |obj|
      stub(obj).process { raise 'error' }
    end
  end
end
