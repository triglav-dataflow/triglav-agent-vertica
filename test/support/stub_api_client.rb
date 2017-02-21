module StubApiClient
  def self.included(klass)
    klass.extend(self)
  end

  def stub_api_client
    stub.proxy(Triglav::Agent::ApiClient).new do |obj|
      stub(obj).list_aggregated_resources { [dummy_resource] }
      stub(obj).send_messages { }
    end
  end

  def dummy_resource
    TriglavClient::AggregatedResourceEachResponse.new(
      uri: 'vertica://vdev/vdb/sandbox/triglav_test',
      unit: 'daily',
      timezone: '+09:00',
      span_in_days: 2,
    )
  end
end
