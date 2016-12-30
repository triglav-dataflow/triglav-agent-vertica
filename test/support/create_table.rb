module CreateTable
  def self.included(klass)
    klass.extend(self)
  end

  def schema
    'sandbox'
  end

  def table
    'triglv_test'
  end

  def data
    now = Time.now
    50.times.map do |i|
      t = now - i * 3600
      { logtime: t.to_i, d: t.strftime("%Y-%m-%d"), t: t.strftime("%Y-%m-%d %H:%M:%S") }
    end
  end

  def create_table
    connection.query(<<~SQL)
      CREATE TABLE IF NOT EXISTS #{schema}.#{table} (
        logtime integer,
        d date,
        t timestamp
      );
    SQL
  end

  def insert_data
    data.each do |row|
      connection.query(%Q[INSERT INTO #{schema}.#{table} VALUES (#{row[:logtime]}, '#{row[:d]}', '#{row[:t]}')])
    end
    connection.query('commit')
  end

  def drop_table
    connection.query("DROP TABLE IF EXISTS #{schema}.#{table}")
  end

  def connection
    return @connection if @connection
    connection_info = $setting.dig(:vertica, :connection_info)[:'vertica://']
    @connection ||= Triglav::Agent::Vertica::Connection.new(connection_info)
  end
end
