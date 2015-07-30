require 'rubyXL'
require 'pg'
require 'date'
require 'pry'
Case_DataSet = RubyXL::Parser.parse('/home/rocco/Dropbox/Case_DataSet.xlsx')

def db_connection
  begin
    connection = PG.connect(dbname: "case_study")
    yield(connection)
  ensure
    connection.close
  end
end

def add_consumer(id)
  sql = "INSERT into consumer (id) values ($1)"

  db_connection do |conn|
    conn.exec_params(sql, [id])
  end
end

def add_visit(id, region, mloa_days, reason, consumer_id)
  sql = "INSERT into visit (id, region, mloa_days, reason, consumer_id) values ($1, $2, $3, $4, $5)"

  db_connection do |conn|
    conn.exec_params(sql, [id, region, mloa_days, reason, consumer_id])
  end
end

def get_consumers
  consumers = db_connection do |conn|
    conn.exec("SELECT id FROM consumer")
  end
  consumers.to_a
end

def add_all_data
  binding.pry
  Case_DataSet.worksheets[1].extract_data.each_with_index do |row, index|
    current_consumers = get_consumers
    unless index == 0
      unless current_consumers.include?({"id" => row[1].to_s})
        add_consumer(row[1])
      end
      add_visit(row[0], row[2], row[3], row[4], row[1])
    end
  end
end

def region_a_hospitalizations
  unique_hospitalizations = 0
  sql = "SELECT consumer_id, mloa_days FROM visit WHERE visit.region = 'A' AND visit.reason = 'Hospital' AND visit.mloa_days > '2013-12-31' ORDER BY visit.consumer_id, visit.mloa_days ASC;"
  hospitalizations = db_connection do |conn|
    conn.exec(sql)
  end

  hospitalizations.each_with_index do |event, index|
    next_event = (index + 1) < hospitalizations.to_a.length ? hospitalizations[index + 1]: {"consumer_id" => nil, "mloa_days" => "1800-01-01"}
    same_consumer = event["consumer_id"] == next_event["consumer_id"]
    successive_days = (Date.parse(next_event["mloa_days"]) - Date.parse(event["mloa_days"])) == 1

    unless same_consumer && successive_days
      unique_hospitalizations += 1
    end
  end
  unique_hospitalizations
end

def region_b_hospitalizations
  unique_hospitalizations = 0
  sql = "SELECT consumer_id, mloa_days FROM visit WHERE visit.region = 'B' AND visit.reason = 'Hospital' AND visit.mloa_days > '2013-12-31' ORDER BY visit.consumer_id, visit.mloa_days ASC;"
  hospitalizations = db_connection do |conn|
    conn.exec(sql)
  end

  hospitalizations.each_with_index do |event, index|
    next_event = (index + 1) < hospitalizations.to_a.length ? hospitalizations[index + 1]: {"consumer_id" => nil, "mloa_days" => "1800-01-01"}
    same_consumer = event["consumer_id"] == next_event["consumer_id"]
    successive_days = Date.parse(next_event["mloa_days"]) - Date.parse(event["mloa_days"]) == 1

    unless same_consumer && successive_days
      unique_hospitalizations += 1
    end
  end
  unique_hospitalizations
end

def total_hospitializations
  sql = "select count(consumer_id) from visit where visit.reason = 'Hospital' AND visit.mloa_days > '2013-12-31' group by consumer_id;"

  hospitalization_count = db_connection do |conn|
    conn.exec(sql)
  end
  hospitalization_count.to_a.length
end

def average_stay
  stay_length = 1
  stay_lengths = 0
  total_visits = 0
  sql = "SELECT consumer_id, mloa_days FROM visit WHERE visit.reason = 'Hospital' AND visit.mloa_days > '2013-12-31' ORDER BY visit.consumer_id, visit.mloa_days ASC;"
  hospitalizations = db_connection do |conn|
    conn.exec(sql)
  end

  hospitalizations.each_with_index do |event, index|
    next_event = (index + 1) < hospitalizations.to_a.length ? hospitalizations[index + 1]: {"consumer_id" => nil, "mloa_days" => "1800-01-01"}
    same_consumer = event["consumer_id"] == next_event["consumer_id"]
    successive_days = Date.parse(next_event["mloa_days"]) - Date.parse(event["mloa_days"]) == 1

    if same_consumer && successive_days
      stay_length += 1
    else
      stay_lengths += stay_length
      total_visits += 1
      stay_length = 1
    end
  end
  (stay_lengths/total_visits).to_f
end

add_all_data
puts "Question 1"
puts "Region A Hospitalizations: #{region_a_hospitalizations} unique hospitalizations"
puts "Region B Hospitalizations: #{region_b_hospitalizations} unique hospitalizations"
puts ""
puts "Question 2"
puts "Total Consumers Hospitalized: #{total_hospitializations} hospitalizations"
puts ""
puts "Question 3"
puts "Average Hospital Stay: #{average_stay} days"
