require 'rubyXL'
require 'pg'
require 'date'

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

def add_visit(id, region, mloa_start, mloa_end, reason, consumer_id)
  sql = "INSERT into visit (id, region, mloa_start, mloa_end, reason, consumer_id) values ($1, $2, $3, $4, $5, $6)"

  db_connection do |conn|
    conn.exec_params(sql, [id, region, mloa_start, mloa_end, reason, consumer_id])
  end
end

def get_consumers
  consumers = db_connection do |conn|
    conn.exec("SELECT id FROM consumer")
  end
  consumers.to_a
end

def add_all_data
  mloa_start = nil
  sorted_data = Case_DataSet.worksheets[1].extract_data[1..Case_DataSet.worksheets[1].extract_data.to_a.length].sort_by do |row|
    [row[1], row[3]]
  end

  sorted_data.each_with_index do |row, index|
    current_consumers = get_consumers
    next_row = (index + 1) < sorted_data.length ? sorted_data[index + 1]: [0, 0, "Z", DateTime.new(1800, 1, 1), "end of the list so considering this the end of the visit"]
    stay_continues = row[1] == next_row[1] && row[2] == next_row[2] && next_row[3] - row[3] == 1 && row[4] == next_row[4]

    unless current_consumers.include?({"id" => row[1].to_s})
      add_consumer(row[1])
    end

    if stay_continues && mloa_start.nil?
      mloa_start = row[3]
    end

    if !(stay_continues) && mloa_start != nil
      add_visit(row[0], row[2], mloa_start, row[3], row[4], row[1])
      mloa_start = nil
    elsif !(stay_continues) && mloa_start.nil?
      add_visit(row[0], row[2], row[3], row[3], row[4], row[1])
    end
  end
end

def region_a_hospitalizations
  sql = "SELECT consumer_id FROM visit WHERE visit.region = 'A' AND visit.reason = 'Hospital' AND visit.mloa_start > '2013-12-31' ORDER BY visit.consumer_id;"
  hospitalizations = db_connection do |conn|
    conn.exec(sql)
  end

  hospitalizations.to_a.length
end

def region_b_hospitalizations
  sql = "SELECT consumer_id FROM visit WHERE visit.region = 'B' AND visit.reason = 'Hospital' AND visit.mloa_start > '2013-12-31' ORDER BY visit.consumer_id;"
  hospitalizations = db_connection do |conn|
    conn.exec(sql)
  end

  hospitalizations.to_a.length
end

def total_hospitializations
  sql = "select count(consumer_id) from visit where visit.reason = 'Hospital' AND visit.mloa_start > '2013-12-31' group by consumer_id;"

  hospitalization_count = db_connection do |conn|
    conn.exec(sql)
  end
  hospitalization_count.to_a.length
end

def average_stay
  stay_lengths = 0
  total_visitors = 0
  # sql = "select mloa_end - mloa_start from visit where visit.reason = 'Hospital' AND visit.mloa_start > '2013-12-31';"
  sql = "SELECT consumer_id, mloa_start, mloa_end FROM visit WHERE visit.reason = 'Hospital' AND visit.mloa_start > '2013-12-31' ORDER BY visit.consumer_id;"
  hospitalizations = db_connection do |conn|
    conn.exec(sql)
  end

  hospitalizations.each do |visit|
    stay_lengths += Date.parse(visit["mloa_end"]) - Date.parse(visit["mloa_start"]) + 1
    total_visitors += 1
  end

  (stay_lengths/total_visitors).to_f
end

# add_all_data
puts "Question 1"
puts "Region A Hospitalizations: #{region_a_hospitalizations} unique hospitalizations"
puts "Region B Hospitalizations: #{region_b_hospitalizations} unique hospitalizations"
puts ""
puts "Question 2"
puts "Total Consumers Hospitalized: #{total_hospitializations} hospitalizations"
puts ""
puts "Question 3"
puts "Average Hospital Stay: #{average_stay} days"
