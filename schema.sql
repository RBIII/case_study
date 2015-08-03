CREATE TABLE consumer (
  id integer PRIMARY KEY
);

CREATE TABLE visit (
  id integer PRIMARY KEY,
  region varchar(10) NOT NULL,
  mloa_start date NOT NULL,
  mloa_end date NOT NULL,
  reason varchar(30) NOT NULL,
  consumer_id integer NOT NULL,
  FOREIGN KEY (consumer_id) references consumer(id)
);
