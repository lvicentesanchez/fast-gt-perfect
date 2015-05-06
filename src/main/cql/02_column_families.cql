CREATE TABLE fast.trends (
  time TIMESTAMP,
  tags BLOB,
  PRIMARY KEY time
);

CREATE TABLE fast.revenue (
  time TIMESTAMP,
  filter BLOB,
  amount INT,
  PRIMARY KEY time
);

CREATE TABLE fast.unique (
  time TIMESTAMP,
  counter BLOB,
  PRIMARY KEY time
);
