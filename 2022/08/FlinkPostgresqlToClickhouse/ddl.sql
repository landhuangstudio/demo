-- postgresql

DROP TABLE test_source;

CREATE TABLE test_source (
  "id" int4 NOT NULL,
  "name" varchar(50) NOT NULL,
  "py_code" varchar(50) ,
  "seq_no" int4 NOT NULL,
  "description" varchar(200) ,
  CONSTRAINT "test_source_key" PRIMARY KEY ("id")
)
;

INSERT INTO test_source("id", "name", "py_code", "seq_no", "description") VALUES (1, '2', '3', 4, '5');
UPDATE test_source SET "id" = 1, "name" = '2', "description" = '55' WHERE "id" = 1;
DELETE FROM test_source WHERE "id" = 1;

-- clickhouse

DROP TABLE test_source;

CREATE TABLE test_source
(
  `id`            Int8,
  `name`          String,
  `py_code`       String,
  `seq_no`        Int8,
  `description`   String
)
ENGINE = MergeTree
ORDER BY (name,py_code,seq_no,description);
