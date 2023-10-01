CREATE TABLE t_ItemBay(
  itemId INT,
  cnt INT
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'idx_bay'
);

CREATE TABLE t_ItemShow(
  itemId INT,
  cnt INT
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'idx_show'
);