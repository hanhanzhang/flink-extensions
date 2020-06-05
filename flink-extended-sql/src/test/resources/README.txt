code1 对应SQL :

  INSERT INTO t2
  SELECT
     uname, sex, age, action, `timestamp`
  FROM
     t1
  WHERE
     sex = '男' and age >= 22


code2 对应SQL :

  INSERT INTO t2
  SELECT
     uname, sex, age, action, `timestamp`
  FROM
     t1
  WHERE
     sex = '女' and age >= 22


code3 对应SQL :

  INSERT INTO t2
  SELECT
     uname, sex, age, action, `timestamp`
  FROM
     t1
  WHERE
     sex = '女'

