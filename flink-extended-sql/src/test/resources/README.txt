code1 对应SQL :

  INSERT INTO t2
    SELECT
       uname, sex, age, action, `timestamp`
    FROM
       t1
    WHERE
       sex = '女'


code2 对应SQL :

  INSERT INTO t2
  SELECT
     uname, sex, age, action, `timestamp`
  FROM
     t1
  WHERE
     sex = '男' and action = '登录'


code3 对应SQL :

  INSERT INTO t2
    SELECT
       uname, sex, age, action, `timestamp`
    FROM
       t1
    WHERE
       sex = '女' and action = '支付'


