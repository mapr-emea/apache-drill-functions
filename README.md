# apache-drill-functions


# MASK EXAMPLE


0: jdbc:drill:> select  account_id, amount, transaction_id  from dfs.`/frdo/gess/json/demo_gess_sink.json` where account_id LIKE 'a61%' and amount > 50 order by transaction_id limit 20;


+-------------+---------+---------------------------------------+
| account_id  | amount  |            transaction_id             |
+-------------+---------+---------------------------------------+
| a612        | 300     | 000105c2-0df7-11e5-b970-0a96a0cdd607  |
| a613        | 300     | 0001809c-0df7-11e5-b970-0a96a0cdd607  |
| a615        | 200     | 00029dec-0df7-11e5-b970-0a96a0cdd607  |



0: jdbc:drill:> select  account_id, amount, mask_nullable(cast(transaction_id as varchar(100)),'-','!','*') as transaction_id from dfs.`/frdo/gess/json/demo_gess_sink.json` where account_id LIKE 'a61%' and amount > 50 order by transaction_id limit 20;


+-------------+---------+---------------------------------------+
| account_id  | amount  |            transaction_id             |
+-------------+---------+---------------------------------------+
| a612        | 300     | 000105c2!****!****!****!************  |
| a613        | 300     | 0001809c!****!****!****!************  |
| a615        | 200     | 00029dec!****!****!****!************  |
