# apache-drill-functions

MASK EXAMPLE



0: jdbc:drill:> select  account_id, amount, transaction_id  from dfs.`/frdo/gess/json/demo_gess_sink.json` where account_id LIKE 'a61%' and amount > 50 order by transaction_id limit 20;
+-------------+---------+---------------------------------------+
| account_id  | amount  |            transaction_id             |
+-------------+---------+---------------------------------------+
| a612        | 300     | 000105c2-0df7-11e5-b970-0a96a0cdd607  |
| a613        | 300     | 0001809c-0df7-11e5-b970-0a96a0cdd607  |
| a615        | 200     | 00029dec-0df7-11e5-b970-0a96a0cdd607  |
| a615        | 200     | 0003ca74-0df6-11e5-b970-0a96a0cdd607  |
| a618        | 300     | 000444a4-0df6-11e5-b970-0a96a0cdd607  |
| a617        | 200     | 0006b372-0df8-11e5-b970-0a96a0cdd607  |
| a618        | 300     | 000c7224-0df5-11e5-b970-0a96a0cdd607  |
| a610        | 100     | 000d85bc-0df8-11e5-b970-0a96a0cdd607  |
| a619        | 200     | 000fd250-0df7-11e5-b970-0a96a0cdd607  |
| a618        | 400     | 0011a3f2-0df5-11e5-b970-0a96a0cdd607  |
| a610        | 300     | 0011f32c-0df8-11e5-b970-0a96a0cdd607  |
| a619        | 200     | 0012b53a-0df5-11e5-b970-0a96a0cdd607  |
| a611        | 400     | 0014aa92-0df6-11e5-b970-0a96a0cdd607  |
| a610        | 300     | 001c7cc6-0df7-11e5-b970-0a96a0cdd607  |
| a611        | 100     | 00200d4a-0df8-11e5-b970-0a96a0cdd607  |
| a616        | 100     | 0021faa8-0df6-11e5-b970-0a96a0cdd607  |
| a617        | 100     | 00232f86-0df6-11e5-b970-0a96a0cdd607  |
| a616        | 300     | 00298170-0df5-11e5-b970-0a96a0cdd607  |
| a611        | 400     | 0029d49e-0df6-11e5-b970-0a96a0cdd607  |
| a616        | 200     | 0029d7f6-0df5-11e5-b970-0a96a0cdd607  |
+-------------+---------+---------------------------------------+
20 rows selected (27,804 seconds)

0: jdbc:drill:> select  account_id, amount,  mask_nullable(cast(transaction_id as varchar(100)),'-','!','*') as transaction_id from dfs.`/frdo/gess/json/demo_gess_sink.json` where account_id LIKE 'a61%' and amount > 50 order by transaction_id limit 20;
+-------------+---------+---------------------------------------+
| account_id  | amount  |            transaction_id             |
+-------------+---------+---------------------------------------+
| a612        | 300     | 000105c2!****!****!****!************  |
| a613        | 300     | 0001809c!****!****!****!************  |
| a615        | 200     | 00029dec!****!****!****!************  |
| a615        | 200     | 0003ca74!****!****!****!************  |
| a618        | 300     | 000444a4!****!****!****!************  |
| a617        | 200     | 0006b372!****!****!****!************  |
| a618        | 300     | 000c7224!****!****!****!************  |
| a610        | 100     | 000d85bc!****!****!****!************  |
| a619        | 200     | 000fd250!****!****!****!************  |
| a618        | 400     | 0011a3f2!****!****!****!************  |
| a610        | 300     | 0011f32c!****!****!****!************  |
| a619        | 200     | 0012b53a!****!****!****!************  |
| a611        | 400     | 0014aa92!****!****!****!************  |
| a610        | 300     | 001c7cc6!****!****!****!************  |
| a611        | 100     | 00200d4a!****!****!****!************  |
| a616        | 100     | 0021faa8!****!****!****!************  |
| a617        | 100     | 00232f86!****!****!****!************  |
| a616        | 300     | 00298170!****!****!****!************  |
| a611        | 400     | 0029d49e!****!****!****!************  |
| a616        | 200     | 0029d7f6!****!****!****!************  |
+-------------+---------+---------------------------------------+
20 rows selected (26,266 seconds)
