# redis-active-record
## 背景

- 某些云Redis数据库不能支持 `EVAL` 命令，导致lua脚本无法执行，redis ActiveRecord 的查询无法使用。

## 解决方案

- v1.0：利用redis事务批量执行查询，返回数据后由PHP处理格式。
- v2：根据官方文档，管道技术较快于事务，抓包分析管道的tcp通讯显著较事务的少，于是此次利用redis的管道技术，将查询命令全部发送到redis，返回数据后由PHP处理格式。
