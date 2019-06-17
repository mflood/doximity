-- 
-- grants.sql
-- 
-- Sets up a user with same credentials as the source DB
-- 
GRANT USAGE ON *.* TO 'de_candidate'@'%' IDENTIFIED BY PASSWORD '*3F486E34372BBBE2A77C2AD3168BFE88ACFA076F';
GRANT SELECT ON `data_engineer`.`user` TO 'de_candidate'@'%';
GRANT insert, delete, select on data_engineer.* to 'de_candidate'@'%';  

