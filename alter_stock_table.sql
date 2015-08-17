use chinastock;

set @tbl_name := 'sh600030';

set @s0 := concat('alter table ', @tbl_name);

set @s1 := concat(@s0, "\nchange column `date` `date` date,");
set @s1 := concat(@s1, "\nchange column `open` `open` decimal(6,2) not null,");
set @s1 := concat(@s1, "\nchange column `high` `high` decimal(6,2) not null,");
set @s1 := concat(@s1, "\nchange column `low` `low` decimal(6,2) not null,");
set @s1 := concat(@s1, "\nchange column `close` `close` decimal(6j,2) not null,");
set @s1 := concat(@s1, "\ndrop column `price_change`,");
set @s1 := concat(@s1, "\ndrop column `p_change`,");
set @s1 := concat(@s1, "\ndrop column `ma5`,");
set @s1 := concat(@s1, "\ndrop column `ma10`,");
set @s1 := concat(@s1, "\ndrop column `ma20`,");
set @s1 := concat(@s1, "\ndrop column `v_ma5`,");
set @s1 := concat(@s1, "\ndrop column `v_ma10`,");
set @s1 := concat(@s1, "\ndrop column `v_ma20`;");

prepare alter_index1 from @s1;


set @s2 := concat('update ', @tbl_name);
set @s2 := concat(@s2,"\nset `volume` = ceil(`volume` *100);");

prepare update_index from @s2;


set @s3 := concat(@s0, "\nchange `volume` `volume` bigint not null comment 'volume in shares (not in million shares)';");

prepare alter_index2 from @s3;


execute alter_index1;
execute update_index;
execute alter_index2;


deallocate prepare alter_index1;
deallocate prepare update_index;
deallocate prepare alter_index2;
