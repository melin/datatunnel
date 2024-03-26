INSERT INTO lucky_dw_international.t_vision_shop_sale_info_cyber (`dept_id`,`dt`,`monthly`,`shop_id`,`shop_model`,`coop_brand_code`,`coop_pattern_code`,`shop_level`,`country_code`,`administrative_area_mid`,`locality_mid`,`sublocality_mid`,`currency_code`,`order_cnt`,`take_out_order_cnt`,`take_self_order_cnt`,`take_out_foodpanda_order_cnt`,`take_out_grad_order_cnt`,`take_out_luckin_order_cnt`,`cmdty_sold_cnt`,`make_cmdty_sold_cnt`,`make_cf_cmdty_sold_cnt`,`make_uncf_cmdty_sold_cnt`,`pur_cmdty_sold_cnt`,`pur_food_cmdty_sold_cnt`,`pp_cmdty_sold_cnt`,`bb_cmdty_sold_cnt`,`free_cmdty_sold_cnt`,`luckintea_cmdty_sold_cnt`,`exfreezo_cmdty_sold_cnt`,`classic_cmdty_sold_cnt`,`luckinnut_cmdty_sold_cnt`,`healthfood_cmdty_sold_cnt`,`other_cmdty_sold_cnt`,`take_out_cmdty_sold_cnt`,`take_self_cmdty_sold_cnt`,`new_user_cmdty_sold_cnt`,`old_user_cmdty_sold_cnt`,`charge_cmdty_sold_cnt`,`cmdty_gmv`,`take_out_cmdty_gmv`,`take_self_cmdty_gmv`,`make_cmdty_gmv`,`pur_cmdty_gmv`,`charge_cmdty_gmv`,`make_charge_cmdty_gmv`,`pur_charge_cmdty_gmv`,`cmdty_real_income`,`self_sup_cmdty_real_income`,`out_sup_cmdty_real_income`,`make_cmdty_real_income`,`pur_cmdty_real_income`,`exp_real_income`,`mem_real_income`,`charge_cmdty_real_income`,`make_charge_cmdty_real_income`,`pur_charge_cmdty_real_income`,`make_cf_cmdty_real_income`,`luckintea_cmdty_real_income`,`exfreezo_cmdty_real_income`,`luckinnut_cmdty_real_income`,`healthfood_cmdty_real_income`,`valid_mem_cnt`,`self_service_order_cnt`,`self_service_cmdty_gmv`,`self_service_real_income`,`self_service_cmdty_cnt`,`self_service_machine_cnt`)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
 `monthly` = VALUES(`monthly`),
 `shop_id` = VALUES(`shop_id`),
 `shop_model` = VALUES(`shop_model`),
 `coop_brand_code` = VALUES(`coop_brand_code`),
 `coop_pattern_code` = VALUES(`coop_pattern_code`),

 `shop_level` = VALUES(`shop_level`),

 `country_code` = VALUES(`country_code`),

 `administrative_area_mid` = VALUES(`administrative_area_mid`),

 `locality_mid` = VALUES(`locality_mid`),

 `sublocality_mid` = VALUES(`sublocality_mid`),

 `order_cnt` = VALUES(`order_cnt`),

 `take_out_order_cnt` = VALUES(`take_out_order_cnt`),

 `take_self_order_cnt` = VALUES(`take_self_order_cnt`),

 `take_out_foodpanda_order_cnt` = VALUES(`take_out_foodpanda_order_cnt`),

 `take_out_grad_order_cnt` = VALUES(`take_out_grad_order_cnt`),

 `take_out_luckin_order_cnt` = VALUES(`take_out_luckin_order_cnt`),

 `cmdty_sold_cnt` = VALUES(`cmdty_sold_cnt`),

 `make_cmdty_sold_cnt` = VALUES(`make_cmdty_sold_cnt`),

 `make_cf_cmdty_sold_cnt` = VALUES(`make_cf_cmdty_sold_cnt`),

 `make_uncf_cmdty_sold_cnt` = VALUES(`make_uncf_cmdty_sold_cnt`),

 `pur_cmdty_sold_cnt` = VALUES(`pur_cmdty_sold_cnt`),

 `pur_food_cmdty_sold_cnt` = VALUES(`pur_food_cmdty_sold_cnt`),

 `pp_cmdty_sold_cnt` = VALUES(`pp_cmdty_sold_cnt`),

 `bb_cmdty_sold_cnt` = VALUES(`bb_cmdty_sold_cnt`),

 `free_cmdty_sold_cnt` = VALUES(`free_cmdty_sold_cnt`),

 `luckintea_cmdty_sold_cnt` = VALUES(`luckintea_cmdty_sold_cnt`),

 `exfreezo_cmdty_sold_cnt` = VALUES(`exfreezo_cmdty_sold_cnt`),

 `classic_cmdty_sold_cnt` = VALUES(`classic_cmdty_sold_cnt`),

 `luckinnut_cmdty_sold_cnt` = VALUES(`luckinnut_cmdty_sold_cnt`),

 `healthfood_cmdty_sold_cnt` = VALUES(`healthfood_cmdty_sold_cnt`),

 `other_cmdty_sold_cnt` = VALUES(`other_cmdty_sold_cnt`),

 `take_out_cmdty_sold_cnt` = VALUES(`take_out_cmdty_sold_cnt`),

 `take_self_cmdty_sold_cnt` = VALUES(`take_self_cmdty_sold_cnt`),

 `new_user_cmdty_sold_cnt` = VALUES(`new_user_cmdty_sold_cnt`),

 `old_user_cmdty_sold_cnt` = VALUES(`old_user_cmdty_sold_cnt`),

 `charge_cmdty_sold_cnt` = VALUES(`charge_cmdty_sold_cnt`),

 `cmdty_gmv` = VALUES(`cmdty_gmv`),

 `take_out_cmdty_gmv` = VALUES(`take_out_cmdty_gmv`),

 `take_self_cmdty_gmv` = VALUES(`take_self_cmdty_gmv`),

 `make_cmdty_gmv` = VALUES(`make_cmdty_gmv`),

 `pur_cmdty_gmv` = VALUES(`pur_cmdty_gmv`),

 `charge_cmdty_gmv` = VALUES(`charge_cmdty_gmv`),

 `make_charge_cmdty_gmv` = VALUES(`make_charge_cmdty_gmv`),

 `pur_charge_cmdty_gmv` = VALUES(`pur_charge_cmdty_gmv`),

 `cmdty_real_income` = VALUES(`cmdty_real_income`),

 `self_sup_cmdty_real_income` = VALUES(`self_sup_cmdty_real_income`),

 `out_sup_cmdty_real_income` = VALUES(`out_sup_cmdty_real_income`),

 `make_cmdty_real_income` = VALUES(`make_cmdty_real_income`),

 `pur_cmdty_real_income` = VALUES(`pur_cmdty_real_income`),

 `exp_real_income` = VALUES(`exp_real_income`),

 `mem_real_income` = VALUES(`mem_real_income`),

 `charge_cmdty_real_income` = VALUES(`charge_cmdty_real_income`),

 `make_charge_cmdty_real_income` = VALUES(`make_charge_cmdty_real_income`),

 `pur_charge_cmdty_real_income` = VALUES(`pur_charge_cmdty_real_income`),

 `make_cf_cmdty_real_income` = VALUES(`make_cf_cmdty_real_income`),

 `luckintea_cmdty_real_income` = VALUES(`luckintea_cmdty_real_income`),

 `exfreezo_cmdty_real_income` = VALUES(`exfreezo_cmdty_real_income`),

 `luckinnut_cmdty_real_income` = VALUES(`luckinnut_cmdty_real_income`),

 `healthfood_cmdty_real_income` = VALUES(`healthfood_cmdty_real_income`),

 `valid_mem_cnt` = VALUES(`valid_mem_cnt`),

 `self_service_order_cnt` = VALUES(`self_service_order_cnt`),

 `self_service_cmdty_gmv` = VALUES(`self_service_cmdty_gmv`),

 `self_service_real_income` = VALUES(`self_service_real_income`),

 `self_service_cmdty_cnt` = VALUES(`self_service_cmdty_cnt`),

 `self_service_machine_cnt` = VALUES(`self_service_machine_cnt`)