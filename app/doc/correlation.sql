-- see if there is a correlation between platforms
select 
  @ax := avg(is_doximity_user_active)
, @ay := avg(is_friendly_vendor_user_active)
, @div := (stddev_samp(is_doximity_user_active) * stddev_samp(is_friendly_vendor_user_active))
from friendly_vendor_match
where classification_match + location_match + specialty_match = 3;

select sum( ( is_doximity_user_active - @ax ) * (is_friendly_vendor_user_active - @ay) ) / ((count(is_doximity_user_active) -1) * @div) as corr
from friendly_vendor_match
where classification_match + location_match + specialty_match = 3;

/*
+------------------------+
| corr                   |
+------------------------+
| -0.0033726835749660613 |
+------------------------+
*/

-- nope!
