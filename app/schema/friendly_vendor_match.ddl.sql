-- friendly_vendor_match.ddl.sql

use data_engineer;

select current_timestamp;

\s

show table status like 'friendly_vendor_match';

-- drop table friendly_vendor_match;

create table friendly_vendor_match (
    friendly_vendor_match_id        bigint unsigned not null auto_increment primary key,
    doximity_user_id                   int unsigned not null,
    friendly_vendor_user_id            int unsigned not null,
    is_doximity_user_active        tinyint unsigned not null,
    is_friendly_vendor_user_active tinyint unsigned not null,
    classification_match           tinyint unsigned not null,
    location_match                 tinyint unsigned not null,
    specialty_match                tinyint unsigned not null,
    report_date                       date not null,
    doximity_last_active_date         date not null,
    friendly_vendor_last_active_date  date not null,
    _worker_id                     tinyint unsigned not null,
    _friendly_vendor_page              int unsigned not null,
    _friendly_vendor_row               int unsigned not null,
    _create_time                 timestamp not null default current_timestamp,
    index(report_date, _worker_id),
    unique(report_date, doximity_user_id, friendly_vendor_user_id)
);

show table status like 'friendly_vendor_match';

select current_timestamp;

