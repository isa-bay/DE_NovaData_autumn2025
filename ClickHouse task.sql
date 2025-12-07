-- создаем таблицу с сырыми логами событий (TTL 30 дней)
create table user_events
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
engine = MergeTree()
order by (event_time, user_id)
TTL event_time + interval 30 day;

-- создаем таблицу агрегатов (TTL 180 дней)
create table user_events_agg
(
    event_date Date,
    event_type String,
    unique_users AggregateFunction(uniq, UInt32),
    total_spent AggregateFunction(sum, UInt32),
    total_actions AggregateFunction(count, UInt32)
)
engine = AggregatingMergeTree()
order by (event_date, event_type)
TTL event_date + interval 180 day;

-- создаем MV
create materialized view user_events_mv to user_events_agg
as select
    toDate(event_time) as event_date,
    event_type,
    uniqState(user_id) as unique_users,
    sumState(points_spent) as total_spent,
    countState() as total_actions
from user_events
group by event_date, event_type;

INSERT INTO user_events VALUES
-- События 10 дней назад
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),

-- События 7 дней назад
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),

-- События 5 дней назад
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),

-- События 3 дня назад
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),

-- События вчера
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),

-- События сегодня
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- расчет retention (7-дневный)
with cohort as (
    -- когорты за каждый день
    select 
        user_id,
        toDate(MIN(event_time)) as cohort_date
    from user_events
    group by user_id
),
activity as (
    -- активность пользователей в течение 7 дней после когорты
    select 
        c.cohort_date,
        c.user_id,
        toDate(ue.event_time) as activity_date
    from cohort c
    left join user_events ue on c.user_id = ue.user_id
        and toDate(ue.event_time) between c.cohort_date + 1 and c.cohort_date + 7
    group by c.cohort_date, c.user_id, toDate(ue.event_time)
),
retention_stats as (
    -- расчет статистики удержания
    SELECT 
        cohort_date,
        count(distinct user_id) as total_users_day_0,
        count(distinct case when activity_date is not null then user_id end) as returned_in_7_days,
        returned_in_7_days / total_users_day_0 * 100 as retention_7d_percent
    from (
        select 
            c.cohort_date,
            c.user_id,
            MAX(a.activity_date) as activity_date
        FROM cohort c
        left join activity a on c.cohort_date = a.cohort_date and c.user_id = a.user_id
        group by c.cohort_date, c.user_id
    )
    group by cohort_date
)
select 
    format('{}|{}|{:.2f}%', 
        total_users_day_0, 
        returned_in_7_days, 
        retention_7d_percent
    ) as retention_metric
from retention_stats
order by cohort_date;

-- запрос на быструю аналитику по дням
select 
    event_date,
    event_type,
    uniqMerge(unique_users) as unique_users,
    sumMerge(total_spent) as total_spent,
    countMerge(total_actions) as total_actions
from user_events_agg
where event_date >= today() - interval 7 day
group by event_date, event_type
group by event_date desc, event_type;
