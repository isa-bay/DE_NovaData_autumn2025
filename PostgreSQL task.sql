-- создаем таблицы пользователей (хранит данные о пользователях)
create table users(
    id serial primary key,
    name text,
    email text,
    role text,
    updated_at timestamp default current_timestamp
);

-- создаем таблицы аудита для отслеживания изменений (хранит всю историю изменений пользовательских данных)
create table users_audit (
    id serial primary key,
    user_id int,
    changed_at timestamp default current_timestamp,
    changed_by text,
    field_changed text,
    old_value text,
    new_value text
);

-- создаем расширение pg_cron для планирования задач
create extension if not exists pg_cron;

-- триггерная функция, вызываемая при обновлениия таблицы users
create or replace function log_user_changes()
returns trigger as $$
begin
    -- вставка данных в таблицу аудита при изменениях
    insert into users_audit (user_id, changed_by, field_changed, old_value, new_value)
    select 
        old.id, 
        current_user,
        case 
            when old.name is distinct from new.name then 'name'
            when old.email is distinct from new.email then 'email'
            when old.role is distinct from new.role then 'role'
        end,
        case 
            when old.name is distinct from new.name then old.name
            when old.email is distinct from new.email then old.email
            when old.role is distinct from new.role then old.role
        end,
        case 
            when old.name is distinct from new.name then new.name
            when old.email is distinct from new.email then new.email
            when old.role is distinct from new.role then new.role
        end
    where 
        old.name is distinct from new.name or
        old.email is distinct from new.email or
        old.role is distinct from new.role;
    
    new.updated_at = current_timestamp;
    return new;
end;
$$ language plpgsql;

-- создаем триггер, срабатывающий перед обновлением таблицы users
create trigger user_audit_trigger
    before update on users
    for each row execute function log_user_changes();

-- функция создаетс csv файл с данными об изменениях за предыдущий день
create or replace function export_yesterday_audit()
returns void as $$
begin
    execute format(
        'copy ( 
            select user_id, changed_at, changed_by, field_changed, old_value, new_value
            from users_audit
            where changed_at >= current_date - 1 and changed_at < current_date
        ) to %L with csv header', 
        '/tmp/users_audit_export_' || to_char(current_date - 1, 'yyyy-mm-dd') || '.csv'
    );
end;
$$ language plpgsql;

-- задача на расписание на каждый день в 03:00
select cron.schedule(
    'daily-audit-export',
    '0 3 * * *',
    'select export_yesterday_audit();'
);
