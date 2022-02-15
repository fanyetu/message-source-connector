create database `zhn_test` character set utf8 collate utf8_general_ci;

-- auto-generated definition
create table t_message
(
    id           int auto_increment
        primary key,
    message_id   varchar(255)           not null,
    target_topic varchar(255)           null,
    source_topic varchar(255)           not null,
    content      varchar(2000)          not null,
    create_time  datetime               not null,
    instance_key varchar(255)           null,
    process_flag varchar(2) default '0' not null,
    message_type varchar(20)            null,
    constraint t_message_message_id_uindex
        unique (message_id)
);

-- auto-generated definition
create table t_received_message
(
    id           int auto_increment
        primary key,
    message_id   varchar(255)  not null,
    topic        varchar(255)  not null,
    content      varchar(2000) not null,
    receive_time datetime      not null,
    instance_key varchar(255)  not null,
    constraint t_received_message_pk
        unique (message_id, instance_key)
);

