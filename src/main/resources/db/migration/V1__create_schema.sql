create domain public_key_type as binary(32) not null;
create domain address_type as binary(64) not null;
create domain digest_type as binary(64) not null;
create domain signature_type as binary(64) not null;
create domain amount_type as bigint not null check (value >= 0);
create domain height_type as int not null check (value > 0);

create table blocks (
    height height_type primary key,
    block_id signature_type,
    block_timestamp timestamp not null,
    generator_address address_type,
    block_data_bytes binary not null
);

create table waves_balances (
    address address_type,
    regular_balance amount_type,
    effective_balance amount_type,
    height height_type references blocks(height) on delete cascade,
    primary key (address, height)
);

create index regular_balance_index on waves_balances(regular_balance);

create table asset_info (
    asset_id digest_type primary key,
    issuer public_key_type,
    decimals tinyint not null,
    name binary not null,
    description binary not null,
    height height_type references blocks(height) on delete cascade,
);

create table asset_quantity (
    asset_id digest_type references asset_info(asset_id) on delete cascade,
    quantity amount_type,
    reissuable boolean not null,
    height height_type references blocks(height) on delete cascade,
    primary key (asset_id, height)
);

create table asset_balances (
    address public_key_type,
    asset_id digest_type references asset_info(asset_id) on delete cascade,
    balance amount_type,
    height height_type references blocks(height) on delete cascade,
    primary key (address, asset_id, height)
);

create table lease_info (
    lease_id digest_type primary key,
    sender public_key_type,
    recipient public_key_type,
    amount amount_type,
    height height_type references blocks(height) on delete cascade
);

create table lease_status (
    lease_id digest_type references lease_info(lease_id) on delete cascade,
    active boolean not null,
    height height_type references blocks(height) on delete cascade
);

create table lease_balances (
    address address_type,
    lease_in bigint not null,
    lease_out bigint not null,
    height height_type references blocks(height) on delete cascade,

    constraint non_negative_lease_in check (height < 462000 or lease_in >= 0),
    constraint non_negative_lease_out check (height < 462000 or lease_out >= 0),

    primary key (address, height)
);

create table filled_quantity (
    order_id digest_type,
    filled_quantity amount_type,
    fee amount_type,
    height height_type references blocks(height) on delete cascade,

    primary key (order_id, height)
);

create table transaction_offsets (
    tx_id digest_type,
    signature signature_type,
    start_offset int not null,
    height height_type references blocks(height) on delete cascade,

    primary key (tx_id, signature)
);

create table address_transaction_ids (
    address address_type,
    tx_id digest_type,
    signature signature_type,
    height height_type references blocks(height) on delete cascade,

    foreign key (tx_id, signature) references transaction_offsets(tx_id, signature) on delete cascade
);

create table payment_transactions (
    tx_hash digest_type primary key,
    height height_type references blocks(height) on delete cascade
);
