create schema demo_load;
create table demo_load.order_data (id serial primary key, json_data jsonb);
create table demo_load.staging_orders_flat (
	order_id text,
	customer_id text,
	order_date text,
	amount numeric,
	status text,
	product_name text,
	customer_name text,
	customer_email text,
	customer_city text,
	valid_from date,
	valid_to date,
	is_current boolean,
	processed_at date
	);

create table demo_load.dim_clients_scd2 (
	order_id text,
	customer_id text,
	order_date text,
	amount numeric,
	status text,
	product_name text,
	customer_name text,
	customer_email text,
	customer_city text,
	valid_from date,
	valid_to date,
	is_current boolean,
	processed_at date
	);

CREATE TABLE demo_load.mart_daily_sales (
    report_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_amount DECIMAL(10,2),
    avg_order_amount DECIMAL(10,2),
    unique_customers INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
	
	
INSERT INTO demo_load.orders_data (json_data) VALUES (
    '{
    "order_id": "ORD-2024-005",
    "customer_id": "CUST-1001",
    "order_date": "2024-01-15",
    "amount": 1500.50,
    "status": "completed",
    "product_name": "Планшет",
    "customer": {
        "name": "Максим Иван Иванович",
        "email": "ivanovvvvvvv@example.com",
        "city": "Москва"
    }
    }'::jsonb
);

INSERT INTO demo_load.orders_data (json_data) VALUES (
    '{
    "order_id": "ORD-2024-006",
    "customer_id": "CUST-1002",
    "order_date": "2024-01-15",
    "amount": 3000.00,
    "status": "completed",
    "product_name": "Ноутбук",
    "customer": {
        "name": "Илон Маск",
        "email": "I_Mask@example.com",
        "city": "Претория"
    }
    }'::jsonb
);
