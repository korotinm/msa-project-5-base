CREATE DATABASE shop;
\c shop

CREATE TABLE orders (
    id          bigint PRIMARY KEY,
    user_id     int NOT NULL,
    amount      numeric(10, 2) NOT NULL,
    status      text NOT NULL,
    created_at  timestamp NOT NULL
);

-- ~1 млн заказов, как в условии задания
INSERT INTO orders
SELECT g,
       (random() * 50000)::int,
       (random() * 1000)::numeric(10, 2),
       (ARRAY['paid', 'paid', 'paid', 'cancelled', 'refunded'])[1 + (random() * 4)::int],
       now() - (random() * 30) * interval '1 day'
FROM generate_series(1, 1000000) g;

CREATE TABLE delivery_report (
    run_date     date PRIMARY KEY,
    orders_total bigint,
    failed_share numeric(5, 2),
    revenue      numeric(14, 2)
);
