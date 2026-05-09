-- 1. Справочные таблицы (словари)

CREATE TABLE public.customers (
    id SERIAL PRIMARY KEY,
    name character varying(300) NOT NULL,
    inn character varying(12) UNIQUE,
    contacts text,
    address text,
    is_active boolean DEFAULT true
);

CREATE TABLE public.suppliers (
    id SERIAL PRIMARY KEY,
    name character varying(300) NOT NULL,
    inn character varying(12) UNIQUE,
    contacts text,
    is_active boolean DEFAULT true
);

CREATE TABLE public.warehouses (
    id SERIAL PRIMARY KEY,
    name character varying(200) NOT NULL,
    address text,
    is_active boolean DEFAULT true
);

CREATE TABLE public.drivers (
    id SERIAL PRIMARY KEY,
    full_name character varying(200) NOT NULL,
    license_number character varying(50) UNIQUE,
    phone character varying(50),
    is_active boolean DEFAULT true
);

CREATE TABLE public.vehicles (
    id SERIAL PRIMARY KEY,
    plate_number character varying(20) NOT NULL UNIQUE,
    model character varying(200),
    is_active boolean DEFAULT true
);

CREATE TABLE public.products (
    id SERIAL PRIMARY KEY,
    sku character varying(50) NOT NULL UNIQUE,
    name character varying(500) NOT NULL,
    unit character varying(20) NOT NULL,
    cost_price numeric(15,2) DEFAULT 0,
    selling_price numeric(15,2) DEFAULT 0,
    is_active boolean DEFAULT true
);

-- 2. Таблицы документов (шапки и строки)

CREATE TABLE public.purchase_headers (
    id SERIAL PRIMARY KEY,
    number character varying(50) NOT NULL UNIQUE,
    date date DEFAULT CURRENT_DATE NOT NULL,
    supplier_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    total_amount numeric(15,2) DEFAULT 0,
    comment text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.purchase_items (
    id SERIAL PRIMARY KEY,
    purchase_id integer NOT NULL,
    product_id integer NOT NULL,
    quantity numeric(15,3) NOT NULL,
    purchase_price numeric(15,2) NOT NULL,
    amount numeric(15,2) GENERATED ALWAYS AS (quantity * purchase_price) STORED,
    CONSTRAINT purchase_items_quantity_check CHECK (quantity > 0),
    CONSTRAINT purchase_items_purchase_price_check CHECK (purchase_price >= 0)
);

CREATE TABLE public.sale_headers (
    id SERIAL PRIMARY KEY,
    number character varying(50) NOT NULL UNIQUE,
    date date DEFAULT CURRENT_DATE NOT NULL,
    customer_id integer NOT NULL,
    driver_id integer,
    vehicle_id integer,
    warehouse_id integer NOT NULL,
    total_amount numeric(15,2) DEFAULT 0,
    comment text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.sale_items (
    id SERIAL PRIMARY KEY,
    sale_id integer NOT NULL,
    product_id integer NOT NULL,
    quantity numeric(15,3) NOT NULL,
    selling_price numeric(15,2) NOT NULL,
    amount numeric(15,2) GENERATED ALWAYS AS (quantity * selling_price) STORED,
    CONSTRAINT sale_items_quantity_check CHECK (quantity > 0),
    CONSTRAINT sale_items_selling_price_check CHECK (selling_price >= 0)
);

-- 3. Таблицы учёта остатков и движений

CREATE TABLE public.stock_balances (
    id SERIAL PRIMARY KEY,
    product_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    quantity numeric(15,3) DEFAULT 0 NOT NULL,
    CONSTRAINT stock_balances_quantity_check CHECK (quantity >= 0),
    CONSTRAINT stock_balances_unique UNIQUE (product_id, warehouse_id)
);

CREATE TABLE public.stock_movements (
    id SERIAL PRIMARY KEY,
    movement_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    document_type character varying(20) NOT NULL,
    document_id integer NOT NULL,
    operation_type character varying(10) NOT NULL,
    product_id integer NOT NULL,
    warehouse_id integer NOT NULL,
    quantity_before numeric(15,3) NOT NULL,
    quantity_change numeric(15,3) NOT NULL,
    quantity_after numeric(15,3) NOT NULL,
    comment text,
    created_by character varying(100),
    CONSTRAINT stock_movements_document_type_check 
        CHECK (document_type IN ('PURCHASE', 'SALE', 'ADJUSTMENT')),
    CONSTRAINT stock_movements_operation_type_check 
        CHECK (operation_type IN ('INCOME', 'OUTCOME'))
);


-- 4. Индексы для производительности

CREATE INDEX idx_purchase_headers_supplier_id ON public.purchase_headers(supplier_id);
CREATE INDEX idx_purchase_headers_warehouse_id ON public.purchase_headers(warehouse_id);
CREATE INDEX idx_purchase_headers_date ON public.purchase_headers(date);

CREATE INDEX idx_purchase_items_purchase_id ON public.purchase_items(purchase_id);
CREATE INDEX idx_purchase_items_product_id ON public.purchase_items(product_id);

CREATE INDEX idx_sale_headers_customer_id ON public.sale_headers(customer_id);
CREATE INDEX idx_sale_headers_driver_id ON public.sale_headers(driver_id);
CREATE INDEX idx_sale_headers_vehicle_id ON public.sale_headers(vehicle_id);
CREATE INDEX idx_sale_headers_warehouse_id ON public.sale_headers(warehouse_id);
CREATE INDEX idx_sale_headers_date ON public.sale_headers(date);

CREATE INDEX idx_sale_items_sale_id ON public.sale_items(sale_id);
CREATE INDEX idx_sale_items_product_id ON public.sale_items(product_id);

CREATE INDEX idx_stock_balances_product_warehouse ON public.stock_balances(product_id, warehouse_id);
CREATE INDEX idx_stock_movements_product_id ON public.stock_movements(product_id);
CREATE INDEX idx_stock_movements_warehouse_id ON public.stock_movements(warehouse_id);
CREATE INDEX idx_stock_movements_document ON public.stock_movements(document_type, document_id);
CREATE INDEX idx_stock_movements_date ON public.stock_movements(movement_date);

-- 5. Внешние ключи

ALTER TABLE ONLY public.purchase_headers
    ADD CONSTRAINT purchase_headers_supplier_id_fkey 
    FOREIGN KEY (supplier_id) REFERENCES public.suppliers(id);

ALTER TABLE ONLY public.purchase_headers
    ADD CONSTRAINT purchase_headers_warehouse_id_fkey 
    FOREIGN KEY (warehouse_id) REFERENCES public.warehouses(id);

ALTER TABLE ONLY public.purchase_items
    ADD CONSTRAINT purchase_items_purchase_id_fkey 
    FOREIGN KEY (purchase_id) REFERENCES public.purchase_headers(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.purchase_items
    ADD CONSTRAINT purchase_items_product_id_fkey 
    FOREIGN KEY (product_id) REFERENCES public.products(id);

ALTER TABLE ONLY public.sale_headers
    ADD CONSTRAINT sale_headers_customer_id_fkey 
    FOREIGN KEY (customer_id) REFERENCES public.customers(id);

ALTER TABLE ONLY public.sale_headers
    ADD CONSTRAINT sale_headers_driver_id_fkey 
    FOREIGN KEY (driver_id) REFERENCES public.drivers(id);

ALTER TABLE ONLY public.sale_headers
    ADD CONSTRAINT sale_headers_vehicle_id_fkey 
    FOREIGN KEY (vehicle_id) REFERENCES public.vehicles(id);

ALTER TABLE ONLY public.sale_headers
    ADD CONSTRAINT sale_headers_warehouse_id_fkey 
    FOREIGN KEY (warehouse_id) REFERENCES public.warehouses(id);

ALTER TABLE ONLY public.sale_items
    ADD CONSTRAINT sale_items_sale_id_fkey 
    FOREIGN KEY (sale_id) REFERENCES public.sale_headers(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.sale_items
    ADD CONSTRAINT sale_items_product_id_fkey 
    FOREIGN KEY (product_id) REFERENCES public.products(id);

ALTER TABLE ONLY public.stock_balances
    ADD CONSTRAINT stock_balances_product_id_fkey 
    FOREIGN KEY (product_id) REFERENCES public.products(id);

ALTER TABLE ONLY public.stock_balances
    ADD CONSTRAINT stock_balances_warehouse_id_fkey 
    FOREIGN KEY (warehouse_id) REFERENCES public.warehouses(id);

ALTER TABLE ONLY public.stock_movements
    ADD CONSTRAINT stock_movements_product_id_fkey 
    FOREIGN KEY (product_id) REFERENCES public.products(id);

ALTER TABLE ONLY public.stock_movements
    ADD CONSTRAINT stock_movements_warehouse_id_fkey 
    FOREIGN KEY (warehouse_id) REFERENCES public.warehouses(id);