#!/usr/bin/env bash
set -euo pipefail

# CREATED with OpenAI GPT5
# Seed many Oracle tables in your existing XE container.

# =========================
# CONFIG (matches your setup.sh)
# =========================
ORACLE_CONTAINER="${ORACLE_CONTAINER:-oracle-test-db}"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-oracle}"
ORACLE_PORT="${ORACLE_PORT:-1521}"
ORACLE_SERVICE="${ORACLE_SERVICE:-XEPDB1}"

SEED_USER="${SEED_USER:-SYSTEM}"
SEED_PASS="${SEED_PASS:-oracle}"

# Scale factors
NUM_EMPLOYEES="${NUM_EMPLOYEES:-50}"
NUM_PROJECTS="${NUM_PROJECTS:-12}"
NUM_DEPARTMENTS="${NUM_DEPARTMENTS:-6}"
NUM_PRODUCTS="${NUM_PRODUCTS:-30}"
NUM_CUSTOMERS="${NUM_CUSTOMERS:-40}"
NUM_ORDERS="${NUM_ORDERS:-120}"
MAX_ITEMS_PER_ORDER="${MAX_ITEMS_PER_ORDER:-5}"

# =========================
# HELPERS
# =========================
oracle_exec_sql () {
  local sql="$1"
  docker exec -i "$ORACLE_CONTAINER" bash -lc "sqlplus -s system/${ORACLE_PASSWORD}@localhost:${ORACLE_PORT}/${ORACLE_SERVICE} <<'SQL'
whenever sqlerror exit failure
set define off
set feedback off verify off heading off pages 0 termout on serveroutput on size 100000
${sql}
exit
SQL"
}

# =========================
# CHECKS
# =========================
if ! docker ps --format '{{.Names}}' | grep -q "^${ORACLE_CONTAINER}\$"; then
  echo "[!] Oracle container '${ORACLE_CONTAINER}' not running. Start your stack first."
  exit 1
fi


echo "[*] Seeding Oracle schema '${SEED_USER}' ..."

# =========================
# CLEAN EXISTING SEED OBJECTS (child-first drops; idempotent)
# =========================
oracle_exec_sql "
DECLARE
  PROCEDURE drop_obj(p_name IN VARCHAR2, p_type IN VARCHAR2) IS
  BEGIN
    BEGIN
      IF p_type = 'TABLE' THEN
        EXECUTE IMMEDIATE 'DROP TABLE '||'${SEED_USER}.'||p_name||' CASCADE CONSTRAINTS';
      ELSIF p_type = 'SEQUENCE' THEN
        EXECUTE IMMEDIATE 'DROP SEQUENCE '||'${SEED_USER}.'||p_name;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        -- -942: table or view does not exist, -2289: sequence does not exist
        IF SQLCODE NOT IN (-942, -2289) THEN RAISE; END IF;
    END;
  END;
BEGIN
  -- Drop tables in child-first order
  drop_obj('order_items',     'TABLE');
  drop_obj('payments',        'TABLE');
  drop_obj('emp_projects',    'TABLE');
  drop_obj('orders',          'TABLE');
  drop_obj('employees',       'TABLE');
  drop_obj('projects',        'TABLE');
  drop_obj('addresses',       'TABLE');
  drop_obj('customers',       'TABLE');
  drop_obj('products',        'TABLE');
  drop_obj('departments',     'TABLE');
  drop_obj('event_log',       'TABLE');
  drop_obj('audit_trail',     'TABLE');

  -- Drop sequences
  drop_obj('departments_seq', 'SEQUENCE');
  drop_obj('employees_seq',   'SEQUENCE');
  drop_obj('projects_seq',    'SEQUENCE');
  drop_obj('customers_seq',   'SEQUENCE');
  drop_obj('products_seq',    'SEQUENCE');
  drop_obj('orders_seq',      'SEQUENCE');
  drop_obj('addresses_seq',   'SEQUENCE');
  drop_obj('payments_seq',    'SEQUENCE');
  drop_obj('event_log_seq',   'SEQUENCE');
  drop_obj('audit_trail_seq', 'SEQUENCE');
END;
/
"

# =========================
# CREATE/GRANT USER (idempotent)
# =========================
oracle_exec_sql "
declare
  v_count number;
begin
  select count(*) into v_count from dba_users where username = upper('${SEED_USER}');
  if v_count = 0 then
    execute immediate 'CREATE USER ${SEED_USER} IDENTIFIED BY ${SEED_PASS} QUOTA UNLIMITED ON USERS';
    execute immediate 'GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE TO ${SEED_USER}';
  end if;
end;
/
"

# =========================
# TABLES + SEQUENCES (idempotent)
# =========================
oracle_exec_sql "
-- Helper to 'create if not exists' table
declare
  procedure create_table(p_sql in varchar2) is
  begin
    begin
      execute immediate p_sql;
    exception when others then
      if sqlcode != -955 then -- ORA-00955: name already used by an existing object
        raise;
      end if;
    end;
  end;
  procedure create_seq(p_name in varchar2, p_start in integer default 1) is
  begin
    begin
      execute immediate 'CREATE SEQUENCE ${SEED_USER}.'||p_name||' START WITH '||p_start||' INCREMENT BY 1 NOCACHE';
    exception when others then
      if sqlcode != -955 then raise; end if;
    end;
  end;
begin
  -- Departments
  create_table(q'[
    CREATE TABLE ${SEED_USER}.departments (
      dept_id     NUMBER PRIMARY KEY,
      name        VARCHAR2(80) NOT NULL,
      location    VARCHAR2(80)
    )
  ]');
  create_seq('departments_seq');

  -- Employees
  create_table(q'[
    CREATE TABLE ${SEED_USER}.employees (
      emp_id      NUMBER PRIMARY KEY,
      dept_id     NUMBER REFERENCES ${SEED_USER}.departments(dept_id),
      first_name  VARCHAR2(50),
      last_name   VARCHAR2(50),
      email       VARCHAR2(120),
      salary      NUMBER(10,2),
      hired_at    DATE
    )
  ]');
  create_seq('employees_seq');

  -- Projects
  create_table(q'[
    CREATE TABLE ${SEED_USER}.projects (
      proj_id     NUMBER PRIMARY KEY,
      dept_id     NUMBER REFERENCES ${SEED_USER}.departments(dept_id),
      name        VARCHAR2(120) NOT NULL,
      start_date  DATE,
      end_date    DATE
    )
  ]');
  create_seq('projects_seq');

  -- Employee→Project assignment (M:N)
  create_table(q'[
    CREATE TABLE ${SEED_USER}.emp_projects (
      emp_id      NUMBER REFERENCES ${SEED_USER}.employees(emp_id),
      proj_id     NUMBER REFERENCES ${SEED_USER}.projects(proj_id),
      role_name   VARCHAR2(60),
      assigned_at DATE,
      PRIMARY KEY(emp_id, proj_id)
    )
  ]');

  -- Customers / Products / Orders
  create_table(q'[
    CREATE TABLE ${SEED_USER}.customers (
      cust_id     NUMBER PRIMARY KEY,
      name        VARCHAR2(120) NOT NULL,
      email       VARCHAR2(160),
      created_at  DATE
    )
  ]');
  create_seq('customers_seq');

  create_table(q'[
    CREATE TABLE ${SEED_USER}.products (
      prod_id     NUMBER PRIMARY KEY,
      name        VARCHAR2(120) NOT NULL,
      sku         VARCHAR2(64) UNIQUE,
      price       NUMBER(10,2),
      active      NUMBER(1) DEFAULT 1
    )
  ]');
  create_seq('products_seq');

  create_table(q'[
    CREATE TABLE ${SEED_USER}.orders (
      order_id    NUMBER PRIMARY KEY,
      cust_id     NUMBER REFERENCES ${SEED_USER}.customers(cust_id),
      ordered_at  DATE,
      status      VARCHAR2(20)
    )
  ]');
  create_seq('orders_seq');

  create_table(q'[
    CREATE TABLE ${SEED_USER}.order_items (
      order_id    NUMBER REFERENCES ${SEED_USER}.orders(order_id),
      line_no     NUMBER,
      prod_id     NUMBER REFERENCES ${SEED_USER}.products(prod_id),
      qty         NUMBER,
      unit_price  NUMBER(10,2),
      PRIMARY KEY(order_id, line_no)
    )
  ]');

  -- Addresses
  create_table(q'[
    CREATE TABLE ${SEED_USER}.addresses (
      addr_id     NUMBER PRIMARY KEY,
      cust_id     NUMBER REFERENCES ${SEED_USER}.customers(cust_id),
      line1       VARCHAR2(120),
      city        VARCHAR2(80),
      region      VARCHAR2(80),
      postal_code VARCHAR2(20),
      country     VARCHAR2(2)
    )
  ]');
  create_seq('addresses_seq');

  -- Payments
  create_table(q'[
    CREATE TABLE ${SEED_USER}.payments (
      pay_id      NUMBER PRIMARY KEY,
      order_id    NUMBER REFERENCES ${SEED_USER}.orders(order_id),
      amount      NUMBER(10,2),
      method      VARCHAR2(20),
      paid_at     DATE
    )
  ]');
  create_seq('payments_seq');

  -- Logs & Audit
  create_table(q'[
    CREATE TABLE ${SEED_USER}.event_log (
      id          NUMBER PRIMARY KEY,
      who         VARCHAR2(80),
      action      VARCHAR2(80),
      created_at  DATE,
      payload     CLOB
    )
  ]');
  create_seq('event_log_seq');

  create_table(q'[
    CREATE TABLE ${SEED_USER}.audit_trail (
      id          NUMBER PRIMARY KEY,
      table_name  VARCHAR2(120),
      pk_value    VARCHAR2(120),
      action      VARCHAR2(20),
      at_time     DATE
    )
  ]');
  create_seq('audit_trail_seq');

end;
/
"

# =========================
# SEED DATA (idempotent-ish)
# =========================
oracle_exec_sql "
declare
  -- helpers
  function rand(n in pls_integer) return pls_integer is
  begin
    return trunc(dbms_random.value(1, n+1));
  end;
begin
  -- Departments
  for i in 1..${NUM_DEPARTMENTS} loop
    begin
      insert into ${SEED_USER}.departments(dept_id, name, location)
      values (${SEED_USER}.departments_seq.nextval,
              'Dept '||i,
              case mod(i,3) when 0 then 'Brisbane' when 1 then 'Sydney' else 'Melbourne' end);
    exception when dup_val_on_index then null; end;
  end loop;
  commit;

  -- Employees
  for i in 1..${NUM_EMPLOYEES} loop
    begin
      insert into ${SEED_USER}.employees(emp_id, dept_id, first_name, last_name, email, salary, hired_at)
      values (${SEED_USER}.employees_seq.nextval,
              mod(i, ${NUM_DEPARTMENTS})+1,  -- simple dept spread (not exact if rerun)
              'First'||i, 'Last'||i,
              'user'||i||'@example.com',
              round(dbms_random.value(60000, 160000), 2),
              trunc(sysdate - dbms_random.value(0, 365*5)));
    exception when dup_val_on_index then null; end;
  end loop;
  commit;

  -- Projects
  for i in 1..${NUM_PROJECTS} loop
    begin
      insert into ${SEED_USER}.projects(proj_id, dept_id, name, start_date, end_date)
      values (${SEED_USER}.projects_seq.nextval,
              mod(i, ${NUM_DEPARTMENTS})+1,
              'Project '||i,
              trunc(sysdate - dbms_random.value(0, 365*2)),
              null);
    exception when dup_val_on_index then null; end;
  end loop;
  commit;

  -- Emp→Proj assignments
  for i in 1..${NUM_EMPLOYEES} loop
    for j in 1..2 loop
      begin
        insert into ${SEED_USER}.emp_projects(emp_id, proj_id, role_name, assigned_at)
        values (i, mod(i+j, ${NUM_PROJECTS})+1,
                case mod(j,3) when 0 then 'Lead' when 1 then 'Dev' else 'QA' end,
                trunc(sysdate - dbms_random.value(0, 365)));
      exception when dup_val_on_index then null; end;
    end loop;
  end loop;
  commit;

  -- Customers
  for i in 1..${NUM_CUSTOMERS} loop
    begin
      insert into ${SEED_USER}.customers(cust_id, name, email, created_at)
      values (${SEED_USER}.customers_seq.nextval,
              'Customer '||i,
              'cust'||i||'@example.com',
              trunc(sysdate - dbms_random.value(0, 365*3)));
    exception when dup_val_on_index then null; end;
  end loop;
  commit;

  -- Addresses
  for i in 1..${NUM_CUSTOMERS} loop
    begin
      insert into ${SEED_USER}.addresses(addr_id, cust_id, line1, city, region, postal_code, country)
      values (${SEED_USER}.addresses_seq.nextval,
              i,
              'Addr line '||i,
              case mod(i,4) when 0 then 'Brisbane' when 1 then 'Sydney' when 2 then 'Perth' else 'Adelaide' end,
              'QLD',
              to_char(4000+mod(i,9000)),
              'AU');
    exception when dup_val_on_index then null; end;
  end loop;
  commit;

  -- Products
  for i in 1..${NUM_PRODUCTS} loop
    begin
      insert into ${SEED_USER}.products(prod_id, name, sku, price, active)
      values (${SEED_USER}.products_seq.nextval,
              'Product '||i,
              'SKU-'||to_char(10000+i),
              round(dbms_random.value(5, 500), 2),
              case when mod(i,10)=0 then 0 else 1 end);
    exception when dup_val_on_index then null; end;
  end loop;
  commit;

  -- Orders & items
  for i in 1..${NUM_ORDERS} loop
    declare
      v_order_id number;
      v_items pls_integer := rand(${MAX_ITEMS_PER_ORDER});
    begin
      begin
        v_order_id := ${SEED_USER}.orders_seq.nextval;
        insert into ${SEED_USER}.orders(order_id, cust_id, ordered_at, status)
        values (v_order_id,
                mod(i, ${NUM_CUSTOMERS})+1,
                trunc(sysdate - dbms_random.value(0, 365)),
                case mod(i,4) when 0 then 'NEW' when 1 then 'PAID' when 2 then 'SHIPPED' else 'CLOSED' end);
      exception when dup_val_on_index then null; end;

      for ln in 1..v_items loop
        declare
          v_prod number := mod(i+ln, ${NUM_PRODUCTS})+1;
          v_qty  number := rand(4);
          v_price number;
        begin
          select price into v_price from ${SEED_USER}.products where prod_id = v_prod;
          begin
            insert into ${SEED_USER}.order_items(order_id, line_no, prod_id, qty, unit_price)
            values (i, ln, v_prod, v_qty, v_price);
          exception when dup_val_on_index then null; end;
        end;
      end loop;
    end;
  end loop;
  commit;

  -- Payments (for some orders)
  for i in 1..${NUM_ORDERS} loop
    if mod(i, 2) = 0 then
      declare
        v_amt number := null;
      begin
        -- Compute order amount first (sequence NEXTVAL isn't allowed directly in this SELECT context)
        select sum(qty*unit_price)
          into v_amt
          from ${SEED_USER}.order_items
         where order_id = i;

        -- Insert the payment row using VALUES so NEXTVAL is valid here
        insert into ${SEED_USER}.payments(pay_id, order_id, amount, method, paid_at)
        values (
          ${SEED_USER}.payments_seq.nextval,
          i,
          nvl(v_amt, 0),
          case mod(i,3) when 0 then 'CARD' when 1 then 'PAYPAL' else 'BANK' end,
          trunc(sysdate - dbms_random.value(0, 365))
        );
      exception when dup_val_on_index then null; end;
    end if;
  end loop;
  commit;

  -- Logs
  for i in 1..100 loop
    begin
      insert into ${SEED_USER}.event_log(id, who, action, created_at, payload)
      values (${SEED_USER}.event_log_seq.nextval,
              'system',
              case mod(i,5) when 0 then 'insert' when 1 then 'update' when 2 then 'delete' when 3 then 'login' else 'logout' end,
              trunc(sysdate - dbms_random.value(0, 90)),
              to_clob('{"i":'||i||',"demo":"payload"}'));
    exception when dup_val_on_index then null; end;
  end loop;
  commit;

  -- Audit trail
  for i in 1..50 loop
    begin
      insert into ${SEED_USER}.audit_trail(id, table_name, pk_value, action, at_time)
      values (${SEED_USER}.audit_trail_seq.nextval,
              'ORDERS',
              to_char(i),
              case mod(i,3) when 0 then 'INSERT' when 1 then 'UPDATE' else 'DELETE' end,
              trunc(sysdate - dbms_random.value(0, 365)));
    exception when dup_val_on_index then null; end;
  end loop;
  commit;
end;
/
"

echo "[✅] Oracle seed complete for user '${SEED_USER}'."
echo "    Tables: departments, employees, projects, emp_projects,"
echo "            customers, addresses, products, orders, order_items,"
echo "            payments, event_log, audit_trail"
echo "    Use these in your demo 'Discover → Generate DDL → Apply DDL → Copy Data'."