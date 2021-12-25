-- Query 1
SELECT sup.SUPPLIER_NAME, prod.PRODUCT_NAME, rec.MONTH_NAME,rec.QUARTER_NO, sum(sale.TOTAL_SALES) as Sales
FROM i191708_dwh.sales as sale join i191708_dwh.record as rec join i191708_dwh.product as prod
join i191708_dwh.supplier as sup where sale.SUPPLIER_ID=sup.SUPPLIER_ID 
and sale.DATE_ID=rec.DATE_ID and sale.PRODUCT_ID=prod.PRODUCT_ID
group by sale.SUPPLIER_ID, sale.Product_id,rec.MONTH_NAME 
order by sale.SUPPLIER_ID, sale.Product_id,rec.DATE_ID;

-- Query 2
SELECT sale.STORE_ID, store.STORE_NAME, prod.PRODUCT_NAME, sum(sale.Total_sales) as Total_Sales 
FROM i191708_dwh.sales sale join i191708_dwh.product prod join i191708_dwh.store as store
where sale.Product_id=prod.Product_id and sale.STORE_ID=store.STORE_ID
group by sale.STORE_ID, prod.Product_id
order by sale.STORE_ID, prod.Product_id;

-- Query 3
SELECT prod.PRODUCT_NAME, sum(f.TOTAL_QUANTITY) as TOTAL_QUANTITY_SOLD FROM 
i191708_dwh.Sales as f join i191708_dwh.record as d  join i191708_dwh.product as prod
where f.DAte_ID=d.DAte_ID and f.PRODUCT_ID=prod.PRODUCT_ID 
and d.DAY_NAME in ('SATURDAY', 'SUNDAY') 
group by f.PRODUCT_ID order by TOTAL_QUANTITY_SOLD desc limit 5;

-- Query 4
WITH quarter_1 AS
(SELECT sale.PRODUCT_ID as prod, sum(sale.TOTAL_SALES) as Quarter_1_Total_Sales
FROM i191708_dwh.sales as sale join i191708_dwh.record as rec 
where sale.DATE_ID=rec.DATE_ID and rec.QUARTER_NO=1 group by sale.PRODUCT_ID),
quarter_2 AS
(SELECT sale.PRODUCT_ID as prod, sum(sale.TOTAL_SALES) as Quarter_2_Total_Sales
FROM i191708_dwh.sales as sale join i191708_dwh.record as rec 
where sale.DATE_ID=rec.DATE_ID and rec.QUARTER_NO=2 group by sale.PRODUCT_ID),
quarter_3 AS
(SELECT sale.PRODUCT_ID as prod, sum(sale.TOTAL_SALES) as Quarter_3_Total_Sales
FROM i191708_dwh.sales as sale join i191708_dwh.record as rec 
where sale.DATE_ID=rec.DATE_ID and rec.QUARTER_NO=3 group by sale.PRODUCT_ID),
quarter_4 AS
(SELECT sale.PRODUCT_ID as prod, sum(sale.TOTAL_SALES) as Quarter_4_Total_Sales
FROM i191708_dwh.sales as sale join i191708_dwh.record as rec 
where sale.DATE_ID=rec.DATE_ID and rec.QUARTER_NO=4 group by sale.PRODUCT_ID)
SELECT Prod.PRODUCT_NAME,  quarter_1.Quarter_1_Total_Sales,  quarter_2.Quarter_2_Total_Sales,  
quarter_3.Quarter_3_Total_Sales,  quarter_4.Quarter_4_Total_Sales, sum(sale.TOTAL_SALES) as Yearly_Sales
FROM i191708_dwh.sales as sale join i191708_dwh.product as Prod join quarter_1 join quarter_2 join quarter_3 join quarter_4
where sale.PRODUCT_ID=Prod.PRODUCT_ID and sale.PRODUCT_ID=quarter_1.prod and sale.PRODUCT_ID=quarter_2.prod
and sale.PRODUCT_ID=quarter_3.prod and sale.PRODUCT_ID=quarter_4.prod group by sale.PRODUCT_ID order by prod.PRODUCT_Name;

-- Query 5
WITH quarter_1 AS
(SELECT sale.PRODUCT_ID as prod, sum(sale.TOTAL_SALES) as Quarter_1_Total_Sales
FROM i191708_dwh.sales as sale join i191708_dwh.record as rec 
where sale.DATE_ID=rec.DATE_ID and rec.QUARTER_NO=1 group by sale.PRODUCT_ID),
quarter_2 AS
(SELECT sale.PRODUCT_ID as prod, sum(sale.TOTAL_SALES) as Quarter_2_Total_Sales
FROM i191708_dwh.sales as sale join i191708_dwh.record as rec 
where sale.DATE_ID=rec.DATE_ID and rec.QUARTER_NO=2 group by sale.PRODUCT_ID)
SELECT Prod.PRODUCT_NAME,  quarter_1.Quarter_1_Total_Sales,  quarter_2.Quarter_2_Total_Sales, 
sum(sale.TOTAL_SALES) as Yearly_Sales
FROM i191708_dwh.sales as sale join i191708_dwh.product as Prod join quarter_1 join quarter_2
where sale.PRODUCT_ID=Prod.PRODUCT_ID and sale.PRODUCT_ID=quarter_1.prod and sale.PRODUCT_ID=quarter_2.prod
group by sale.PRODUCT_ID;

-- Query 6
SELECT * FROM db.masterdata as mas where mas.PRODUCT_NAME="Tomatoes"; 

-- Query 7
CREATE VIEW STOREANALYSIS_MV As
SELECT sale.STORE_ID, sale.PRODUCT_ID, sum(sale.TOTAL_SALES) as STORE_TOTAL
FROM i191708_dwh.sales as sale
group by sale.STORE_ID, sale.PRODUCT_ID;