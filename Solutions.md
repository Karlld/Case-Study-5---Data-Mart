**Case Study Questions**

The following case study questions require some data cleaning steps before we start to unpack Danny’s key business questions in more depth.
Data Cleansing Steps

1. In a single query, perform the following operations and generate a new table in the data_mart schema named clean_weekly_sales:

Convert the week_date to a DATE format

Add a week_number as the second column for each week_date value, for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc

Add a month_number with the calendar month for each week_date value as the 3rd column

Add a calendar_year column as the 4th column containing either 2018, 2019 or 2020 values

Add a new column called age_band after the original segment column using the following mapping on the number inside the segment value

segment	age_band
1	Young Adults
2	Middle Aged
3 or 4	Retirees
Add a new demographic column using the following mapping for the first letter in the segment values:
segment	demographic
C	Couples
F	Families
Ensure all null string values with an "unknown" string value in the original segment column as well as the new age_band and demographic columns

Generate a new avg_transaction column as the sales value divided by transactions rounded to 2 decimal places for each record

```sql
DROP TABLE IF EXISTS clean_weekly_sales;
CREATE TABLE clean_weekly_sales AS 
      SELECT
        TO_DATE(week_date,'DD/MM/YY') AS week_date,
        EXTRACT(WEEK FROM TO_DATE(week_date, 'DD/MM/YY')) AS week_number,
        EXTRACT(MONTH FROM TO_DATE(week_date, 'DD/MM/YY')) AS month_number,
        EXTRACT(YEAR FROM TO_DATE(week_date, 'DD/MM/YY')) AS calendar_year,
         region,
         platform,
	     CASE WHEN segment = 'null' THEN null
		      ELSE segment
			  END AS segment,
	     CASE 
         WHEN RIGHT(segment, 1) ~ '^[0-9]$' THEN
         CASE 
            WHEN RIGHT(segment, 1)::INT = 1 THEN 'Young Adult'
            WHEN RIGHT(segment, 1)::INT = 2 THEN 'Middle Aged'
            WHEN RIGHT(segment, 1)::INT IN (3, 4) THEN 'Retirees'
            ELSE NULL
        END
    ELSE NULL
END AS age_band,
	     CASE WHEN LEFT(segment, 1) = 'C' THEN 'Couples'
	          WHEN LEFT(segment, 1) = 'F'  THEN 'Families'
	          ELSE NULL
	          END AS demographic,
	     customer_type,
	     transactions,
	     sales,
	     ROUND((sales::NUMERIC/transactions),2) AS avg_transaction
	FROM weekly_sales;
```
2. Data Exploration

What day of the week is used for each week_date value?

```sql
SELECT DISTINCT(TO_CHAR(week_date, 'day')) AS week_day 
           FROM clean_weekly_sales;
``` 

| week_day |
|----------|
| monday   |

What range of week numbers are missing from the dataset?

```sql
WITH missing_weeks AS (SELECT gs.week_number
                        FROM generate_series(1, 52) AS gs(week_number)
                        WHERE gs.week_number NOT IN (SELECT DISTINCT week_number
                        FROM clean_weekly_sales)),
          numbered AS (SELECT week_number,
                              week_number - ROW_NUMBER() OVER (ORDER BY week_number) AS grp
                       FROM missing_weeks),
          grouped_ranges AS (SELECT MIN(week_number) AS range_start,
                                    MAX(week_number) AS range_end
                             FROM numbered
                             GROUP BY grp
                             ORDER BY range_start)
                   
				   SELECT CASE WHEN range_start = range_end THEN range_start::text
                               ELSE range_start || '–' || range_end
                               END AS missing_ranges
                         FROM grouped_ranges;
```

| missing_ranges |
|----------------|
|     1–12   |
|  37–52   |


How many total transactions were there for each year in the dataset?

```sql
SELECT calendar_year, 
       SUM(transactions) AS total_transaction 
     FROM clean_weekly_sales
	 GROUP BY calendar_year
	 ORDER BY calendar_year;
```


| calendar_year  |  total_transaction  |
|----------------|---------------------|
|      2018	     |      346406460  |
|        2019	  |            365639285 |
|       2020	  |            375813651  |


What is the total sales for each region for each month?

```sql
SELECT region,
       month_number, 
       calendar_year, 
       SUM(sales) AS total_transaction 
     FROM clean_weekly_sales
	 GROUP BY region, month_number, calendar_year
	 ORDER BY region, calendar_year, month_number;
```
This table is only a sample of the output:

| region  | month_number  | calendar_year  |  total_transaction  |
|---------|---------------|----------------|---------------------|
| AFRICA |	3	| 2018	| 130542213 |
| AFRICA |	4	| 2018	| 650194751 |
| AFRICA |	5	| 2018	| 522814997 |
| AFRICA |	6	| 2018	| 519127094 |
| AFRICA |	7	| 2018	| 674135866 |
| AFRICA |	8	| 2018	| 539077371 |
| AFRICA |	9	| 2018	|135084533 |
| AFRICA |	3	| 2019	| 141619349 |
| AFRICA |	4	| 2019	| 700447301 |
| AFRICA |	5	| 2019	| 553828220 |
| AFRICA |	6	| 2019	| 546092640 |
| AFRICA |	7	| 2019	| 711867600 |
| AFRICA | 8	| 2019	| 564497281 |
| AFRICA |	9	| 2019	| 141236454 |
| AFRICA |	3	| 2020	| 295605918 |
| AFRICA |	4	| 2020	| 561141452 |
| AFRICA |	5	| 2020	| 570601521 |
| AFRICA | 6	| 2020	| 702340026 |
| AFRICA |	7	| 2020	| 574216244 |
| AFRICA | 8	| 2020	| 706022238 |
| ASIA |	3	| 2018	 | 119180883 |
| ASIA |	4	| 2018	| 603716301 |
| ASIA |	5	| 2018	| 472634283 |


What is the total count of transactions for each platform

```sql
SELECT platform,
       SUM(transactions) AS total_transactions
    FROM clean_weekly_sales
    GROUP BY platform;
```

| platform   |   total_transactions  |
|------------|-----------------------|
| Shopify   |	   5925169 |
| Retail  |	      1081934227  |


What is the percentage of sales for Retail vs Shopify for each month?

```sql
WITH total_shopify AS (SELECT platform,
                              calendar_year,
                              month_number,
                              SUM(sales) AS total_shopify
                       FROM clean_weekly_sales
                       WHERE platform = 'Shopify'
                       GROUP BY platform, calendar_year, month_number),

total_retail AS (SELECT platform,
                        calendar_year,
                        month_number,
                        SUM(sales) AS total_retail
                   FROM clean_weekly_sales
                   WHERE platform = 'Retail'
                  GROUP BY platform,calendar_year, month_number)
						 
SELECT ts.calendar_year,
       ts.month_number,
       ROUND((100.0*total_shopify/(total_shopify+total_retail)),2) AS shopify_pcent,
       ROUND((100.0*total_retail/(total_shopify+total_retail)),2) AS retail_pcent
  FROM total_shopify ts
  JOIN total_retail tr on ts.calendar_year = tr.calendar_year
  AND ts.month_number = tr.month_number
  GROUP BY ts.calendar_year, ts.month_number, ts.total_shopify, tr.total_retail
  ORDER BY ts.calendar_year, ts.month_number;
```

| calendar_year   |  month_number  |  shopify_pcent  |   retail_pcent |
|-----------------|----------------|-----------------|----------------|
| 2018 |	3 |	2.08 |	97.92 |
| 2018 |	4 |	2.07 |	97.93 |
| 2018 |	5 |	2.27 |	97.73 |
| 2018 |	6 |	2.24 |	97.76 |
| 2018 |	7 |	2.25 |	97.75 |
| 2018 |	8 |	2.29 |	97.71 |
| 2018 |	9 |	2.32 |	97.68 |
| 2019 |	3 |	2.29 |	97.71 |
| 2019 |	4 |	2.20 |	97.80 |
| 2019 |	5 |	2.48 |	97.52 |
| 2019 |	6 |	2.58 |	97.42 |
| 2019 | 	7 |	2.65 |	97.35 |
| 2019 |	8 |	2.79 |	97.21 |
| 2019 |	9 |	2.91 |	97.09 |
| 2020 |	3 |	2.70 |	97.30 |
| 2020 |	4 |	3.04 |	96.96 |
| 2020 |	5 |	3.29 |	96.71 |
| 2020 |	6 |	3.20 |	96.80 |
| 2020 |	7 |	3.33 |	96.67 |
| 2020 |	8 |	3.49 |	96.51 |

