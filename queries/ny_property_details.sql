SELECT 
    CASE(`boro`="1","MANHATTAN",
        `boro`="2", "BRONX",
        `boro`="3", "BROOKLYN",
        `boro`="4", "QUEENS",
        `boro`="5", "STATEN ISLAND")
     as `borough`, 
    `year` as `year`,
    `zip_code` as `zipcode`,
    AVG(`pyacttot`) as `avg_prop_total_value`, 
    AVG(`gross_sqft`) as `avg_prop_gross_sqft`
WHERE year="{yyyy}" and `boro` in ("1", "2", "3", "4", "5")
AND `zip_code` is not null and `zip_code` != "" and `zip_code` != "0"
GROUP BY borough, year, zipcode