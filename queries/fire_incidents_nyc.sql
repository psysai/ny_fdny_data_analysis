select "{yyyy}" as `incident_year`,
       `incident_borough`,
       `incident_classification`,
       `incident_classification_group`,
       `zipcode`,
       count(`starfire_incident_id`) AS `incident_count`
WHERE `starfire_incident_id` like "{yy}%"
AND `zipcode` is not null and `zipcode` != "" and `zipcode` != "0"
GROUP BY `incident_year`,
         `incident_borough`,
         `incident_classification`,
         `incident_classification_group`,
         `zipcode` 