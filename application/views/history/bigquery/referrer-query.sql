SELECT
  team_id,
  content_id,
  GROUP_CONCAT_UNQUOTED(CONCAT(STRING(traffic_type), '|||', referrer_list), '||||') AS referrer_list
FROM (
  SELECT
    team_id,
    content_id,
    traffic_type,
    GROUP_CONCAT_UNQUOTED(CONCAT(referrer, '|', STRING(share)), '||') AS referrer_list
  FROM (
    SELECT
      team_id,
      content_id,
      traffic_type,
      referrer,
      SUM(1) AS share
    FROM [prod_realtime.views_{{table}}]
    WHERE
      is_share AND
      time = SEC_TO_TIMESTAMP({{time}})
    GROUP BY
      team_id,
      content_id,
      traffic_type,
      referrer)
  GROUP BY
    team_id,
    content_id,
    traffic_type)
GROUP BY
  team_id,
  content_id