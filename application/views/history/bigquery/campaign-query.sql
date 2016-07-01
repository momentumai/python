SELECT
  team_id,
  campaign,
  SUM(1) AS share
FROM [{{dataset}}.views_{{table}}]
WHERE
  is_share
  AND campaign > 0 AND
  time = SEC_TO_TIMESTAMP({{time}})
GROUP BY
  team_id,
  campaign