SELECT
  team_id,
  IF(campaign > 0, STRING(campaign), experiment) as campaign,
  SUM(1) AS share
FROM [{{dataset}}.views_{{table}}]
WHERE
  is_share AND
  time = SEC_TO_TIMESTAMP({{time}}) AND (
    campaign > 0 OR (
      experiment IS NOT NULL AND
      experiment != '' AND
      experiment != '0'
    )
  )
GROUP BY
  team_id,
  campaign
