SELECT
  team_id,
  cat1,
  cat2,
  cat3,
  SUM(share) AS share,
  SUM(view) AS view,
  SUM(user) AS user,
  GROUP_CONCAT_UNQUOTED(CONCAT(STRING(TIMESTAMP_TO_SEC(time)), '|', STRING(share), '|', STRING(view), '|', STRING(user)), '||') AS by_time
FROM (
  SELECT
    team_id,
    cat1,
    cat2,
    cat3,
    time,
    SUM(is_share) AS share,
    COUNT(1) AS view,
    SUM(is_new) AS user
  FROM
    getCategory (
      SELECT
        team_id,
        cat1,
        cat2,
        cat3,
        is_share,
        time,
        new_cat0,
        new_cat1,
        new_cat2,
        new_cat3
      FROM (TABLE_QUERY([prod_realtime], 'table_id BETWEEN "views_{{to_table}}" AND "views_{{from_table}}"'))
      WHERE
        time BETWEEN SEC_TO_TIMESTAMP({{to}}) AND SEC_TO_TIMESTAMP({{from}})
    )
  GROUP BY
    team_id,
    cat1,
    cat2,
    cat3,
    time) stats_sub
GROUP BY
  team_id,
  cat1,
  cat2,
  cat3