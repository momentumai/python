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
  FROM (
      SELECT
        team_id,
        {{cat1}} AS cat1,
        {{cat2}} AS cat2,
        {{cat3}} AS cat3,
        is_share,
        time,
        {{new_cat}} AS is_new
      FROM (TABLE_QUERY([{{dataset}}], 'table_id BETWEEN "views_{{to_table}}" AND "views_{{from_table}}"'))
      WHERE
        time BETWEEN SEC_TO_TIMESTAMP({{to}}) AND SEC_TO_TIMESTAMP({{from}})
    ) t1
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