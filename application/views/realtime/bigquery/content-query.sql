SELECT
  toplist.team_id AS team_id,
  toplist.cat1 AS cat1,
  toplist.cat2 AS cat2,
  toplist.cat3 AS cat3,
  toplist.toplist_rank AS rank,
  toplist.content_id AS content_id,
  toplist.category_map AS category_map,
  content.share AS share,
  content.view AS view,
  content.user AS user,
  content.organic AS organic,
  content.team AS team,
  content.paid AS paid,
  content.by_time AS by_time,
  source.toplist AS source
FROM (
  SELECT
    team_id,
    cat1,
    cat2,
    cat3,
    category_map,
    content_id,
    toplist_rank,
    share
  FROM (
    SELECT
      team_id,
      cat1,
      cat2,
      cat3,
      category_map,
      content_id,
      RANK() OVER (PARTITION BY team_id, cat1, cat2, cat3 ORDER BY share DESC) toplist_rank,
      share
    FROM (
      SELECT
        team_id,
        cat1,
        cat2,
        cat3,
        category_map,
        content_id,
        SUM(is_share) share
      FROM
        getCategory(
        SELECT
          team_id,
          cat1,
          cat2,
          cat3,
          category_map,
          content_id,
          is_share
        FROM (TABLE_QUERY([{{dataset}}], 'table_id BETWEEN "views_{{to_table}}" AND "views_{{from_table}}"'))
        WHERE
          time BETWEEN SEC_TO_TIMESTAMP({{to}}) AND SEC_TO_TIMESTAMP({{from}}))
      GROUP BY
        team_id,
        cat1,
        cat2,
        cat3,
        category_map,
        content_id ))
  WHERE
    toplist_rank <= 10) toplist
LEFT JOIN (
  SELECT
    team_id,
    content_id,
    SUM(share) AS share,
    SUM(view) AS view,
    SUM(user) AS user,
    GROUP_CONCAT_UNQUOTED(CONCAT(STRING(TIMESTAMP_TO_SEC(time)), '|', STRING(share), '|', STRING(view), '|', STRING(user)), '||') AS by_time,
    SUM(organic) AS organic,
    SUM(team) AS team,
    SUM(paid) AS paid
  FROM (
    SELECT
      team_id,
      content_id,
      time,
      SUM(is_share) AS share,
      SUM(1) AS view,
      SUM(new_content) AS user,
      SUM(IF(traffic_type == 1, 1, 0)) AS organic,
      SUM(IF(traffic_type == 2, 1, 0)) AS team,
      SUM(IF(traffic_type == 3, 1, 0)) AS paid
    FROM (TABLE_QUERY([{{dataset}}], 'table_id BETWEEN "views_{{to_table}}" AND "views_{{from_table}}"'))
    WHERE
      time BETWEEN SEC_TO_TIMESTAMP({{to}}) AND SEC_TO_TIMESTAMP({{from}})
    GROUP BY
      team_id,
      content_id,
      time )
  GROUP BY
    team_id,
    content_id ) content
ON
  toplist.team_id = content.team_id
  AND toplist.content_id = content.content_id
LEFT JOIN (
  SELECT
    team_id,
    content_id,
    GROUP_CONCAT_UNQUOTED(CONCAT(STRING(traffic_type), '|||', toplist), '||||') AS toplist
  FROM (
    SELECT
      team_id,
      content_id,
      traffic_type,
      GROUP_CONCAT_UNQUOTED(CONCAT(referrer, '|', STRING(share)), '||') AS toplist
    FROM (
      SELECT
        team_id,
        content_id,
        traffic_type,
        referrer,
        share,
        RANK() OVER (PARTITION BY team_id, content_id, traffic_type ORDER BY share DESC) referrer_rank
      FROM (
        SELECT
          team_id,
          content_id,
          traffic_type,
          referrer,
          SUM(1) AS share
        FROM (TABLE_QUERY([{{dataset}}], 'table_id BETWEEN "views_{{to_table}}" AND "views_{{from_table}}"'))
        WHERE
          is_share AND
          time BETWEEN SEC_TO_TIMESTAMP({{to}}) AND SEC_TO_TIMESTAMP({{from}})
        GROUP BY
          team_id,
          content_id,
          traffic_type,
          referrer))
    WHERE
      referrer_rank <= 10
    GROUP BY
      team_id,
      content_id,
      traffic_type )
  GROUP BY
    team_id,
    content_id ) source
ON
  toplist.team_id = source.team_id
  AND toplist.content_id = source.content_id
