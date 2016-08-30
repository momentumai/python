SELECT rule.*, team.name AS team_name from rule
INNER JOIN team
ON rule.team_id = team.id
WHERE IFNULL(deleted, 0) <> 1 AND team_id = {exclude};
