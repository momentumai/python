function getCategory(row, emit) {
    emit({
        team_id: row.team_id,
        cat1: row.cat1,
        cat2: row.cat2,
        cat3: row.cat3,
        is_share: row.is_share,
        time: row.time,
        is_new: row.new_cat3
    });
    emit({
        team_id: row.team_id,
        cat1: row.cat1,
        cat2: row.cat2,
        cat3: 'NONE',
        is_share: row.is_share,
        time: row.time,
        is_new: row.new_cat2
    });
    emit({
        team_id: row.team_id,
        cat1: row.cat1,
        cat2: 'NONE',
        cat3: 'NONE',
        is_share: row.is_share,
        time: row.time,
        is_new: row.new_cat1
    });
    emit({
        team_id: row.team_id,
        cat1: 'NONE',
        cat2: 'NONE',
        cat3: 'NONE',
        is_share: row.is_share,
        time: row.time,
        is_new: row.new_cat0
    });
}

bigquery.defineFunction(
    'getCategory', ['team_id', 'cat1', 'cat2', 'cat3', 'is_share', 'time', 'new_cat0', 'new_cat1', 'new_cat2', 'new_cat3'], [{
        'name': 'team_id',
        'type': 'integer'
    }, {
        'name': 'cat1',
        'type': 'string'
    }, {
        'name': 'cat2',
        'type': 'string'
    }, {
        'name': 'cat3',
        'type': 'string'
    }, {
        'name': 'is_share',
        'type': 'boolean'
    }, {
        'name': 'time',
        'type': 'timestamp'
    }, {
        'name': 'is_new',
        'type': 'boolean'
    }],
    getCategory
);