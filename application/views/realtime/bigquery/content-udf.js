function getCategory(row, emit) {
    emit({
        team_id: row.team_id,
        cat1: row.cat1,
        cat2: row.cat2,
        cat3: row.cat3,
        is_share: row.is_share,
        content_id: row.content_id
    });
    emit({
        team_id: row.team_id,
        cat1: row.cat1,
        cat2: row.cat2,
        cat3: 'NONE',
        is_share: row.is_share,
        content_id: row.content_id
    });
    emit({
        team_id: row.team_id,
        cat1: row.cat1,
        cat2: 'NONE',
        cat3: 'NONE',
        is_share: row.is_share,
        content_id: row.content_id
    });
    emit({
        team_id: row.team_id,
        cat1: 'NONE',
        cat2: 'NONE',
        cat3: 'NONE',
        is_share: row.is_share,
        content_id: row.content_id
    });
}

bigquery.defineFunction(
    'getCategory', ['team_id', 'cat1', 'cat2', 'cat3', 'is_share', 'content_id'], [{
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
        'name': 'content_id',
        'type': 'string'
    }],
    getCategory
);
