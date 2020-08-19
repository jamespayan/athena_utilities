import boto3
import time
import json

PARAMS = {
    'region': 'us-west-2',
    'database': 'fpd_staging',
    'bucket': 'a-bucket-name',
    'path': 'temp/path/where/athena/load/outputs',
    'query': None
}


def athena_query(client, params):
    '''\
        Starts the query execution asynchronically and returns a response object
        that has the execution id.
    '''
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response


def parse_athena_rows(resultSet, header=True):
    '''\
        Based on the resultset metadata, build a list of dict objects
        with the data extracted from athena service.    
    '''
    column_names = [col['Name'] for col in resultSet['ResultSetMetadata']['ColumnInfo']]
    rows_info = resultSet['Rows'][1:] if header else resultSet['Rows']
    rows = [
        dict(zip(
            column_names, 
            [col_val['VarCharValue'] for col_val in row['Data']]
        ))
        for row in (rows_info)
    ]
    return rows


def fetch_all(query, max_execution=120, page=0, page_size=0):
    '''\
        Fetch all the data extracted from athena service, parsed
        as dict objects, and paginated.
        (So far pagination is not so performant. Currently it downloads every
        single page until it gets the page that the user is requiring.)
    '''
    params = dict(PARAMS)
    params['query'] = query
    client = boto3.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while max_execution > 0:
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if (state == 'FAILED') or (state == 'CANCELLED') :
                raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query))
            elif state == 'SUCCEEDED':
                if not page or not page_size:
                    ## Function to get output results
                    response_query_result = client.get_query_results(QueryExecutionId=execution_id)
                    return parse_athena_rows(response_query_result['ResultSet'])
                else:
                    ## Get data by page:
                    results_paginator = client.get_paginator('get_query_results')
                    results_iter = results_paginator.paginate(
                        QueryExecutionId=execution_id,
                        PaginationConfig={
                            'PageSize': page_size
                        }
                    )
                    current_page = 0
                    for results_page in results_iter:
                        current_page += 1
                        if current_page == page:
                            return parse_athena_rows(results_page['ResultSet'], header=(page==1))
                    # requested page overloads the number of pages.
                    return []
        time.sleep(1)
    
    raise Exception('Athena query with the string "{}" failed by timeout'.format(query))