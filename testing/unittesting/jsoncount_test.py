import pytest

from kit.utils import do_json_counts

# this allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_context", "spark_session")


def test_do_json_counts(spark_context, spark_session):
    """ test that a single event is parsed correctly
    Args:
        spark_context: test fixture SparkContext
        hive_context: test fixture HiveContext
    """

    test_input = [
        {'name': 'vikas'},
        {'name': 'vikas'},
        {'name': 'john'},
        {'name': 'jane'},
    ]

    input_rdd = spark_context.parallelize(test_input, 1)
    #df = hive_context.jsonRDD(input_rdd)
    
    
    df = spark_session.read.json(input_rdd)
    
    results = do_json_counts(df, 'vikas')

    expected_results = 2
    
    assert results == expected_results
