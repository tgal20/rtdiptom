# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
sys.path.insert(0, '.')
import pandas as pd
import pytest
from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.odbc.test_db_sql_connector import MockedDBConnection, MockedCursor 
from src.sdk.python.rtdip_sdk.odbc.db_sql_connector import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.functions.interpolate import get as interpolate_get

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = 'databricks.sql.connect'
DATABRICKS_SQL_CONNECT_CURSOR = 'databricks.sql.connect.cursor'
MOCKED_QUERY='SELECT a.EventTime, a.TagName, last_value(b.Value, true) OVER (PARTITION BY a.TagName ORDER BY a.EventTime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Value FROM (SELECT explode(sequence(to_timestamp(\'2011-01-01T00:00:00\'), to_timestamp(\'2011-01-02T23:59:59\'), INTERVAL \'1 hour\')) AS EventTime, explode(array(\'"MOCKED-TAGNAME"\')) AS TagName) a LEFT OUTER JOIN (SELECT DISTINCT TagName, w.start AS EventTime, avg(Value) OVER (PARTITION BY TagName, w.start ORDER BY EventTime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS Value FROM (SELECT EventTime, WINDOW(EventTime, \'1 hour\') w, TagName, Status, Value FROM mocked-buiness-unit.sensors.mocked-asset_mocked-data-security-level_events_mocked-data-type WHERE EventDate BETWEEN to_date(\'2011-01-01T00:00:00\') AND to_date(\'2011-01-02T23:59:59\') AND EventTime BETWEEN to_timestamp(\'2011-01-01T00:00:00\') AND to_timestamp(\'2011-01-02T23:59:59\') AND TagName in (\'MOCKED-TAGNAME\') AND Status = \'Good\')) b ON a.EventTime = b.EventTime AND a.TagName = b.TagName'
MOCKED_PARAMETER_DICT = {
        "business_unit": "mocked-buiness-unit",
        "region": "mocked-region",
        "asset": "mocked-asset",
        "data_security_level": "mocked-data-security-level",
        "data_type": "mocked-data-type",
        "tag_names": ["MOCKED-TAGNAME"],
        "start_date": "2011-01-01",
        "end_date": "2011-01-02",
        "sample_rate": "1",
        "sample_unit": "hour",
        "agg_method": "avg",
        "interpolation_method": "forward_fill",
        "include_bad_data": False
        }

def test_interpolate(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(MockedCursor, "fetchall", return_value = pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]}))
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = interpolate_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)

def test_interpolate_fails(mocker: MockerFixture):
    mocker.spy(MockedDBConnection, "cursor")
    mocker.spy(MockedCursor, "execute")
    mocker.patch.object(MockedCursor, "fetchall", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    with pytest.raises(Exception):
        interpolate_get(mocked_connection, MOCKED_PARAMETER_DICT)