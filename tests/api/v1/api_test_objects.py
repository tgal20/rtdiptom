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

from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.odbc.test_db_sql_connector import MockedDBConnection
from tests.sdk.python.rtdip_sdk.functions.test_raw import DATABRICKS_SQL_CONNECT

BASE_URL = "https://test"

BASE_MOCKED_PARAMETER_DICT = {
        "business_unit": "mocked-buiness-unit",
        "region": "mocked-region",
        "asset": "mocked-asset",
        "data_security_level": "mocked-data-security-level",
        }

METADATA_MOCKED_PARAMETER_DICT = BASE_MOCKED_PARAMETER_DICT.copy()
METADATA_MOCKED_PARAMETER_DICT["tag_name"] = "MOCKED-TAGNAME1" 

METADATA_MOCKED_PARAMETER_ERROR_DICT = METADATA_MOCKED_PARAMETER_DICT.copy()
METADATA_MOCKED_PARAMETER_ERROR_DICT.pop("business_unit")

METADATA_POST_MOCKED_PARAMETER_DICT = METADATA_MOCKED_PARAMETER_DICT.copy()
METADATA_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

METADATA_POST_BODY_MOCKED_PARAMETER_DICT = {}
METADATA_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = ["MOCKED-TAGNAME1", "MOCKED-TAGNAME2"]

RAW_MOCKED_PARAMETER_DICT = BASE_MOCKED_PARAMETER_DICT.copy()
RAW_MOCKED_PARAMETER_DICT["data_type"] = "mocked-data-type"
RAW_MOCKED_PARAMETER_DICT["tag_name"] = "MOCKED-TAGNAME1"
RAW_MOCKED_PARAMETER_DICT["include_bad_data"] = True      
RAW_MOCKED_PARAMETER_DICT["start_date"] = "2011-01-01"
RAW_MOCKED_PARAMETER_DICT["end_date"] = "2011-01-02"

RAW_POST_MOCKED_PARAMETER_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
RAW_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

RAW_POST_BODY_MOCKED_PARAMETER_DICT = {}
RAW_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = ["MOCKED-TAGNAME1", "MOCKED-TAGNAME2"]

RAW_MOCKED_PARAMETER_ERROR_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
RAW_MOCKED_PARAMETER_ERROR_DICT.pop("start_date")

RESAMPLE_MOCKED_PARAMETER_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT = RAW_MOCKED_PARAMETER_ERROR_DICT.copy()

RESAMPLE_MOCKED_PARAMETER_DICT["sample_rate"] = 1
RESAMPLE_MOCKED_PARAMETER_DICT["sample_unit"] = "minute"
RESAMPLE_MOCKED_PARAMETER_DICT["agg_method"] = "avg"
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT["sample_rate"] = 1
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT["sample_unit"] = "minute"
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT["agg_method"] = "avg"

RESAMPLE_POST_MOCKED_PARAMETER_DICT = RESAMPLE_MOCKED_PARAMETER_DICT.copy()
RESAMPLE_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

RESAMPLE_POST_BODY_MOCKED_PARAMETER_DICT = {}
RESAMPLE_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = ["MOCKED-TAGNAME1", "MOCKED-TAGNAME2"]

INTERPOLATE_MOCKED_PARAMETER_DICT = RESAMPLE_MOCKED_PARAMETER_DICT.copy()
INTERPOLATE_MOCKED_PARAMETER_ERROR_DICT = RESAMPLE_MOCKED_PARAMETER_ERROR_DICT.copy()

INTERPOLATE_MOCKED_PARAMETER_DICT["interpolation_method"] = "forward_fill"
INTERPOLATE_MOCKED_PARAMETER_ERROR_DICT["interpolation_method"] = "forward_fill"

INTERPOLATE_POST_MOCKED_PARAMETER_DICT = INTERPOLATE_MOCKED_PARAMETER_DICT.copy()
INTERPOLATE_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

INTERPOLATE_POST_BODY_MOCKED_PARAMETER_DICT = {}
INTERPOLATE_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = ["MOCKED-TAGNAME1", "MOCKED-TAGNAME2"]

TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT = RAW_MOCKED_PARAMETER_ERROR_DICT.copy()

TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT["window_size_mins"] = 10
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT["window_length"] = 10
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT["step"] = "metadata"
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["window_size_mins"] = 10
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["window_length"] = 10
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["step"] = "metadata"

TIME_WEIGHTED_AVERAGE_POST_MOCKED_PARAMETER_DICT = TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT.copy()
TIME_WEIGHTED_AVERAGE_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT = {}
TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = ["MOCKED-TAGNAME1", "MOCKED-TAGNAME2"]

TEST_HEADERS = {"Authorization": "Bearer Test Token"}
TEST_HEADERS_POST = {"Authorization": "Bearer Test Token", "Content-Type": "application/json"}

def mocker_setup(mocker: MockerFixture, patch_method, test_data, side_effect=None):
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection(), side_effect=side_effect)
    mocker.patch(patch_method, return_value = test_data)
    mocker.patch("src.api.auth.azuread.get_azure_ad_token", return_value = "token")
    return mocker   