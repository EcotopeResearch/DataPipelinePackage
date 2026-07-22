import pytest
from unittest.mock import MagicMock
import pandas as pd
from ecopipeline.load import AlarmSetLoader


def _make_df():
    return pd.DataFrame({
        'alarm_type': ['BALVALV', 'SHRTCYC'],
        'variables': [
            'PowerIn_Total;PowerIn_SwingTank1;PowerIn_SwingTank2',
            'HeatOut_TM;PowerIn_SwingTank1;PowerIn_SwingTank2'
        ]
    })


def _make_config(mocker, table_exists_count):
    configMock = MagicMock()
    cursor_mock = MagicMock()
    con_mock = MagicMock()

    mocker.patch.object(cursor_mock, 'execute')
    mocker.patch.object(con_mock, 'close')
    mocker.patch.object(con_mock, 'commit')
    mocker.patch.object(con_mock, 'rollback')

    cursor_mock.fetchall.side_effect = [[(table_exists_count,)]]
    configMock.connect_db.side_effect = [(con_mock, cursor_mock)]
    configMock.get_db_name.return_value = 'test_db'
    configMock.get_site_name.return_value = 'test_site'

    return configMock, cursor_mock, con_mock


def test_load_database_empty_df():
    loader = AlarmSetLoader()
    configMock = MagicMock()
    result = loader.load_database(configMock, pd.DataFrame())

    assert result is True
    configMock.connect_db.assert_not_called()


def test_load_database_missing_columns():
    loader = AlarmSetLoader()
    configMock = MagicMock()
    bad_df = pd.DataFrame({'alarm_type': ['BALVALV']})

    with pytest.raises(Exception, match="alarm_set_df is missing required columns"):
        loader.load_database(configMock, bad_df)


def test_load_database_table_already_exists(mocker):
    loader = AlarmSetLoader()
    df = _make_df()
    configMock, cursor_mock, con_mock = _make_config(mocker, table_exists_count=1)

    result = loader.load_database(configMock, df, table_name='alarm_set', dbname='test_db')

    assert result is True
    expected_queries = [
        "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'test_db') AND (TABLE_NAME = 'alarm_set')",
        "DELETE FROM alarm_set WHERE site_name = %s",
        "INSERT INTO alarm_set (site_name, alarm_type, vars)\n                VALUES (%s, %s, %s)\n            ",
        "INSERT INTO alarm_set (site_name, alarm_type, vars)\n                VALUES (%s, %s, %s)\n            ",
    ]
    assert cursor_mock.execute.call_count == len(expected_queries)
    assert cursor_mock.execute.call_args_list[0][0][0] == expected_queries[0]
    assert cursor_mock.execute.call_args_list[1][0][0] == expected_queries[1]
    assert cursor_mock.execute.call_args_list[1][0][1] == ('test_site',)
    assert cursor_mock.execute.call_args_list[2][0][1] == (
        'test_site', 'BALVALV', 'PowerIn_Total;PowerIn_SwingTank1;PowerIn_SwingTank2'
    )
    assert cursor_mock.execute.call_args_list[3][0][1] == (
        'test_site', 'SHRTCYC', 'HeatOut_TM;PowerIn_SwingTank1;PowerIn_SwingTank2'
    )
    con_mock.commit.assert_called_once()
    con_mock.rollback.assert_not_called()


def test_load_database_creates_table_when_missing(mocker):
    loader = AlarmSetLoader()
    df = _make_df()
    configMock, cursor_mock, con_mock = _make_config(mocker, table_exists_count=0)

    result = loader.load_database(configMock, df, table_name='alarm_set', dbname='test_db')

    assert result is True
    executed_queries = [call[0][0] for call in cursor_mock.execute.call_args_list]
    assert "CREATE TABLE IF NOT EXISTS alarm_set" in executed_queries[1]
    assert "site_name VARCHAR(20)" in executed_queries[1]
    assert "alarm_type VARCHAR(20)" in executed_queries[1]
    assert "vars VARCHAR(120)" in executed_queries[1]
    assert executed_queries[2] == "DELETE FROM alarm_set WHERE site_name = %s"
    assert len(executed_queries) == 5  # count check, create, delete, 2 inserts


def test_load_database_defaults_site_name_and_dbname_from_config(mocker):
    loader = AlarmSetLoader()
    df = _make_df()
    configMock, cursor_mock, con_mock = _make_config(mocker, table_exists_count=1)

    loader.load_database(configMock, df)

    configMock.get_db_name.assert_called_once()
    configMock.get_site_name.assert_called_once()


def test_load_database_rolls_back_on_exception(mocker):
    loader = AlarmSetLoader()
    df = _make_df()
    configMock, cursor_mock, con_mock = _make_config(mocker, table_exists_count=1)

    def execute_side_effect(sql, *args, **kwargs):
        if sql.startswith("DELETE"):
            raise Exception("boom")
    cursor_mock.execute.side_effect = execute_side_effect

    result = loader.load_database(configMock, df, table_name='alarm_set', dbname='test_db')

    assert result is False
    con_mock.rollback.assert_called_once()
    con_mock.commit.assert_not_called()


def test_create_new_table(mocker):
    loader = AlarmSetLoader()
    cursor_mock = MagicMock()
    mocker.patch.object(cursor_mock, 'execute')

    assert loader.create_new_table(cursor_mock, 'alarm_set') is True
    executed_sql = cursor_mock.execute.call_args_list[0][0][0]
    assert "CREATE TABLE IF NOT EXISTS alarm_set" in executed_sql
    assert "site_name VARCHAR(20)" in executed_sql
    assert "alarm_type VARCHAR(20)" in executed_sql
    assert "vars VARCHAR(120)" in executed_sql
