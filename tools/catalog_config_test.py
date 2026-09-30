#!/usr/bin/env python3
"""Configuration contract: explicit empty passwords differ from missing values."""
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cdc_catalog


def main():
    values = dict(CDC_MYSQL_HOST='127.0.0.1', CDC_MYSQL_PORT='3306',
                  CDC_MYSQL_USER='root', CDC_MYSQL_PASSWORD='', CDC_MYSQL_SCHEMA='synthetic',
                  CDC_SR_FE_HOST='127.0.0.1', CDC_SR_FE_PORT='8030', CDC_SR_QUERY_PORT='9030',
                  CDC_SR_USER='root', CDC_SR_PASSWORD='', CDC_SR_DB='synthetic', CDC_SERVER_ID='188611')
    result = cdc_catalog.connection_settings_values(values)
    assert result['mysql']['password'] == result['starrocks']['password'] == ''
    for key in cdc_catalog.SECRET_VARIABLES:
        for missing in ('absent', None):
            candidate = dict(values)
            if missing == 'absent':
                candidate.pop(key)
            else:
                candidate[key] = None
            assert cdc_catalog.connection_settings_values(candidate, require=False) is None
            try:
                cdc_catalog.connection_settings_values(candidate)
            except ValueError as exc:
                assert key in str(exc)
            else:
                raise AssertionError('missing credential was treated as empty')
    for key in ('CDC_MYSQL_HOST', 'CDC_SR_DB'):
        candidate = dict(values, **{key: ''})
        assert cdc_catalog.connection_settings_values(candidate, require=False) is None
    with tempfile.TemporaryDirectory(prefix='m2s-config-test-') as directory:
        path = str(Path(directory) / 'catalog.sqlite3')
        commands = ["SET VARIABLE " + key + " = '" + value + "'" for key, value in values.items()]
        cdc_catalog.execute_batch(path, commands)
        assert cdc_catalog.connection_configured(path)
        saved = cdc_catalog.connection_settings(path)
        assert saved == result
    from e2e_contract import setup_catalog, source_options
    from starrocks_contract import configuration
    for mode in ('merge_async', 'transaction'):
        with tempfile.TemporaryDirectory(prefix='m2s-e2e-config-test-') as directory:
            env = setup_catalog(Path(directory), configuration(), source_options(), mode, 128)
            plan = cdc_catalog.load_plan(env['CDC_CATALOG_FILE'])
            assert len(plan['mappings']) == 1
            assert plan['mappings'][0]['sr_table'] == 'events'
    print('CATALOG CONFIG PASS explicit empty passwords and missing-value distinction', flush=True)


if __name__ == '__main__':
    main()
