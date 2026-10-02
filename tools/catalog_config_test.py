#!/usr/bin/env python3
"""Configuration contract: explicit empty passwords differ from missing values."""
from contextlib import redirect_stdout
import io
from unittest.mock import patch
from pathlib import Path
import sys
import tempfile
import threading

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
    recovery_values = dict(
        CDC_MERGE_UNCERTAIN_RECOVERY='off',
        CDC_MERGE_UNCERTAIN_REPLAY_MAX='7',
        CDC_MERGE_UNCERTAIN_REPLAY_BACKOFF_SECONDS='9',
    )
    assert set(recovery_values).issubset(cdc_catalog.PUBLIC_VARIABLES)
    with tempfile.TemporaryDirectory(prefix='m2s-config-test-') as directory:
        path = str(Path(directory) / 'catalog.sqlite3')
        configured = dict(values, **recovery_values)
        commands = ["SET VARIABLE " + key + " = '" + value + "'" for key, value in configured.items()]
        cdc_catalog.execute_batch(path, commands)
        assert cdc_catalog.connection_configured(path)
        saved = cdc_catalog.connection_settings(path)
        assert saved == result
        durable = cdc_catalog.variables_get(path)
        for key,value in recovery_values.items():
            assert durable[key] == value
    # Remote scripts must not report success when the durable catalog was
    # committed but runtime installation needs a restart. No sockets/network.
    with tempfile.TemporaryDirectory(prefix='m2s-remote-result-') as directory:
        path = str(Path(directory) / 'catalog.sqlite3')
        socket = str(Path(directory) / 'control.sock')
        Path(socket).touch()
        script = Path(directory) / 'deploy.sql'
        script.write_text('HELP;')
        for status, expected in [('restart_required', 1), ('rebuild_required', 1), ('hot_pending', 0)]:
            response = dict(ok=True, result=dict(publish=dict(activation=dict(
                status=status, reason='synthetic install result'))))
            with patch.object(cdc_catalog, 'client_script', return_value=response), \
                    patch.object(cdc_catalog, 'client', return_value={}), redirect_stdout(io.StringIO()):
                assert cdc_catalog.shell(path, socket, file_path=str(script)) == expected
    from e2e_contract import setup_catalog, source_options
    from starrocks_contract import configuration
    for mode in ('merge_async', 'transaction'):
        with tempfile.TemporaryDirectory(prefix='m2s-e2e-config-test-') as directory:
            env = setup_catalog(Path(directory), configuration(), source_options(), mode, 128)
            plan = cdc_catalog.load_plan(env['CDC_CATALOG_FILE'])
            assert len(plan['mappings']) == 1
            assert plan['mappings'][0]['sr_table'] == 'events'
    import j4
    runtime = dict(plan_lock=threading.RLock(), active_plan_version=1)
    with redirect_stdout(io.StringIO()):
        for status in ('restart_required', 'rebuild_required'):
            validation = dict(status=status, version=2, reason='synthetic activation constraint')
            assert j4.install_hot_catalog_plan({}, runtime, {}, validation) == validation
            assert runtime['catalog_activation'] == validation
        result = j4.catalog_publish_callback({}, runtime, dict(version=1), 'install_config')
        assert result['status'] == runtime['catalog_activation']['status'] == 'restart_required'
        j4.catalog_activation_record(runtime, dict(status='active', version=1))
    assert runtime['catalog_activation'] == dict(status='active', version=1)
    print('CATALOG CONFIG PASS credentials, remote deployment errors and activation diagnostics', flush=True)


if __name__ == '__main__':
    main()
