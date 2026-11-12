# Offline bundle

This directory is only the synthetic layout. It is not the install bundle.
`python3 tools/make_offline_bundle.py /tmp/m2s-offline.tgz` writes the
tarball outside the repository. The tarball holds the Python wheels,
`install.sh`, config templates, and the native reader when that binary
has already been built. Do not commit the tarball or the wheels.
`install.sh` installs with `pip --no-index`.
The chart under `deploy/helm/m2s` uses `exampleOnly: true`.
