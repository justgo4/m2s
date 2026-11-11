# Offline bundle

This directory is the synthetic layout for an air-gapped install.
It does not contain database passwords, production addresses, or run data.
Build the image from `deploy/Dockerfile` on the target network.
The chart under `deploy/helm/m2s` uses `exampleOnly: true`.
