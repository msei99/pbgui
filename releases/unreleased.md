# Unreleased

- Publish and launch the independent deadline guard over the authenticated root SSH transport before worker preflight, even when Vast onstart did not run. Reuse container-local credentials, retain guard/amendment state, and serialize guard processes without relaxing host identity, revision, deadline or budget validation.

- Retry guard publication and transient SSH authentication during the first five minutes of worker setup; fail closed on timeout or identity/validation errors.
