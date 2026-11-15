# PB9 Config Editor

Open **PBv9 → Config Editor** for the first PB9 editor preview. It edits the complete authored configuration as JSON. PB7/PB8 editors and configurations remain separate.

## Runtime paths

Open **Runtime paths**, enter the absolute source-directory path of a separate Passivbot v9 checkout and the Python executable inside its own virtual environment, then choose **Save paths**. The checkout must declare a v9 config schema. The environment must contain the CLI and Rust extension and use a supported Python version. PB7, PB8 and PBGui directories/environments cannot be reused.

The editor does not install or update Passivbot or compile Rust. **PB9 detected** reports static layout checks; loading a template or validating additionally exercises the selected environment. Installations and bot/VPS changes remain separate operations. An unavailable runtime leaves the text editor usable, but native actions remain unavailable.

## Create and edit

Choose **New from template** to obtain the authored template from the selected PB9 runtime. Enter a visible filename ending in `.json`, edit **Raw JSON**, and choose **Validate**. Validation checks inline native source without expanding its returned defaults into your document. Duplicate JSON keys, unsupported values, nonfinite numbers and external file references are rejected.

Saving uses four-space indentation and moves root `pbgui` metadata to the end; authored values remain unchanged.

Only original root `pbgui` metadata is kept as GUI data and is excluded from native validation. Runtime/schema/commit evidence comes from the selected environment, not that metadata. No migration is performed. A v8 config must first be migrated explicitly to a separate v9 document outside this preview.

## Save and reopen

**Save** creates a new file exclusively in PBGui's private `data/configs_v9` directory. Select its name in the sidebar to open it. Updates require the exact loaded revision; another writer's changes cannot be overwritten silently. Enter a different filename to create a copy. The preview does not delete or rename files or start backtests, optimizers or live bots.

If another writer changes the file, your text is kept. **Open stored version** opens the current saved document after confirming that unsaved edits may be discarded; alternatively save with a new filename. If storage acknowledgement fails after publication, the editor checks the stored file and does not blindly retry the write. A native validation failure can prevent opening a stored file; paste corrected JSON into the editor and save it with a new filename.

## Automatic updates and navigation

Runtime status, the file list and the selected saved document update automatically while the page is visible and when you return. Unchanged files are checked by a bounded revision-only read, without a native helper; only changed clean files are loaded again. Clean documents accept updated stored contents; unsaved text and path edits remain in place. Validation results refer to the submitted version if you continue typing during the request.

Browser reload restores the selected filename and sidebar search. Unsaved configuration text is not stored in browser storage; the browser warns before leaving a dirty document. Save before reloading to keep those edits. The shared **Guide** opens above the current editor; **Information → Help** opens the separate help page.

## Limits

Runtime paths containing `%` are currently unsupported by the INI slot writer and are rejected without changing settings.

Requests and documents have separate one-MiB limits. The document limit counts ASCII-escaped JSON and the native transport includes additional runtime identity, so a compact or non-ASCII request can reach a limit below one MiB on the wire. JSON nesting is limited to 64 levels and filenames to 255 UTF-8 bytes. Only inline configurations are supported; file-backed coin lists/overrides and symbol-map-dependent sources are rejected. The library scan is bounded to 1,000 directory entries.

Native helper ownership currently requires Linux. Stop/shutdown cancels in-flight native checks; an atomic save already published remains saved. Revision locks coordinate PBGui writers; unrelated same-user filesystem editors do not participate in that lock protocol. A real v9/Rust installation is required for native testing; offline browser tests use isolated fixtures.
