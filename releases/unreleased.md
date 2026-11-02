# Unreleased

- Fix large Vast Pareto collections losing publication progress: scale the manual deadline, allow INI/environment timeout overrides, publish resumable batches (100 files by default), retain committed files on timeout, and record incomplete collection progress. Preserve the rental's final-collection reserve; document the controls in EN/DE (#507).

- Add a direct, owner-scoped DeepSeek provider to AI Setup, streaming AI Chat and isolated AI Loop decisions, with private API-key storage, native tool/reasoning support, current Flash/Pro models and explicit legacy Chat/Reasoner choices.
