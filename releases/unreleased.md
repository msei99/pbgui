# Unreleased

## Fixes

- Vast collection teardown no longer destroys a rental while a job bound to it is still collecting. The lease guard now consults every bound job's collection state (status, final_collected and the final-collection anchors) and defers the destroy within the bounded grace window, so a stop/deadline cancel can no longer win the race against the final collection.
