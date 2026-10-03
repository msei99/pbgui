# Help Overview

Help keeps the latest selected topic and language when requests finish out of order; older content and search results are ignored.

This is the central Help page for PBGui. It contains general documentation and tutorials that are shared across multiple pages, such as Strategy Explorer (incl. Movie Builder) and Pareto Explorer.

- Use the **Contents** list to choose specific topics.
- Languages supported: EN / DE

The top navigation works like an application menu: click a menu to open it, then move the mouse across the other menu headings to switch directly. Clicking outside closes the menu. On touch screens, tap each menu heading to open or close it.

Help content is sanitized before display. Links and images accept only HTTP(S) URLs, and search highlights are built as text nodes rather than executable HTML. The Help page and overlay retain the configured ASGI mount prefix for local assets and API requests.

On pages with a sidebar, drag its right edge to set the width. PBGui saves one sidebar width in this browser and restores it across pages and refreshes. The focused resize handle also supports Left/Right arrow keys and Home/End. Narrow screens use a full-width stacked sidebar with independently scrollable navigation and content.


All page sidebars use the same spacing and controls: 8 px outer padding, 4 px gaps, 13 px button text, and buttons at least 32 px high with 6 px corners. Long labels wrap instead of being cut off. The active destination or selected account has a blue highlight. Action colors remain meaningful: save/create (teal), information (blue), warning (amber), success (green), and destructive actions (red). Long action lists scroll within the sidebar.

The standalone Help page uses the shared resizable sidebar for language selection, topic filtering and the topic list. Documentation fills the main area; topic search and All topics search remain available above it. Selected topic, language, topic filter, search text and search mode are restored from the page URL after reload. The Guide overlay on other pages remains separate.

The EN/DE language buttons use the compact shared button row and remain side by side even at the minimum sidebar width.
