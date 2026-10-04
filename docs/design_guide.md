# PBGui Design Guide

This is the mandatory design and implementation guide for new pages and changes to existing page layouts, top navigation, sidebars and main views. Read it before editing the affected UI. It complements [AGENTS.md](../AGENTS.md); task-specific user instructions take precedence.

## 1. Page structure and responsibilities

Every normal application page opened from the top navigation uses the shared page shell:

```text
Top navigation (#topnav)
└── Page body (#page-body)
    ├── Sidebar (#sidebar)
    │   ├── Sticky area (#sidebar-sticky)
    │   │   ├── Title / count (#sidebar-header)
    │   │   └── Controls (#sidebar-toolbar)
    │   ├── Scrollable navigation / list (#sidebar-inner)
    │   └── Resize handle (#sidebar-resize)
    └── Main view (#main-content)
```

- **Top navigation:** application destinations, account/global status, Guide and About. Use `frontend/pbgui_nav.js` and `PBGUI_NAV_CONFIG`. Do not build another navigation bar.
- **Application menus:** open the first top-level menu by click or keyboard. While one menu is open, moving the mouse over another top-level menu switches to it immediately; hovering alone does not open a closed menu bar. Touch remains tap-to-toggle, and outside clicks close dropdowns without dismissing detail dialogs.
- **Sidebar:** page navigation, entity selection, filters, context actions and compact status information. Use meaningful page controls; do not add an empty decorative sidebar.
- **Main view:** the actual table, editor, chart, results or documentation. It fills the remaining width. Do not center the entire application inside a narrow fixed-width container.
- **Dialog:** a temporary interaction layered over the current page. A menu destination must not become a floating dialog over an otherwise empty background.
- **Help distinction:** **Information → Help** opens a full page with a topic sidebar. **Guide** opens a floating help window while retaining the current page, including when clicked on Help itself. Test these as two separate paths.
- Do not add outside-click dismissal to dialogs. Retain explicit close actions.
- The shared AI drawer message input resizes vertically using the separator along its top edge. Drag upward to enlarge; Arrow Up/Down and Home/End provide keyboard control. Persist only its browser-local height, clamp it to the viewport, and preserve the draft while resizing. The drawer's side handle remains responsible for width.
- The AI drawer shows activity and streamed ChatGPT assistant text inline in chronological order after the current turn’s last visible message while working (after the previous assistant answer for approval continuations), then collapses it into an expandable history after completion. Its header shows elapsed work time and a compact context usage ring based on provider telemetry; unavailable usage is shown explicitly rather than estimated.
- Embedded components, such as an internal LogViewerPanel sidebar, keep their component layout. Do not give them duplicate page-shell IDs or the global page-sidebar width.

### Concise UI text and on-demand help

- Keep the GUI focused on controls, data and short, clear labels. Do not add long feature explanations, instructional paragraphs or permanent explanatory banners to pages, panels, forms or dialogs.
- Put field-specific explanations behind the existing **…** help control beside the field. Show that help only on request; do not duplicate it as always-visible text below the field.
- Put detailed feature descriptions, workflows, examples and background information in the relevant **Guide / Help** topic. Guide / Help is the primary place for product documentation; keep its English and German topics in sync.
- Keep necessary validation errors, current status and information needed for an explicit decision visible, but brief and specific. They must not turn into general feature tutorials.
- Apply this rule to new UI and when updating existing UI. Move explanatory prose into field help or Guide / Help instead of adding another explanation block.

## 2. Shared files and stylesheet order

| File | Responsibility |
| --- | --- |
| `frontend/pbgui_nav.js` | Top navigation, page route mapping, global UI |
| `frontend/css/sidebar.css` | Base sidebar structure and resize-handle placement |
| `frontend/css/sidebar_layout.css` | Authoritative shared dimensions, spacing, controls, states and mobile layout |
| `frontend/js/sidebar_resize.js` | Pointer/keyboard resizing and browser-local width persistence |
| `frontend/css/sidebar_controls.css` | Additional editor, preview and content controls where needed; not required by every page |

Load base sidebar CSS first, any required content CSS next, then page-specific styles, and **`sidebar_layout.css` last**. Load the resize module once. Its automatic initialization is sufficient for static markup. A dynamically replaced sidebar must call `PBGuiSidebarResize.init()` after insertion; initialization is idempotent for the same handle.

Page-local CSS may style domain content. Do not duplicate sidebar widths, header padding, toolbar spacing, button geometry, active states or mouse-drag implementations. If a necessary reusable variant is missing, extend the shared component and test its consumers.

Do not copy an entire old page as a design reference: legacy pages can contain obsolete styles. The shared files and this guide define the standard.

## 3. Sidebar dimensions and controls

| Property | Shared standard |
| --- | --- |
| Initial width | 240 px |
| User-adjustable width | 160–420 px |
| Storage | `pbgui.sidebar.width`, shared across pages in the same browser origin |
| Outer content spacing | 8 px |
| Vertical control gaps | 4 px |
| Text button | Full available width, minimum 32 px height |
| Button padding | 6 px vertically, 10 px horizontally |
| Button type | 13 px text, 18 px line height, 6 px corner radius |
| Input / select | 32 px height, shared `sb-input` styling |
| Compact action | 32 × 32 px, centered contents |
| Mobile breakpoint | At or below 760 px, sidebar stacks above the main view |

Allow long action labels to wrap and increase button height. Keep complete information available in multi-line account/host entries. Do not assign special fixed sidebar widths to account lists or individual pages.

### Full-width actions

Use `.sb-btn` for action buttons and `.sb-section` for section navigation. Shared selected/active styles provide the blue highlight. Use semantic variants consistently: `primary`, `info`, `accent`, `danger`, `warning`, `ok`. Preserve status meaning and disabled states.

```html
<div id="sidebar-toolbar">
  <label class="sb-label" for="account-filter">Search accounts</label>
  <input class="sb-input" id="account-filter" type="search">
  <button class="sb-btn primary" type="button">Calculate</button>
</div>
```

### Compact icons and language buttons

Use **`.sb-icon-row` on the container with direct `.sb-btn` children**. This opt-in shared variant makes the buttons square, centers their content and wraps the row when needed. It works both as the toolbar itself and as a group within a toolbar.

```html
<div class="sb-icon-row" role="group" aria-label="Help language">
  <button class="sb-btn active" type="button" aria-pressed="true">EN</button>
  <button class="sb-btn" type="button" aria-pressed="false">DE</button>
</div>

<div id="sidebar-toolbar" class="sb-icon-row" role="group" aria-label="Dashboard actions">
  <button class="sb-btn" type="button" title="New dashboard" aria-label="New dashboard">+</button>
  <button class="sb-btn primary" type="button" title="Save dashboard" aria-label="Save dashboard">💾</button>
</div>
```

Do not put two full-width `.sb-btn` elements in a plain flex row: `width:100%` and non-shrinking buttons can create horizontal overflow. `.sb-button-row` alone is not the compact icon variant. Do not compensate with page-local widths or `overflow-x:hidden`; fix the chosen control layout.

Use real buttons for keyboard-operable navigation. Provide accessible labels for icons, visible focus, and `aria-pressed` or `aria-current` where appropriate. Preserve `[hidden]` and disabled behavior when styling controls.

## 4. Minimal page template

The asset versions below describe the current implementation. Check current versions when using the template. API routes must safely replace placeholders and preserve the configured application mount prefix.

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Page Name - PBGui</title>
  <link rel="stylesheet" href="/app/css/sidebar.css?v=6">
  <style>
    html, body { height:100%; margin:0; }
    body { display:flex; flex-direction:column; overflow:hidden; }
    #topnav { flex-shrink:0; }
    #page-body { display:flex; flex:1; min-height:0; overflow:hidden; }
    #main-content { flex:1; min-width:0; min-height:0; overflow:auto; padding:20px; }
    /* Add the page's dark theme tokens and domain-specific content styles. */
  </style>
  <link rel="stylesheet" href="/app/css/sidebar_layout.css?v=4">
  <script src="/app/js/sidebar_resize.js?v=3"></script>
</head>
<body>
  <nav id="topnav"></nav>
  <div id="page-body">
    <aside id="sidebar" aria-label="Page controls">
      <div id="sidebar-sticky">
        <div id="sidebar-header"><span class="sb-title">Page Name</span></div>
        <div id="sidebar-toolbar">
          <!-- Page-specific controls using the shared classes. -->
        </div>
      </div>
      <div id="sidebar-inner">
        <!-- Navigation, entities or page status. -->
      </div>
      <div id="sidebar-resize"></div>
    </aside>
    <main id="main-content">
      <h1>Page Name</h1>
      <!-- Tables, editor, results or documentation. -->
    </main>
  </div>
  <script>
    window.API_BASE = "%%API_BASE%%";
    window.WS_BASE = "%%WS_BASE%%";
    window.PBGUI_NAV_CONFIG = { subtitle:'Page Name', current:'registered_page_key' };
  </script>
  <script src="/app/pbgui_nav.js?v=%%NAV_HASH%%"></script>
</body>
</html>
```

For static pages under `/app/`, preserve mounted deployments with relative assets such as `./css/sidebar.css` and `./js/sidebar_resize.js`, as Help does. Never derive a different host or discard a configured mount prefix. Keep all runtime assets local. Browser authentication uses the same-origin HttpOnly cookie, never a token embedded in HTML or URLs.

## 5. Main view, scrolling and responsive behavior

- Give shrinking flex/grid children `min-width:0` and `min-height:0`. A viewport-constrained body plus an unbounded child can make content unreachable.
- Keep the top navigation outside the scrolling main view. Account for visible status banners rather than adding independent viewport heights that exceed the window.
- Let the shared mobile rules control sidebar stacking and the hidden resize handle. Keep both sidebar content and the main view reachable.
- Long sidebar lists scroll in `#sidebar-inner`. Long sticky action groups must also remain reachable at short viewport heights.
- Put wide tables in their own overflow container. Use sticky dark headers with a 2 px bottom border. Horizontal scrolling is legitimate for wide data tables, not for an EN/DE button row.
- Size multi-column content for the **remaining main-view width**, not only window width. Prefer container queries or an equivalent responsive layout. Dragging the sidebar wider can require editors/results to stack.
- Do not solve clipping by hiding overflow while leaving controls outside the visible area.
- Keep existing fields, columns, status meanings, selected rows and contextual actions when changing presentation.

### Main content conventions from existing pages

Use the established editor/table hierarchy, as in Optimize (`v7_optimize.html`, also the PB8 adapter), Backtest (`v7_backtest.html`) and Run Config (`v7_edit.html`). Monitoring pages use the same full-width main view with a toolbar and scrollable data area.

- A main view uses the available width and the page's `--bg` background. Its padding comes from `--sp-lg` (mobile `--sp-md`), not a separate narrow wrapper.
- Use an `.editor-shell` for vertically arranged editor sections; use `.section-title` for section headings with the existing font size, spacing and bottom divider. Do not mix unstyled h3/h4 headings with editor sections.
- Fields use the existing `.form-row` grid, column/span variants and `.form-group` controls. Arrange configuration/input, objectives, execution limits and actions in that order. Adapt grids to the remaining content width, including a wide sidebar.
- Use `.panel-toolbar` for contextual actions, selections and compact status. Use regular `.btn` controls for primary workflow actions. `.act-btn`/`.tiny-btn` are compact table actions, not the default for a whole workflow or window tabs.
- Main data tables use the existing `.tbl` presentation: sticky dark header, uppercase muted column labels, 2 px header separator, standard row spacing and selection highlight. Keep tables in bordered overflow containers. Report prose/diffs belong in a detail window reached from a row or action.
- Show summary values using the editor's grid/field hierarchy or the page's existing status controls. Do not invent a second dashboard-card style inside an editor.
- Preserve the shell, all fields/actions, automatic updates and navigation/edit state when reorganizing the main content.
- Config compatibility changes use the shared `editor_shared.js` markers: amber outlines on the exact mapped structured fields, and amber highlights at exact paths in Raw JSON. JSON syntax errors retain red priority. Preserve focus, selection and scroll; annotations never enter saved config data. The Long/Short JSON editors keep their individual HSL review markers instead of coloring unrelated blocks.

### Windows and detail dialogs

An inventory on 2026-10-01 found 18 existing window-bearing frontend implementations using centered overlays; the AI Loop native `<dialog>` was the outlier. Reference implementations include Optimize `.modal-backdrop`/`.modal`/`.modal-head`/`.modal-body`, Backtest `.modal-overlay`/`.modal-box`, Run Config import windows, Jobs Monitor and the shared `pbgui_dialogs.js`/Guide overlay. Their common geometry defines the rule below; historical class names may differ.

- Open a full-viewport fixed overlay with a dim backdrop and a **centered** window. The main view remains at its current position behind it. Do not rely on native `<dialog>` user-agent margins/positioning: the global CSS reset can place it in the top-left corner.
- The window uses `--bg2`, `--border`, existing text/font/spacing tokens, an 8–10 px radius, and the established shadow. The title/header is visually separated by a bottom border. Put the explicit close action at the top right.
- Use `frontend/css/modals_shared.css` sizing tokens: wide detail windows `--pbg-modal-wide-width` (1100 px bounded by viewport), settings windows `--pbg-modal-settings-max-width` (760 px), and `--pbg-modal-max-h`. Keep viewport margins on narrow/short screens; no off-screen controls.
- New interactive detail windows use the reusable `.pbg-modal-overlay` → `.pbg-modal-window` shell from `modals_shared.css`, with `.pbg-modal-head`, optional `.pbg-modal-tabs` and `.pbg-modal-body`. Domain CSS may style content, not replace the shared positioning/header/body geometry. Existing modal families remain valid; do not mass-migrate unrelated pages.
- The header and tabs stay visible; only the content body scrolls. Wide content tables scroll inside their own container. Use regular-size tabs with an active state, not miniature action buttons. Do not force a large empty window for a short message.
- Confirmation, warning and prompt workflows use `PBGuiDialogs`. Detail windows use safe DOM rendering (`textContent`), `role="dialog"`, `aria-modal="true"` and a labelled title. Keep keyboard focus inside the window; restore focus to the trigger on close. Escape and the close control are explicit close actions. **Never close on backdrop/outside clicks.**
- Updates must preserve the open window, active tab, selection and scroll position. Persist non-secret report navigation for browser reload where appropriate. Do not duplicate full report text in the main view.
- Interactive report/detail windows must be **draggable by the title bar and resizable at their edges/corners**, like the existing log, chart and Guide windows. Centering describes only the initial placement. Preserve chosen position/size across tabs and automatic updates; clamp the window to the viewport after browser resizing. Use `frontend/js/floating_window.js` for the shared detail-window shell. Simple confirmation/prompt dialogs remain a separate interaction type. Do not classify movable report windows as optional special cases.

Minimal detail-window shell (opened by adding `.is-open` and setting `aria-hidden="false"` on the overlay):

```html
<div class="pbg-modal-overlay" aria-hidden="true">
  <div class="pbg-modal-window" role="dialog" aria-modal="true" aria-labelledby="report-title">
    <div class="pbg-modal-head">
      <h3 id="report-title">Report</h3>
      <button class="pbg-modal-close" type="button" aria-label="Close">×</button>
    </div>
    <div class="pbg-modal-tabs" role="tablist"><!-- Accessible tabs --></div>
    <div class="pbg-modal-body"><!-- Report content --></div>
  </div>
</div>
```

## 6. Navigation, loading and state

- Register normal pages in `FASTAPI_PAGES`, the correct navigation group and `GUIDE_TOPICS`, with EN/DE guide coverage.
- Use the existing page key and canonical route for shared PB7/PB8 views.
- Browser reload must restore non-secret navigation state: current topic/view/tab/entity, filters, search mode, sorting and pagination as applicable. Explain unavailable destinations and use the nearest valid context.
- Updates must preserve current selection and unsaved edits. Do not rebuild an editor or reset navigation merely to update a status value.
- Update data automatically after actions, background changes and return to the view. Do not introduce manual in-app Refresh buttons/icons.
- Render loading/error/empty states promptly and honestly. A loading placeholder does not fix a blocked request. Keep expensive synchronous scans off the API event loop and outside lightweight initial status paths.
- Use request generations to reject stale responses and callbacks; clean up timers, requests and listeners according to their lifecycle.

## 7. Cache and delivery

- Bump `?v=` references for changed shared JS/CSS consumers, including dynamically loaded assets.
- For versioned static HTML destinations, also bump the URL in `FASTAPI_PAGES`. Changing `help.html` while keeping the old menu URL can leave users on a cached dialog page.
- Check static-page navigation-script versions as well as server-rendered `%%NAV_HASH%%` references.
- Verify what the **actual menu click** requests. A test that manually injects the new HTML bypasses this failure mode.
- Follow AGENTS.md for API serial increments when API/startup code changes. Frontend-only work does not require a service restart. Do not restart services without authorization.

## 8. Required verification before reporting completion

Use isolated data and intercept browser requests; never run production actions merely to test layout. Do not take screenshots unless the user asks.

1. **Actual entry point:** click the real top-navigation menu with the real navigation script. Confirm the intended route/cache version and the resulting full-page shell.
2. **Separate Guide behavior:** confirm Guide opens a floating window without changing the current destination. On Help, test both menu entry and Guide.
3. **Widths:** check default, minimum 160 px and maximum 420 px sidebars, and viewport widths on both sides of 760 px. Verify resize persistence after page navigation and reload.
4. **Overflow:** assert `scrollWidth <= clientWidth` for sidebar controls/compact rows. Verify both EN/DE buttons remain visible and aligned. Check access to the bottom of long lists/actions and the main content.
5. **Real dynamic controls:** exercise actual renderers for view/edit modes and conditional actions, including Save/Delete, badges, disabled controls and hidden controls. Synthetic buttons alone are insufficient.
6. **Content:** verify all original data fields and interactions remain available, including filters, selections and edits during updates. Keep visible UI copy concise: field explanations belong behind **…**, and detailed feature help belongs in Guide / Help.
7. **Interaction:** test pointer/keyboard resizing, focus, long labels, compact icon wrapping and embedded-frame boundaries when relevant. For windows, assert centered placement (not just lack of overflow), bounded geometry, header/close visibility, focus containment/return, actual title-bar dragging and edge/corner resizing, geometry persistence across tabs/updates/reload, Escape and no backdrop dismissal; test long content and short viewports.
8. **Navigation state:** reload a non-default destination with filters/search and verify restoration. Test asynchronous topic/language changes where applicable.
9. **Mounted routes:** confirm API and asset requests retain the application prefix.
10. **Finish:** update EN/DE user guides and `releases/unreleased.md`; report changed areas and tests actually performed. Do not claim that an isolated component test proves the real menu works.

Relevant test entry points include `tests/ui/test_sidebar_shared.py`, `tests/ui/test_help_sidebar_browser.py`, `tests/ui/test_help_request_generations.py` and `tests/test_help_coverage.py`. Extend focused tests for the affected page; observe the full-suite gate in AGENTS.md.
