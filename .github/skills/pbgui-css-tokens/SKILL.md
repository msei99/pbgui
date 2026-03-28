---
name: pbgui-css-tokens
description: 'PBGui CSS design tokens for FastAPI frontend pages. Use when writing or reviewing CSS in frontend/*.html files, or when migrating hardcoded px/rem font sizes to var(--fs-*) tokens. Covers font sizes (XS–XL), spacing (--sp-*), input/button heights. Trigger: CSS, font-size, spacing, design tokens, --fs-, --sp-, px, rem, frontend HTML.'
argument-hint: 'Optional: name of page or element to migrate (e.g. dashboard_main.html)'
---

# PBGui CSS Design Tokens

## When to Use
- Writing new CSS for any `frontend/*.html` page
- Reviewing/migrating hardcoded `px` or `rem` font-size values
- Adding spacing, padding, gap, margin values to new components
- Ensuring a new page is consistent with `api_keys_editor.html`

## Token Reference

### Typography (6 levels)

| Token | Size | Use |
|-------|------|-----|
| `--fs-xs` | 11px | Timestamps, status dots, badges, secondary hints, code inside prose |
| `--fs-sm` | 13px | Table headers (uppercase), status lines, labels, TOC items, btn-sm |
| `--fs-base` | 14px | **Default** — table cells, inputs, buttons, sidebar buttons, filter fields |
| `--fs-md` | 15px | Panel subtitles, modal titles, close button icon, "All Selected" labels |
| `--fs-lg` | 18px | Page/section headings, primary action buttons (Sync Keys / All) |
| `--fs-xl` | 22px | Large display headings (rare — empty-state pages, H1 in guide content) |

### Spacing (4 levels)

| Token | Size | Use |
|-------|------|-----|
| `--sp-xs` | 4px | Tight gaps in compact elements |
| `--sp-sm` | 8px | Standard button padding (horizontal), gaps between inline controls |
| `--sp-md` | 12px | Section spacing, container padding |
| `--sp-lg` | 20px | Panel padding, large outer gaps |

### Control Heights

| Token | Size | Use |
|-------|------|-----|
| `--input-h` | 32px | All text inputs, number inputs, selects |
| `--btn-h` | 32px | All buttons (same height as inputs) |

## `:root` Block (copy verbatim)

```css
:root {
    --fs-xs: 11px;
    --fs-sm: 13px;
    --fs-base: 14px;
    --fs-md: 15px;
    --fs-lg: 18px;
    --fs-xl: 22px;
    --sp-xs: 4px;
    --sp-sm: 8px;
    --sp-md: 12px;
    --sp-lg: 20px;
    --input-h: 32px;
    --btn-h: 32px;
}
```

## Migration: Hardcoded → Token

| Old value | Correct token |
|-----------|---------------|
| `10px`, `11px` | `var(--fs-xs)` |
| `0.72rem`, `0.78rem`, `0.8rem`, `12px`, `13px` | `var(--fs-sm)` |
| `0.85rem`, `0.875rem`, `0.9rem`, `0.97rem`, `14px` | `var(--fs-base)` |
| `0.95rem`, `1rem`, `15px`, `16px` | `var(--fs-md)` |
| `1.1rem`–`1.2rem`, `18px` | `var(--fs-lg)` |
| `1.4rem`–`1.6rem`, `22px`–`24px` | `var(--fs-xl)` |
| `4px` pad/gap | `var(--sp-xs)` |
| `8px` pad/gap | `var(--sp-sm)` |
| `12px` pad/gap | `var(--sp-md)` |
| `20px` pad/gap | `var(--sp-lg)` |
| `height: 32px` on input/button | `var(--input-h)` / `var(--btn-h)` |

## Per-Element Mapping (api_keys_editor.html reference)

| Element | Token |
|---------|-------|
| `.sb-btn` sidebar buttons | `--fs-base` |
| `.sb-title` "API KEYS" label | `--fs-sm` |
| `.sb-count` badge | `--fs-sm` |
| Table cells (user list, SSH table) | `--fs-base` |
| Table header (th, uppercase labels) | `--fs-sm` |
| Filter input | `--fs-base` |
| `input`, `select` | `--fs-base`, `height: var(--input-h)` |
| `button` (.btn) | `--fs-base`, `height: var(--btn-h)` |
| `.btn-sm` | `--fs-sm` |
| Toast / modal body text | `--fs-base` |
| Modal title / panel heading | `--fs-md` |
| Page section heading | `--fs-lg` |
| Help overlay H1 | `--fs-xl` |
| Help overlay H2 | `--fs-lg` |
| Help overlay H3 | `--fs-md` |
| Help overlay body text | `--fs-base` |
| Help overlay code | `--fs-xs` |
| Help TOC items | `--fs-sm` |
| Search input (#help-search) | `--fs-sm` |
| Search count / global label | `--fs-xs` |

## Procedure: Migrating a New Page

1. Add the `:root` block (above) inside `<style>` at the top of the file
2. Set `html, body { font-size: var(--fs-base); }`
3. Search for `font-size:` in the file and replace each value using the mapping table
4. Search for `padding:`, `gap:`, `margin:` and replace spacing values using `--sp-*`
5. For `input`, `select`, `button`: add `height: var(--input-h)` / `height: var(--btn-h)`
6. Validate: no hardcoded `px`/`rem` font-size values should remain (except inside `:root`)

## Notes
- Source of truth: `frontend/api_keys_editor.html` `:root` block (line ~10)
- `--sp-*`, `--input-h`, `--btn-h` are defined but not yet wired everywhere — wire them as you touch elements
- Do NOT apply these tokens to Streamlit pages (`.py` navi files) — Streamlit has its own theming
