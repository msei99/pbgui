"""Shared PB8/PB9 coin search uses names/contracts without matching venue prefixes."""
from pathlib import Path

import pytest
from browser_driver import sync_browser

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('version',['v8','v9'])
def test_shared_picker_search_and_exact_selection_are_identical(version):
    """Exercise real shared controls with each actual optimizer adapter in an isolated browser."""
    with sync_browser() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page()
            page.route('**/*',lambda route:route.abort())
            page.set_content('''<div class="form-group"><div id="coins">
              <input class="ms-input"><div class="ms-dropdown"></div></div></div>
              <div id="venues"><input class="ms-input"><div class="ms-dropdown"></div></div>''')
            page.add_script_tag(path=str(ROOT/'frontend/js/editor_shared.js'))
            page.add_script_tag(path=str(ROOT/'frontend/js/optimize_editor_adapter.js'))
            page.evaluate('''(version)=>{
              const adapter=PBGuiOptimizeEditorAdapter.create(version);
              const labels={'hyperliquid::19':'STX (hyperliquid: 19)',
                            'bybit::STXUSDT':'STX (bybit: STXUSDT)'};
              window.picker=PBGuiEditorShared.createMultiselectController({
                controllerName:'search-'+adapter.version,coinIds:{coins:true},
                formatOptionLabel:ctx=>labels[ctx.value]||ctx.value});
              picker.rebuild('coins',['HYPE','HYPER','hyperliquid::19','bybit::STXUSDT'],['HYPE']);
              picker.rebuild('venues',['bybit','hyperliquid'],[]);
            }''',version)
            field=page.locator('#coins .ms-input')
            for query,expected in [('hype',['HYPE','HYPER']),
                ('stx',['hyperliquid::19','bybit::STXUSDT']),
                ('19',['hyperliquid::19']),('hyperliquid',[]),
                ('hyperliquid::19',['hyperliquid::19'])]:
                field.fill(query)
                assert page.locator('#coins .ms-option').evaluate_all(
                    '(options)=>options.map(option=>option.dataset.val)')==expected, (version,query)
            field.press('Enter')
            assert page.evaluate("picker.getValues('coins')")==['HYPE','hyperliquid::19']
            page.locator('#venues .ms-input').fill('hype')
            assert page.locator('#venues .ms-option').text_content()=='hyperliquid'
        finally:
            browser.close()
