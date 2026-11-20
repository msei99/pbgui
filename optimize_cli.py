"""Optimizer argv shared by detached local runners and native cloud workers."""

SERVICE = 'Optimize'


def optimizer_argv(cli_path, config_path, options, *, runtime='v8', manifest_path=None):
    """Translate a validated portable launch contract into argv, never a shell."""
    argv = [cli_path, config_path]
    mode = str(options.get('mode') or 'fresh')
    source = str(options.get('source') or '')
    if mode == 'checkpoint_resume':
        argv.extend(['--resume', source])
    elif mode == 'pareto_seed':
        argv.extend(['--start', source])
    elif mode != 'fresh':
        raise ValueError(f'Unsupported optimize launch mode: {mode}')
    fine_tune = options.get('fine_tune_params')
    if isinstance(fine_tune, list):
        fine_tune = ','.join(str(item).strip() for item in fine_tune if str(item).strip())
    if str(fine_tune or '').strip():
        argv.extend(['--fine-tune-params', str(fine_tune).strip()])
    polish = options.get('polish_percentage')
    if polish is not None:
        argv.extend(['--polish-bounds-fraction' if runtime == 'v9' else '--polish-pct', str(polish)])
        argv.extend(['--polish-bounds-mode', str(options.get('polish_bounds_mode') or 'clamp')])
    if runtime == 'v9':
        from pb9_optimize_options import cli_arguments
        argv.extend(cli_arguments(options, manifest_path))
    return argv
